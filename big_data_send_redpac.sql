-- ============================================================
-- 存储过程：P_BATCH_SEND_REDPAC_BIGDATA
-- 功能：大厂场景批量送红包（适配 700~1000 万源数据规模）
-- 基础版本：P_BATCH_SEND_REDPAC v2（P_BATCH_SEND_REDPAC.sql）
--
-- 【前置要求】
-- 建表 DDL 和必备索引参见 P_BATCH_SEND_REDPAC.sql
-- 所有索引必须在执行本过程前创建，尤其是：
--   EMALLUAT.IDX_MEMBER_HOSTCUSTNO      — 客户匹配
--   EMALLUAT.IDX_MYREDPAC_MEMBER_PAC     — 个人已发统计
--   EMALLUAT.IDX_MYREDPAC_PACID          — 红包已发总计
--   ZHJFUAT.IDX_REDPACACT_HIS_DEDUP      — HIS 去重
--   ZHJFUAT.IDX_SENDING_DEL              — 待送表删除
--
-- 【相比 v2 的核心变更（大厂场景适配）】
-- 1. 增量聚合 MYREDPAC —— 不再全表扫描，仅查源数据中出现的 REDID+MEMBERNO
--    2000万行全扫→可能只查几万行，I/O 降低 100~1000 倍
-- 2. MERGE 替代 INSERT+NOT EXISTS —— HIS 去重由逐行索引反查改为单次 HASH JOIN
--    500万次随机读 → 1 次顺序 HASH ANTI JOIN
-- 3. 分批提交 —— 成功记录每 50 万行 COMMIT 一次，打破单事务 UNDO 膨胀
--    10GB+ UNDO → 每批 <500MB，回滚代价可控
-- 4. 失败记录前置提交 —— 小数据量（客户不存在/无效红包/超限）单独提交
--    减少事务跨度，失败不影响成功批次
-- 5. 可重跑安全 —— 成功写入前自动过滤 HIS 中已存在的记录
--    中途失败后可安全重新执行，不会重复发红包
--
-- 【业务逻辑（与原版一致）】
-- a) 过滤客户不存在的记录 → HIS(0) + PRIVI_REDPACACT
-- b) 过滤无效红包 → HIS(2) + PRIVI_REDPACACT
-- c) 过滤超出个人限制 → HIS(3) + SENDING 重试
-- d) 过滤超出红包总数限制 → HIS(4) + SENDING 重试
-- e) 成功记录 → MYREDPAC + REDPACAPPLY + REDPACAPPLYDETAIL + HIS(1)
-- f) 清理已成功的 SENDING 记录 + 过期数据
--
-- 参数：
--   p_day : 清理超过 p_day 天的待送表历史，0 表示清空整个待送表
-- ============================================================

CREATE OR REPLACE PROCEDURE P_BATCH_SEND_REDPAC_BIGDATA(p_day IN NUMBER DEFAULT 30)
IS
    -- 时间变量
    v_now          VARCHAR2(14);
    v_now1         VARCHAR2(19);
    v_now2         VARCHAR2(19);
    v_today        VARCHAR2(8);
    v_batchid      VARCHAR2(20);
    l_accdate      VARCHAR2(8);

    -- 源数据总量
    v_total_source  NUMBER := 0;

    -- 各类处理计数
    v_no_member_cnt    NUMBER := 0;
    v_invalid_pac_cnt  NUMBER := 0;
    v_over_limit_cnt   NUMBER := 0;
    v_over_max_cnt     NUMBER := 0;
    v_success_cnt      NUMBER := 0;

    -- 分批处理变量
    v_batch_size    NUMBER := 500000;
    v_batches       NUMBER;
    v_start_rn      NUMBER;
    v_end_rn        NUMBER;
    v_sub_batchid   VARCHAR2(30);
    v_batch_cnt     NUMBER;

    -- 异常处理
    v_error_info     VARCHAR2(4000);
    v_error_batch    VARCHAR2(50);

    -- 安全删除临时表
    PROCEDURE safe_drop(p_table IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table;
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

BEGIN
    -- =================================================================
    -- 阶段1：初始化
    -- =================================================================
    EXECUTE IMMEDIATE 'SET ob_query_timeout = 86400000000';

    v_now     := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
    v_now1    := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    v_now2    := TO_CHAR(SYSDATE, 'YYYYMMDD HH24:MI:SS');
    v_today   := TO_CHAR(SYSDATE, 'YYYYMMDD');
    v_batchid := 'P' || v_now;

    SELECT sp.sysworkdate INTO l_accdate FROM sysparameter sp;

    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 开始 | 批次: ' || v_batchid
                      || ' | 时间: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

    -- =================================================================
    -- 阶段2：检查待处理数据
    -- =================================================================
    SELECT COUNT(1) INTO v_total_source
    FROM (
        SELECT 1 FROM ZHJFUAT.QS_TEMP_BZ_DATA_REDPACACT WHERE ROWNUM = 1
        UNION ALL
        SELECT 1 FROM ZHJFUAT.BZ_DATA_REDPACACT_SENDING WHERE ROWNUM = 1
    );

    IF v_total_source = 0 THEN
        DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 无待处理数据');
        IF p_day = 0 THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE ZHJFUAT.BZ_DATA_REDPACACT_SENDING';
        ELSE
            DELETE FROM ZHJFUAT.BZ_DATA_REDPACACT_SENDING
            WHERE CREATEDATE < TO_CHAR(SYSDATE - p_day, 'YYYYMMDD');
        END IF;
        COMMIT;
        RETURN;
    END IF;

    -- =================================================================
    -- 阶段3：构建统一源数据（提前到 MYREDPAC 统计之前，为增量聚合提供 REDID 范围）
    -- =================================================================
    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 阶段3: 构建源数据临时表...');
    safe_drop('tmp_source');

    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE tmp_source ON COMMIT PRESERVE ROWS AS
        SELECT
            src.HCNO,
            src.REDID,
            src.OTHERDESC,
            src.OTHERID,
            src.CHANNELNO,
            src.OTHERACTNO,
            src.DATATYPE,
            m.MEMBERNO,
            CASE WHEN m.MEMBERNO IS NULL THEN ''N'' ELSE ''Y'' END AS has_member,
            r.PACID              AS r_pacid,
            NVL(r.PACAMTMIN, 0)  AS pac_amt,
            NVL(r.LIMITCOUNT, 0) AS limit_cnt,
            NVL(r.MAXSEND, 0)    AS max_send,
            CASE
                WHEN r.PACID IS NULL          THEN ''PAC_NOT_EXIST''
                WHEN r.AUTHSTATE != ''1002''  THEN ''AUTH_FAIL''
                WHEN r.STATE != ''00''        THEN ''DISABLED''
                WHEN NVL(r.USEENDTIME, ''9999-12-31 23:59:59'') < ''' || v_now1 || ''' THEN ''EXPIRED''
                ELSE ''VALID''
            END AS pac_status
        FROM (
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO, 0 AS DATATYPE
            FROM ZHJFUAT.QS_TEMP_BZ_DATA_REDPACACT
            UNION ALL
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO, 1 AS DATATYPE
            FROM ZHJFUAT.BZ_DATA_REDPACACT_SENDING
        ) src
        LEFT JOIN EMALLUAT.MEMBER m ON m.HOSTCUSTNO = src.HCNO
        LEFT JOIN EMALLUAT.REDPACACT r ON r.PACID = src.REDID';

    SELECT COUNT(1) INTO v_total_source FROM tmp_source;
    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 源数据总量: ' || v_total_source);

    -- =================================================================
    -- 阶段4：增量聚合 MYREDPAC 统计数据
    -- 核心优化：只查源数据中出现的 REDID + MEMBERNO，避免全表扫描
    -- 使用 EXISTS 半连接，OceanBase 自动选择 INDEX RANGE SCAN + HASH SEMI JOIN
    -- =================================================================
    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 阶段4: 增量聚合 MYREDPAC 统计...');
    safe_drop('tmp_member_rp_stats');

    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE tmp_member_rp_stats ON COMMIT PRESERVE ROWS AS
        SELECT m.MEMBERNO, m.PACID, COUNT(*) AS sent_cnt
        FROM EMALLUAT.MYREDPAC m
        WHERE EXISTS (
            SELECT 1 FROM tmp_source t
            WHERE t.REDID = m.PACID
              AND t.MEMBERNO = m.MEMBERNO
              AND t.has_member = ''Y''
              AND t.pac_status = ''VALID''
        )
        GROUP BY m.MEMBERNO, m.PACID';

    safe_drop('tmp_pac_total_stats');

    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE tmp_pac_total_stats ON COMMIT PRESERVE ROWS AS
        SELECT m.PACID, COUNT(*) AS total_sent
        FROM EMALLUAT.MYREDPAC m
        WHERE EXISTS (
            SELECT 1 FROM tmp_source t
            WHERE t.REDID = m.PACID
              AND t.has_member = ''Y''
              AND t.pac_status = ''VALID''
        )
        GROUP BY m.PACID';

    -- =================================================================
    -- 阶段5：处理客户不存在的记录（失败量小，单独提交）
    -- =================================================================
    SELECT COUNT(1) INTO v_no_member_cnt FROM tmp_source WHERE has_member = 'N';

    IF v_no_member_cnt > 0 THEN
        DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 阶段5: 客户不存在 ' || v_no_member_cnt || ' 条');

        -- 写入 SENDING（仅 DATATYPE=0）
        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_SENDING
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_source WHERE has_member = ''N'' AND DATATYPE = 0';

        -- 写入 HIS（SENDSTATUS=0）
        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_HIS
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
             SENDSTATUS, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''0'', ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_source WHERE has_member = ''N'' AND DATATYPE = 0';

        -- 写入 PRIVI_REDPACACT（仅 REDID 在 REDPACACT 中存在的记录）
        EXECUTE IMMEDIATE '
            INSERT INTO EMALLUAT.PRIVI_REDPACACT
            (ID, CUSTTYPE, CUSTNO, REDID, OTHERDESC, OTHERID, CHANNELNO,
             DATASOURCE, OTHERACTNO, CREATEDATE, STATUS)
            SELECT EMALLUAT.S_PRIVI_REDPACACT.NEXTVAL,
                   ''HCNO'', HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO,
                   1, OTHERACTNO, ''' || v_today || ''', 0
            FROM tmp_source
            WHERE has_member = ''N'' AND DATATYPE = 0
              AND r_pacid IS NOT NULL';
    END IF;

    -- =================================================================
    -- 阶段6：窗口函数计算个人限制和总量限制
    -- 使用预聚合的 MYREDPAC 统计数据 JOIN，替代逐行关联子查询
    -- =================================================================
    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 阶段6: 窗口函数限制校验...');
    safe_drop('tmp_limit_checked');

    EXECUTE IMMEDIATE '
        CREATE GLOBAL TEMPORARY TABLE tmp_limit_checked ON COMMIT PRESERVE ROWS AS
        WITH personal_check AS (
            SELECT
                t.HCNO, t.REDID, t.OTHERDESC, t.OTHERID, t.CHANNELNO, t.OTHERACTNO,
                t.DATATYPE, t.MEMBERNO, t.pac_amt, t.limit_cnt, t.max_send,
                NVL(s.sent_cnt, 0) AS sent_by_member,
                ROW_NUMBER() OVER(PARTITION BY t.MEMBERNO, t.REDID ORDER BY t.OTHERID) AS rn_member,
                CASE
                    WHEN NVL(s.sent_cnt, 0) + ROW_NUMBER() OVER(PARTITION BY t.MEMBERNO, t.REDID ORDER BY t.OTHERID)
                         <= t.limit_cnt THEN ''Y''
                    ELSE ''N''
                END AS personal_ok
            FROM tmp_source t
            LEFT JOIN tmp_member_rp_stats s ON s.MEMBERNO = t.MEMBERNO AND s.PACID = t.REDID
            WHERE t.has_member = ''Y''
              AND t.pac_status = ''VALID''
        )
        SELECT
            pc.*,
            NVL(pt.total_sent, 0) AS sent_total,
            ROW_NUMBER() OVER(PARTITION BY pc.REDID ORDER BY pc.OTHERID) AS rn_total,
            CASE
                WHEN NVL(pt.total_sent, 0) + ROW_NUMBER() OVER(PARTITION BY pc.REDID ORDER BY pc.OTHERID)
                     <= pc.max_send THEN ''Y''
                ELSE ''N''
            END AS total_ok
        FROM personal_check pc
        LEFT JOIN tmp_pac_total_stats pt ON pt.PACID = pc.REDID';

    -- 统计各类结果
    SELECT COUNT(1) INTO v_success_cnt
    FROM tmp_limit_checked WHERE personal_ok = 'Y' AND total_ok = 'Y';

    SELECT COUNT(1) INTO v_over_limit_cnt
    FROM tmp_limit_checked WHERE personal_ok = 'N';

    SELECT COUNT(1) INTO v_over_max_cnt
    FROM tmp_limit_checked WHERE personal_ok = 'Y' AND total_ok = 'N';

    SELECT COUNT(1) INTO v_invalid_pac_cnt
    FROM tmp_source WHERE has_member = 'Y' AND pac_status != 'VALID';

    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 校验结果 — 成功候选: ' || v_success_cnt
                      || ' | 无效红包: ' || v_invalid_pac_cnt
                      || ' | 超出个人限制: ' || v_over_limit_cnt
                      || ' | 超出总量限制: ' || v_over_max_cnt);

    -- =================================================================
    -- 阶段7：写入失败记录（小数据量，单独提交，减少事务跨度）
    -- =================================================================

    -- 7.1 无效红包 → HIS(2) + PRIVI_REDPACACT
    IF v_invalid_pac_cnt > 0 THEN
        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_HIS
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
             SENDSTATUS, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''2'', ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_source
            WHERE has_member = ''Y'' AND pac_status != ''VALID'' AND DATATYPE = 0';

        EXECUTE IMMEDIATE '
            INSERT INTO EMALLUAT.PRIVI_REDPACACT
            (ID, CUSTTYPE, CUSTNO, REDID, OTHERDESC, OTHERID, CHANNELNO,
             DATASOURCE, OTHERACTNO, CREATEDATE, STATUS, MEMBERNO)
            SELECT EMALLUAT.S_PRIVI_REDPACACT.NEXTVAL,
                   ''HCNO'', HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO,
                   1, OTHERACTNO, ''' || v_today || ''', 0, MEMBERNO
            FROM tmp_source
            WHERE has_member = ''Y'' AND pac_status != ''VALID'' AND DATATYPE = 0
              AND r_pacid IS NOT NULL';
    END IF;

    -- 7.2 超出个人限制 → HIS(3) + SENDING 重试
    IF v_over_limit_cnt > 0 THEN
        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_SENDING
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_limit_checked WHERE personal_ok = ''N'' AND DATATYPE = 0';

        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_HIS
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
             SENDSTATUS, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''3'', ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_limit_checked WHERE personal_ok = ''N'' AND DATATYPE = 0';
    END IF;

    -- 7.3 超出红包总量限制 → HIS(4) + SENDING 重试
    IF v_over_max_cnt > 0 THEN
        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_SENDING
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_limit_checked
            WHERE personal_ok = ''Y'' AND total_ok = ''N'' AND DATATYPE = 0';

        EXECUTE IMMEDIATE '
            INSERT INTO ZHJFUAT.BZ_DATA_REDPACACT_HIS
            (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
             SENDSTATUS, DATADATE, CREATEDATE)
            SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                   ''4'', ''' || l_accdate || ''', ''' || v_today || '''
            FROM tmp_limit_checked
            WHERE personal_ok = ''Y'' AND total_ok = ''N'' AND DATATYPE = 0';
    END IF;

    -- 失败记录提交（数据量小，一次性提交安全）
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 阶段7: 失败记录已提交');

    -- =================================================================
    -- 阶段8：成功记录分批写入
    -- 核心变更：每 50 万行 COMMIT 一次 + MERGE 替代 INSERT+NOT EXISTS
    -- =================================================================
    IF v_success_cnt > 0 THEN
        DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 阶段8: 成功记录分批写入 '
                          || v_success_cnt || ' 条...');

        -- 构建仅包含成功记录且带 ROWNUM 的 GTT，用于分批读取
        -- 同时过滤 HIS 中已存在的记录，保证可重跑安全
        safe_drop('tmp_success');

        EXECUTE IMMEDIATE '
            CREATE GLOBAL TEMPORARY TABLE tmp_success ON COMMIT PRESERVE ROWS AS
            SELECT ROWNUM AS rn, t.*
            FROM tmp_limit_checked t
            WHERE t.personal_ok = ''Y'' AND t.total_ok = ''Y''
              AND NOT EXISTS (
                  SELECT 1 FROM ZHJFUAT.BZ_DATA_REDPACACT_HIS his
                  WHERE his.CHANNELNO  = t.CHANNELNO
                    AND his.OTHERID    = t.OTHERID
                    AND his.OTHERACTNO = t.OTHERACTNO
                    AND his.SENDSTATUS = ''1''
              )';

        -- 重新统计真正需要处理的成功记录数（排除 HIS 已存在的）
        SELECT COUNT(1) INTO v_success_cnt FROM tmp_success;

        IF v_success_cnt = 0 THEN
            DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 所有成功记录已在历史表中，跳过写入');
        ELSE
            v_batches := CEIL(v_success_cnt / v_batch_size);
            DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 分 ' || v_batches || ' 批处理，每批 '
                              || v_batch_size || ' 条');

            FOR i IN 1..v_batches LOOP
                v_start_rn   := (i - 1) * v_batch_size + 1;
                v_end_rn     := LEAST(i * v_batch_size, v_success_cnt);
                v_sub_batchid := v_batchid || '_' || LPAD(i, 4, '0');

                -- 当前批次实际行数
                SELECT COUNT(1) INTO v_batch_cnt
                FROM tmp_success WHERE rn BETWEEN v_start_rn AND v_end_rn;

                DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA]   批次 '
                                  || i || '/' || v_batches
                                  || ' | 行范围 ' || v_start_rn || '-' || v_end_rn
                                  || ' | 开始: ' || TO_CHAR(SYSDATE, 'HH24:MI:SS'));

                -- 8.1 MYREDPAC：写入用户红包
                EXECUTE IMMEDIATE '
                    INSERT INTO EMALLUAT.MYREDPAC
                    (MYPACID, MEMBERNO, PACID, PACSTATE, INITAMT, BELONGTYPE, CREATETIME, RECORDID, REFUNDTIMES)
                    SELECT EMALLUAT.S_MYREDPAC.NEXTVAL,
                           MEMBERNO, REDID,
                           ''1002'', pac_amt, ''1001'',
                           ''' || v_now2 || ''', NULL, 0
                    FROM tmp_success
                    WHERE rn BETWEEN ' || v_start_rn || ' AND ' || v_end_rn;

                -- 8.2 REDPACAPPLYDETAIL：写入发放明细
                EXECUTE IMMEDIATE '
                    INSERT INTO EMALLUAT.REDPACAPPLYDETAIL
                    (ID, PHONENO, BATCHID, MEMBERNO, EMAIL, CREATETIME, STATUS, CUSTNO)
                    SELECT EMALLUAT.S_REDPACAPPLYDETAIL.NEXTVAL,
                           NULL,
                           ''' || v_sub_batchid || ''',
                           MEMBERNO, NULL,
                           ''' || v_now1 || ''',
                           ''2'',
                           HCNO
                    FROM tmp_success
                    WHERE rn BETWEEN ' || v_start_rn || ' AND ' || v_end_rn;

                -- 8.3 REDPACAPPLY：按红包+渠道聚合写入申请批次
                EXECUTE IMMEDIATE '
                    INSERT INTO EMALLUAT.REDPACAPPLY
                    (ID, BATCHID, APPLYBY, APPLYTIME, SUMCOUNT, SUCCCOUNT, FAILCOUNT,
                     PACID, TYPE, SENDREASON, SENDACTNO)
                    SELECT EMALLUAT.S_REDPACAPPLY.NEXTVAL,
                           ''' || v_sub_batchid || ''',
                           CHANNELNO,
                           ''' || v_now1 || ''',
                           cnt, cnt, 0,
                           REDID,
                           ''4'',
                           OTHERDESC,
                           OTHERACTNO
                    FROM (
                        SELECT REDID, CHANNELNO, OTHERDESC, OTHERACTNO, COUNT(*) AS cnt
                        FROM tmp_success
                        WHERE rn BETWEEN ' || v_start_rn || ' AND ' || v_end_rn || '
                        GROUP BY REDID, CHANNELNO, OTHERDESC, OTHERACTNO
                    )';

                -- 8.4 HIS：MERGE 去重写入（替代 INSERT+NOT EXISTS）
                -- 单次 HASH ANTI JOIN，等效于 50 万条记录去重
                EXECUTE IMMEDIATE '
                    MERGE INTO ZHJFUAT.BZ_DATA_REDPACACT_HIS his
                    USING (
                        SELECT HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                               ''1'' AS SENDSTATUS,
                               ''' || l_accdate || ''' AS DATADATE,
                               ''' || v_today || ''' AS CREATEDATE
                        FROM tmp_success
                        WHERE rn BETWEEN ' || v_start_rn || ' AND ' || v_end_rn || '
                    ) src
                    ON (his.CHANNELNO  = src.CHANNELNO
                        AND his.OTHERID    = src.OTHERID
                        AND his.OTHERACTNO = src.OTHERACTNO)
                    WHEN NOT MATCHED THEN
                        INSERT (HCNO, REDID, OTHERDESC, OTHERID, CHANNELNO, OTHERACTNO,
                                SENDSTATUS, DATADATE, CREATEDATE)
                        VALUES (src.HCNO, src.REDID, src.OTHERDESC, src.OTHERID,
                                src.CHANNELNO, src.OTHERACTNO,
                                src.SENDSTATUS, src.DATADATE, src.CREATEDATE)';

                -- 8.5 删除 SENDING 中已成功处理的 DATATYPE=1 记录
                EXECUTE IMMEDIATE '
                    DELETE FROM ZHJFUAT.BZ_DATA_REDPACACT_SENDING s
                    WHERE EXISTS (
                        SELECT 1 FROM tmp_success t
                        WHERE t.rn BETWEEN ' || v_start_rn || ' AND ' || v_end_rn || '
                          AND t.DATATYPE = 1
                          AND t.HCNO = s.HCNO
                          AND t.REDID = s.REDID
                          AND t.OTHERID = s.OTHERID
                    )';

                -- 批次提交
                COMMIT;

                DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA]   批次 '
                                  || i || '/' || v_batches
                                  || ' 完成 | ' || v_batch_cnt || ' 条已提交 | '
                                  || TO_CHAR(SYSDATE, 'HH24:MI:SS'));
            END LOOP;
        END IF;

        safe_drop('tmp_success');
    END IF;

    -- =================================================================
    -- 阶段9：清理过期待送表数据
    -- =================================================================
    IF p_day = 0 THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ZHJFUAT.BZ_DATA_REDPACACT_SENDING';
    ELSE
        DELETE FROM ZHJFUAT.BZ_DATA_REDPACACT_SENDING
        WHERE CREATEDATE <= TO_CHAR(SYSDATE - p_day, 'YYYYMMDD');
    END IF;
    COMMIT;

    -- =================================================================
    -- 阶段10：清理临时表 + 输出处理摘要
    -- =================================================================
    safe_drop('tmp_member_rp_stats');
    safe_drop('tmp_pac_total_stats');
    safe_drop('tmp_source');
    safe_drop('tmp_limit_checked');

    DBMS_OUTPUT.PUT_LINE('==========================================');
    DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 处理完成 | 批次: ' || v_batchid);
    DBMS_OUTPUT.PUT_LINE('  源数据总量:        ' || v_total_source);
    DBMS_OUTPUT.PUT_LINE('  客户不存在:        ' || v_no_member_cnt);
    DBMS_OUTPUT.PUT_LINE('  无效红包:          ' || v_invalid_pac_cnt);
    DBMS_OUTPUT.PUT_LINE('  超出个人限制:      ' || v_over_limit_cnt);
    DBMS_OUTPUT.PUT_LINE('  超出总量限制:      ' || v_over_max_cnt);
    DBMS_OUTPUT.PUT_LINE('  成功发送:          ' || v_success_cnt);
    DBMS_OUTPUT.PUT_LINE('  分批提交批次数:    ' || v_batches);
    DBMS_OUTPUT.PUT_LINE('  清理超过 ' || p_day || ' 天的待送表数据');
    DBMS_OUTPUT.PUT_LINE('==========================================');

EXCEPTION
    WHEN OTHERS THEN
        v_error_batch := v_sub_batchid;
        v_error_info  := 'BATCH=' || v_error_batch || CHR(10)
                      || 'SQLCODE=' || SQLCODE || ' | SQLERRM=' || SQLERRM || CHR(10)
                      || 'BACKTRACE=' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

        -- 清理所有临时表
        safe_drop('tmp_member_rp_stats');
        safe_drop('tmp_pac_total_stats');
        safe_drop('tmp_source');
        safe_drop('tmp_limit_checked');
        safe_drop('tmp_success');

        ROLLBACK;

        -- 写入错误日志
        EXECUTE IMMEDIATE '
            INSERT INTO t_proc_oper_log(PROCNAME, execresult, errorinfo, createtime, datadate)
            VALUES(''P_BATCH_SEND_REDPAC_BIGDATA'', ''N'', '''
                || REPLACE(v_error_info, '''', '''''') || ''',
                TO_CHAR(SYSDATE, ''YYYY-MM-DD HH24:MI:SS''),
                ''' || v_today || ''')';

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('[P_BATCH_SEND_REDPAC_BIGDATA] 异常: ' || v_error_info);
        RAISE;
END P_BATCH_SEND_REDPAC_BIGDATA;
/
