# batch-send-redpac

Oracle（OceanBase 兼容）存储过程 —— 批量发送红包业务处理。

## 文件说明

- `P_BATCH_SEND_REDPAC.sql` —— 包含建表 DDL、必需索引、存储过程 `P_BATCH_SEND_REDPAC`（v2 重构版）

## 上线前置条件

存储过程执行前，必须先在目标库创建索引（DDL 见 SQL 文件 404-422 行）：

| 优先级 | 索引 | 所属表 |
|--------|------|--------|
| P0 | `IDX_MEMBER_HOSTCUSTNO` | `EMALLUAT.MEMBER` |
| P0 | `IDX_MYREDPAC_MEMBER_PAC` | `EMALLUAT.MYREDPAC` |
| P0 | `IDX_MYREDPAC_PACID` | `EMALLUAT.MYREDPAC` |
| P0 | `IDX_REDPACACT_HIS_DEDUP` | `ZHJFUAT.BZ_DATA_REDPACACT_HIS` |
| P1 | `IDX_REDPACAPPLY_BATCHID` | `EMALLUAT.REDPACAPPLY` |
| P1 | `IDX_REDPACAPPLYDETAIL_BID` | `EMALLUAT.REDPACAPPLYDETAIL` |
| P1 | `IDX_SENDING_CREATEDATE` | `ZHJFUAT.BZ_DATA_REDPACACT_SENDING` |
| P1 | `IDX_SENDING_DEL` | `ZHJFUAT.BZ_DATA_REDPACACT_SENDING` |

## 部署

在 OceanBase Oracle 模式下使用 SQL 客户端执行整个 .sql 文件，按顺序创建表、索引、存储过程。

## 执行

```sql
-- 默认清理 30 天前数据
CALL P_BATCH_SEND_REDPAC();

-- 清空待送表（p_day=0）
CALL P_BATCH_SEND_REDPAC(0);
```

## v2 重构变更

1. 修复 `TO_CHAR` 格式掩码 `mm→mi`（原版将月份写入分钟字段）
2. 预聚合 `MYREDPAC` 统计替代逐行关联子查询
3. `REDPACAPPLY` 直接写入正确计数，去掉 INSERT-then-UPDATE 反模式
4. `REDPACAPPLYDETAIL` 直接写入 `STATUS='2'`，去掉二次 UPDATE
