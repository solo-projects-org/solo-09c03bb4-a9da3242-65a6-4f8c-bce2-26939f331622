# 质控基线：冻结与核验的一致性设计

质控场景：怀疑测站原始水位被误改、误删，需要把「某站某 UTC 时间段内、
当时已提交的 readings 明细」固定为比对基线，之后随时核验当前库与基线的差异。

## 数据模型

```sql
reading_baselines (
    baseline_no  TEXT PRIMARY KEY,        -- 基线号，全局唯一
    station_id   BIGINT NOT NULL,
    range_start  TIMESTAMPTZ NOT NULL,    -- UTC 起（含）
    range_end    TIMESTAMPTZ NOT NULL,    -- UTC 止（含）
    item_count   INTEGER NOT NULL,        -- 冻结条数（与明细物理行数一致）
    frozen_at    TIMESTAMPTZ NOT NULL     -- 基线提交时刻
)

reading_baseline_items (
    baseline_no TEXT REFERENCES reading_baselines ON DELETE CASCADE,
    reading_id  BIGINT,                   -- 冻结时刻 readings.id
    station_id, observed_at, water_level, discharge, quality,
    PRIMARY KEY (baseline_no, reading_id)
)
```

明细**不带**指向 readings 的外键：原记录日后可能被删除，
这正是核验要发现的「消失」。基线提交后不可变，不提供任何修改/删除接口。

## 冻结：POST /baselines

### 原子性——不发布半份基线

一次冻结 = 同一事务内的三条语句：

1. `INSERT INTO reading_baselines ...`（头行，item_count 暂为 0）
2. `INSERT INTO reading_baseline_items ... SELECT ... FROM readings
   WHERE station_id AND observed_at 在范围内`（明细）
3. `UPDATE reading_baselines SET item_count = (SELECT count(*) ...)`（计数）

事务要么整体提交、要么整体回滚。任何并发读者（其他实例的查询/核验）
只能看到「没有该基线」或「完整基线」，永远看不到半份。进程崩溃、
容器重启只会留下回滚痕迹（即什么都没有），同号重试会重新走完整冻结流程。

### 快照一致性——不混用不同时间点的记录

明细由**单条** `INSERT...SELECT` 写入。PostgreSQL 中一条语句只看到一个
已提交快照（READ COMMITTED 下为语句开始时刻），因此同一份基线的所有明细
都来自同一个时间点的已提交数据，不会混用冻结期间陆续提交的行。

### 幂等与冲突——同号重试不重采样

`baseline_no` 是主键。处理流程：

1. 先读：已存在完整基线 → 参数（站号/起止）一致则 200 幂等返回
   （`reused=true`，不重新采样）；不一致则 409（报文附已冻结参数与本次参数）。
2. 不存在则走上述事务冻结，201 返回。
3. 并发同号冻结：唯一约束会让后到事务**等待**先者提交/回滚后才报错
   `UniqueViolation`；此时重读必然看到先者已提交的完整基线，按第 1 步
   同样的规则幂等返回或 409。

推论：

- 两个 API 实例同时冻结同号同参数 → 恰好一个 201，其余 200，库中只有一份；
- 同号不同参数并发 → 先提交者 201，其余 409，落败方不会覆盖胜出基线；
- 失败/重启后的同号重试：若首次已提交，返回首次完整基线（不重采样）；
  若首次未提交（回滚），则重新冻结——「只取首次**完整**基线」。

## 核验：GET /baselines/{no}/verify

核验是**单条 SQL**（CTE），`rewritten / disappeared / new` 三类结果
来自同一个已提交快照；基线头提交后不可变，其读取不影响一致性。

分类语义：

| 类别 | 判定 | 说明 |
|---|---|---|
| 改写 rewritten | 基线记录 ID 仍存在，但站号/观测时刻/水位/流量/质量任一不同（`IS DISTINCT FROM`） | 按记录 ID 比对，**不受时间段限制**：原记录被改到时间段外仍算改写 |
| 消失 disappeared | 基线记录 ID 在当前库中不存在 | 被删除 |
| 新增 new | 当前库中落在基线站号+时间段内、且 ID 不在基线中的记录 | 迟到记录只算新增；窗口外插入不计 |

边界情形的归属（均有测试锁定）：

- 原记录被改到时间段外 → 改写（不算消失、不算新增）；
- 迟到补录（观测时刻在窗口内、冻结后入库）→ 新增；
- 窗口外的记录被改入窗口内 → 其 ID 不在基线中，按 ID 集合语义计为新增；
- 改写记录不会重复计入新增（ID 已在基线中）。

`rewritten` 附带基线值与当前值，便于质控定位字段级差异。

## 多实例并发

- 冻结/核验/水位写入都是普通事务，一致性完全由数据库保证
  （主键、单事务原子性、语句级快照），任意数量的 API 实例都安全；
- 索引构建批次仍由 worker 咨询锁保证全局单实例驱动，与基线互不影响；
- 两个实例同时启动时的建表 DDL 由另一个咨询锁串行化。

## 性能说明

- 冻结明细按 `(station_id, observed_at)` 范围扫描：管理索引建成后走索引，
  未建成时顺序扫描（样例 10 万行量级无感）；
- 核验按 `baseline_no` 走明细主键、按 `reading_id` 走 readings 主键，
  新增检测走范围扫描 + 反连接，均为线性于基线规模；
- 冻结期间对 readings 只读，核验只读，均不阻塞水位写入。
