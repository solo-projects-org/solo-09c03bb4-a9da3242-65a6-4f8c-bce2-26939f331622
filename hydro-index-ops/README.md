# 水文数仓 · 水位明细索引运维服务（CREATE INDEX CONCURRENTLY 批次化）+ 质控基线

在持续接收观测的 PostgreSQL `readings` 水位明细表上，以 **`CREATE INDEX CONCURRENTLY`**
构建普通复合索引 `(station_id, observed_at)`，通过 FastAPI + psycopg3 提供
**单批次受理、阶段查询、取消、失败/取消清理、重启恢复** 的完整运维闭环；
同时为质控场景提供 **比对基线的冻结与核验**：把某站某 UTC 时间段内
*当时已提交* 的水位明细原子地固定下来，随时核验记录是否被误改、误删或迟到补录。

## 为什么这样设计

- `CREATE INDEX CONCURRENTLY`（CIC）不持有阻止源表写入的长锁，构建期间
  `readings` 的普通读写照常；代价是失败后会留下 `indisvalid=false` 的索引残件，
  必须显式 `DROP INDEX CONCURRENTLY`，且 CIC 不能跑在事务块里。
- 因此系统用**持久化状态机 + 独立 autocommit 构建会话**驱动批次，保证：
  取消/失败后必须等构建会话结束、残件删净才进入终态，之后才接收新批；
  清理失败保持待处理状态自动重试；重启后先恢复再受理。
- 基线冻结把「头行 + 全部明细 + 计数」放在**同一个事务**提交：对外要么没有
  该基线、要么是完整基线，绝不发布半份；明细由单条 `INSERT...SELECT` 写入，
  语句级快照保证不混用不同时间点提交的记录。基线号全局唯一（主键），
  并发/重试/重启后的同号请求一律幂等返回首次完整基线，参数不同则 409。
- 核验是**单条 SQL**：被改写 / 消失 / 新增三类结果来自同一个已提交快照。

## 目录结构

```
hydro-index-ops/
├── docker-compose.yml          # db(postgres:16) + api + api2（双实例）
├── Dockerfile
├── requirements.txt
├── app/
│   ├── main.py                 # FastAPI 路由（Swagger: /docs）
│   ├── baselines.py            # 质控基线：冻结 / 查询 / 核验
│   ├── worker.py               # 批次状态机：构建/取消/清理/恢复
│   ├── sql_helpers.py          # 索引状态、CIC 进度、会话终止、批次 CRUD
│   ├── db.py                   # psycopg 异步连接池 + 批次表/基线表 DDL
│   ├── models.py               # Pydantic 模型
│   └── config.py               # 环境变量配置
├── sql/
│   ├── 01_init_readings.sql    # 水位样表 + 10 万行样例数据 + 人工索引
│   ├── 02_long_tx.sql          # 长事务样例（制造 CIC 构建等待）
│   └── 03_observe.sql          # 排障观察 SQL 速查
├── demo/
│   ├── demo.sh                 # 端到端演示场景（含基线场景 5）
│   └── reset_index.sql
├── docs/
│   ├── state-machine.md        # 索引批次状态机与关键保证
│   └── baseline-freeze-verify.md  # 基线冻结/核验的一致性设计
└── tests/
    ├── integration_test.py     # 索引批次端到端测试
    └── baseline_test.py        # 基线端到端测试（双实例/并发/重启）
```

## 快速启动

```bash
cd hydro-index-ops
docker compose up -d --build
# 等待 healthcheck 通过后：
open http://localhost:8000/docs          # Swagger UI（实例 1）
open http://localhost:8001/docs          # Swagger UI（实例 2，同样的 API）
curl -s http://localhost:8000/health | python3 -m json.tool
```

首次启动会自动建表并写入 10 个测站 × 1 万条/站 = 10 万行水位样例数据，
并创建一个**人工索引** `idx_readings_ingested_manual`，用于验证清理/恢复流程
绝不会误删非本系统创建的索引。

compose 同时启动 **两个 API 实例**（8000/8001 端口）：索引构建由 worker 咨询锁
保证全局只有一个实例驱动；基线冻结/核验与水位读写可由任意实例安全受理，
便于直接验证多实例并发下的一致性语义。

## API 一览（Swagger 中有完整 schema 与示例）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/health` | 健康检查、管理索引状态、是否可受理新批 |
| POST | `/index-batches` | 受理一个构建批次（活动批次存在或索引已 valid 时 409） |
| GET  | `/index-batches` | 批次历史（状态全部持久化在 `index_build_batches` 表） |
| GET  | `/index-batches/{id}` | 查状态/构建阶段（pg_stat_progress_create_index）/错误 |
| POST | `/index-batches/{id}/cancel` | 请求取消，终态前不允许再受理 |
| POST | `/readings` | 写入水位观测（构建/等待/清理期间始终可用） |
| GET  | `/readings?station_id=&start=&end=` | 按站号+时间查询 |
| GET  | `/readings/explain?station_id=` | EXPLAIN 验证复合索引是否生效 |
| POST | `/baselines` | **冻结基线**：保存该站该 UTC 段内当前已提交的快照（201 新建 / 200 幂等重放 / 409 参数冲突） |
| GET  | `/baselines` | 基线列表（最新冻结在前） |
| GET  | `/baselines/{no}` | 基线头：参数、条数、冻结时刻 |
| GET  | `/baselines/{no}/items` | 基线明细（冻结时保存的记录快照，分页） |
| GET  | `/baselines/{no}/verify` | **按基线核验**：同一已提交快照内列出被改写/消失/后来入库的记录 ID |
| POST | `/demo/long-tx?seconds=` | 开长事务制造 CIC 构建等待 |
| DELETE | `/demo/long-tx/{pid}` | 结束演示长事务（只认 `hydro-demo-longtx-*` 会话） |

## 质控基线：冻结与核验

质控员怀疑原始水位被误改、误删时，先**冻结**一份比对基线，之后随时**核验**：

```bash
# 1) 冻结：保存 1 号站 11-14 全天（UTC）当时已提交的明细快照
curl -s -X POST http://localhost:8000/baselines \
  -H 'Content-Type: application/json' \
  -d '{"baseline_no":"BL-20261114-01","station_id":1,
       "start":"2026-11-14T00:00:00Z","end":"2026-11-15T00:00:00Z"}'
# → 201 {"baseline_no":"BL-20261114-01","item_count":24,"reused":false,...}

# 2) 同号同站同范围重试（含另一实例、重启后）：幂等返回首次完整基线，不重采样
curl -s -X POST http://localhost:8001/baselines -H 'Content-Type: application/json' \
  -d '{"baseline_no":"BL-20261114-01","station_id":1,
       "start":"2026-11-14T00:00:00Z","end":"2026-11-15T00:00:00Z"}'
# → 200 {"reused":true,"item_count":24,...}（frozen_at 与首次一致）

# 3) 同号但参数不同 → 409，报文列出已冻结参数与本次请求参数
# 4) 核验：在同一已提交快照内比对基线与当前库
curl -s http://localhost:8001/baselines/BL-20261114-01/verify | python3 -m json.tool
# → {"rewritten":[{"reading_id":..,"baseline":{...},"current":{...}}],
#    "disappeared_ids":[...],"new_ids":[...],"counts":{...}}
```

语义约定（详见 `docs/baseline-freeze-verify.md`）：

- **改写**：基线记录仍在但任一快照字段（站号/观测时刻/水位/流量/质量）被改动；
  原记录被改到时间段**外**仍算改写（按记录 ID 比对，不按时间段重查）。
- **消失**：基线中的记录 ID 在当前库中已不存在（被删除）。
- **新增**：冻结后新入库且落在基线站号+时间段内的记录；迟到记录只算新增。
- 已提交基线**不可变**：不提供修改/删除接口；失败或重启后的同号重试也不会
  重新采样——只有从未完整提交的冻结才允许重试时重新采样。

## 演示场景

```bash
chmod +x demo/demo.sh
./demo/demo.sh all       # 启动并依次跑场景 1~5
# 或单独执行：./demo/demo.sh 1 | 2 | 3 | 4 | 5
```

1. **构建等待 → 成功**：先开持有 `readings` 快照的长事务，再受理批次；
   观察 CIC 停在等待阶段（`wait=Lock...`），期间读写照常；结束长事务后
   构建完成，EXPLAIN 显示走 `idx_readings_station_observed`。
2. **构建中取消**：等待阶段发起 cancel，状态经 `CANCELLING → CLEANING → CANCELLED`；
   期间受理新批返回 409；残件删净后才能受理新批；取消后的迟到完成同样删索引、
   绝不报成功。
3. **清理失败自动重试**：以 `HYDRO_CLEANUP_FAIL_FIRST=2` 注入前两次 DROP 失败
   （compose override 注入），观察批次停留在 `CLEANING`、`cleanup_attempts`
   递增、错误信息可查，源表读写与状态查询全程正常，重试成功后自动进入终态。
4. **重启恢复**：构建等待中 `docker compose restart api`；恢复在后台执行、
   不阻塞 API 就绪，先终止遗留构建会话（按 pid + application_name 双重校验，
   不断其他会话）、清净残件再进入终态；另演示孤儿构建迟到成功时索引被采纳
   保留，以及成功索引与人工索引在重启后均不受影响。
5. **质控基线**：双实例冻结/幂等重放/参数冲突 → 通过 SQL 模拟误改、误删、
   迟到补录 → 另一实例核验出被改写（含改到时间段外）、消失与新增的记录 ID。

手动排障见 `sql/03_observe.sql`：

```bash
docker exec -it hydro-db psql -U hydro -d hydro_warehouse
```

## 关键安全边界

- **只管理一个固定名称的索引** `idx_readings_station_observed`，所有
  DROP 都带该名称且 `IF EXISTS`，不触碰任何人工索引。
- **只终止自有构建会话**：`pg_cancel_backend` / `pg_terminate_backend`
  均带 `application_name LIKE 'hydro-cic-builder-%'` 双重身份校验。
- 批次表上有部分唯一索引，数据库层面兜底“同一时刻只有一个活动批次”。
- worker 用 PostgreSQL 咨询锁保证多副本部署时只有一个实例驱动状态机；
  锁由专用长连接持有并在同一会话释放。
- 状态全部落库（`index_build_batches` / `reading_baselines` /
  `reading_baseline_items`），进程重启不丢状态。
- 基线号主键 + 单事务冻结 + 单语句核验：多实例并发下不发布半份基线、
  不混用不同时间点的记录。

## 配置项（api 环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HYDRO_PG_DSN` | 指向 compose 内 db | PostgreSQL 连接串 |
| `HYDRO_WORK_POLL_INTERVAL` | `1.0` | 调度/阶段刷新间隔（秒） |
| `HYDRO_CLEANUP_RETRY_INTERVAL` | `3.0` | 清理失败重试间隔（秒） |
| `HYDRO_CLEANUP_FAIL_FIRST` | `0` | 演示用：前 N 次清理故意失败，生产必须为 0 |
| `HYDRO_LOG_LEVEL` | `INFO` | 日志级别 |

## 停止与清理

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 连同样例数据卷一并删除
docker exec -i hydro-db psql -U hydro -d hydro_warehouse < demo/reset_index.sql
```

## 集成测试（真实 PostgreSQL，非模拟）

`tests/integration_test.py` 启动一个**真实 PG 实例 + 真实 uvicorn 子进程**，
覆盖 40 项断言：正常构建与执行计划、长事务等待、取消与残件清理、清理失败
自动重试、SIGKILL 崩溃后的后台恢复（含孤儿构建迟到成功的采纳）、成功/人工
索引保留、批次持久化等。

`tests/baseline_test.py` 启动**两个真实 uvicorn 实例**，覆盖 68 项断言：
冻结原子性（头计数==物理行数）、同号幂等重放不重采样、参数冲突 409、
核验三分类（改写含改到时间段外/消失/迟到只算新增）、双实例并发冻结
（同参恰好一次 201、异参一胜一负 409）、冻结与写入并发下核验自洽、
SIGKILL 重启后同号重试不重采样，以及原有水位读写与索引批次回归。

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt httpx
# 准备一个可用的 PostgreSQL，并执行 sql/01_init_readings.sql，然后：
HYDRO_PG_DSN=postgresql://user:pass@host:5432/hydro_warehouse \
  python tests/integration_test.py
HYDRO_TEST_PG_DSN=postgresql://user:pass@host:5432/hydro_warehouse \
  python tests/baseline_test.py
```

两个测试默认连接 `127.0.0.1:55432/hydro_warehouse`（见文件顶部常量，可按环境改）。


