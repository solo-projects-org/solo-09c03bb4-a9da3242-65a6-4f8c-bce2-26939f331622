"""质控基线端到端测试：真实 PostgreSQL + 两个真实 uvicorn 实例。

覆盖：冻结原子性与幂等、参数冲突、核验三分类（改写/消失/新增）、
双实例并发冻结、冻结与写入并发、SIGKILL 重启后同号重试不重采样、
原有水位读写与索引批次回归。

前置：
  - PG 实例可连接（默认 127.0.0.1:55432，可用 HYDRO_TEST_PG_DSN 覆盖），
    库已执行 sql/01_init_readings.sql
  - 依赖已安装（fastapi/uvicorn/psycopg/httpx）

运行：python tests/baseline_test.py
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent.parent
BASE1 = "http://127.0.0.1:8011"
BASE2 = "http://127.0.0.1:8012"
DSN = os.environ.get(
    "HYDRO_TEST_PG_DSN", "postgresql://hydro@127.0.0.1:55432/hydro_warehouse"
)
IDX = "idx_readings_station_observed"
TAG = str(int(time.time()))  # 本次运行的基线号前缀，保证可重复执行

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {detail}")


class ApiServer:
    def __init__(self, port: int, extra_env: dict | None = None) -> None:
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.extra_env = extra_env or {}

    def start(self) -> None:
        env = os.environ.copy()
        env["HYDRO_PG_DSN"] = DSN
        env["HYDRO_WORK_POLL_INTERVAL"] = "0.5"
        env["HYDRO_CLEANUP_RETRY_INTERVAL"] = "1.0"
        env.update(self.extra_env)
        base = f"http://127.0.0.1:{self.port}"
        last_out = ""
        for _ in range(10):
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.main:app",
                 "--host", "127.0.0.1", "--port", str(self.port),
                 "--log-level", "warning"],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True,
            )
            for _ in range(80):
                if self.proc.poll() is not None:
                    last_out = self.proc.stdout.read() if self.proc.stdout else ""
                    self.proc = None
                    break
                try:
                    r = httpx.get(f"{base}/health", timeout=1)
                    if r.status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                time.sleep(0.3)
            if self.proc is not None and self.proc.poll() is None:
                return
            time.sleep(1.0)
        raise RuntimeError(f"API :{self.port} did not start. Last output:\n{last_out}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None


async def pg_exec(sql_text: str, params=None):
    conn = await psycopg.AsyncConnection.connect(
        DSN, autocommit=True, row_factory=dict_row
    )
    try:
        cur = await conn.execute(sql_text, params)
        try:
            return await cur.fetchall()
        except psycopg.ProgrammingError:
            return []
    finally:
        await conn.close()


def http(base: str, method: str, path: str, **kw) -> httpx.Response:
    return httpx.request(method, f"{base}{path}", timeout=20, **kw)


def freeze(base: str, no: str, station: int, start: str, end: str) -> httpx.Response:
    return http(base, "POST", "/baselines", json={
        "baseline_no": no, "station_id": station, "start": start, "end": end,
    })


async def seed_rows(station: int, rows: list[tuple[str, float]]) -> list[int]:
    """插入测试行，返回 readings.id 列表。rows: [(observed_at, water_level)]"""
    ids: list[int] = []
    conn = await psycopg.AsyncConnection.connect(
        DSN, autocommit=True, row_factory=dict_row
    )
    try:
        for observed_at, level in rows:
            cur = await conn.execute(
                "INSERT INTO readings (station_id, observed_at, water_level, "
                "discharge, quality) VALUES (%s, %s, %s, %s, 'GOOD') RETURNING id",
                (station, observed_at, level, 100.0 + level),
            )
            rec = await cur.fetchone()
            ids.append(rec["id"])
    finally:
        await conn.close()
    return ids


async def cleanup_fixtures() -> None:
    await pg_exec(
        "DELETE FROM readings WHERE station_id = 1 "
        "AND observed_at >= '2010-01-01+00' AND observed_at < '2011-01-01+00'"
    )
    await pg_exec("DELETE FROM reading_baselines WHERE baseline_no LIKE 'BT-%'")


async def current_count(station: int, start: str, end: str) -> int:
    rows = await pg_exec(
        "SELECT count(*) AS n FROM readings WHERE station_id = %s "
        "AND observed_at >= %s AND observed_at <= %s",
        (station, start, end),
    )
    return rows[0]["n"]


async def baseline_physical_count(no: str) -> int:
    rows = await pg_exec(
        "SELECT count(*) AS n FROM reading_baseline_items WHERE baseline_no = %s",
        (no,),
    )
    return rows[0]["n"]


# 测试窗口（远早于样例数据覆盖范围，互不干扰）
W1_S, W1_E = "2010-06-01T00:00:00Z", "2010-06-02T00:00:00Z"
W2_S, W2_E = "2010-08-01T00:00:00Z", "2010-08-02T00:00:00Z"
W3_S, W3_E = "2010-08-10T00:00:00Z", "2010-08-11T00:00:00Z"
W4_S, W4_E = "2010-09-01T00:00:00Z", "2010-09-03T00:00:00Z"
W5_S, W5_E = "2010-10-01T00:00:00Z", "2010-10-02T00:00:00Z"


async def main() -> int:
    print("场景 B0：双实例启动、健康检查与 Swagger")
    srv1, srv2 = ApiServer(8011), ApiServer(8012)
    srv1.start()
    srv2.start()
    await cleanup_fixtures()  # 基线表由 API 启动时创建，须在其后清理
    h1 = http(BASE1, "GET", "/health").json()
    h2 = http(BASE2, "GET", "/health").json()
    check("实例1 health ok", h1["status"] == "ok")
    check("实例2 health ok", h2["status"] == "ok")
    spec = http(BASE1, "GET", "/openapi.json").json()
    paths = spec["paths"]
    check("OpenAPI 含冻结接口", "/baselines" in paths and "post" in paths["/baselines"])
    check("OpenAPI 含核验接口", "/baselines/{baseline_no}/verify" in paths)
    check("Swagger UI 可访问", http(BASE2, "GET", "/docs").status_code == 200)

    print("场景 B1：参数校验")
    no1 = f"BT-{TAG}-W1"
    r = freeze(BASE1, no1, 1, "2010-06-01T00:00:00", W1_E)  # 无时区
    check("naive 时间被拒绝 422", r.status_code == 422, str(r.status_code))
    r = freeze(BASE1, no1, 1, W1_E, W1_S)  # start > end
    check("start>=end 被拒绝 422", r.status_code == 422)
    r = freeze(BASE1, "bad no!", 1, W1_S, W1_E)  # 非法基线号
    check("非法基线号被拒绝 422", r.status_code == 422)
    check("非法请求未留痕迹",
          http(BASE1, "GET", f"/baselines/{no1}").status_code == 404)

    print("场景 B2：冻结基线（实例1）→ 201，条数与库一致、头计数=物理行数")
    ids = await seed_rows(1, [
        ("2010-06-01T06:00:00Z", 21.0),
        ("2010-06-01T07:00:00Z", 21.1),
        ("2010-06-01T08:00:00Z", 21.2),
        ("2010-06-01T09:00:00Z", 21.3),
        ("2010-06-01T10:00:00Z", 21.4),
    ])
    r1, r2, r3, r4, r5 = ids
    r = freeze(BASE1, no1, 1, W1_S, W1_E)
    check("冻结返回 201", r.status_code == 201, r.text[:200])
    body = r.json()
    check("item_count=5", body["item_count"] == 5, str(body))
    check("reused=false", body["reused"] is False)
    check("头计数 == 物理行数", await baseline_physical_count(no1) == 5)
    items = http(BASE1, "GET", f"/baselines/{no1}/items?limit=10").json()
    check("明细 5 条且 ID 集合正确", sorted(i["reading_id"] for i in items) == ids)
    check("明细含水位/流量/质量",
          all(set(("water_level", "discharge", "quality")) <= set(i) for i in items))
    lst = http(BASE2, "GET", "/baselines?limit=50").json()
    check("列表含新基线", any(b["baseline_no"] == no1 for b in lst))
    hdr = http(BASE2, "GET", f"/baselines/{no1}").json()
    check("基线头起止正确", hdr["start"].startswith("2010-06-01") and
          hdr["end"].startswith("2010-06-02"), str(hdr))
    check("查不存在基线 404", http(BASE1, "GET", "/baselines/NOPE-1").status_code == 404)
    check("核验不存在基线 404",
          http(BASE1, "GET", "/baselines/NOPE-1/verify").status_code == 404)

    print("场景 B3：无篡改时核验 → 三类皆空")
    v = http(BASE2, "GET", f"/baselines/{no1}/verify").json()
    check("rewritten 为空", v["rewritten"] == [], str(v))
    check("disappeared 为空", v["disappeared_ids"] == [])
    check("new 为空", v["new_ids"] == [])
    check("counts 全 0",
          v["counts"] == {"rewritten": 0, "disappeared": 0, "new": 0})

    print("场景 B4：同号同站同范围重试（实例2）→ 幂等返回首次基线，不重采样")
    cur = await pg_exec(
        "INSERT INTO readings (station_id, observed_at, water_level, quality) "
        "VALUES (1, '2010-06-01T11:00:00+00', 99.9, 'GOOD') RETURNING id"
    )
    late_id = cur[0]["id"]
    r = freeze(BASE2, no1, 1, W1_S, W1_E)
    check("重放返回 200", r.status_code == 200, r.text[:200])
    body2 = r.json()
    check("reused=true", body2["reused"] is True)
    check("item_count 仍为 5（未重采样）", body2["item_count"] == 5, str(body2))
    check("frozen_at 与首次一致", body2["frozen_at"] == body["frozen_at"])
    check("物理行数仍为 5", await baseline_physical_count(no1) == 5)
    v = http(BASE1, "GET", f"/baselines/{no1}/verify").json()
    check("迟到记录只算新增", v["new_ids"] == [late_id], str(v["new_ids"]))
    check("迟到记录不算改写/消失",
          v["rewritten"] == [] and v["disappeared_ids"] == [])

    print("场景 B5：同号不同参数 → 409 冲突")
    r = freeze(BASE1, no1, 1, W1_S, "2010-06-02T01:00:00Z")
    check("改 end → 409", r.status_code == 409, str(r.status_code))
    check("409 报文含 existing/requested",
          "existing" in r.json()["detail"] and "requested" in r.json()["detail"])
    r = freeze(BASE2, no1, 2, W1_S, W1_E)
    check("改 station → 409", r.status_code == 409)
    r = freeze(BASE1, no1, 1, "2010-05-31T23:00:00Z", W1_E)
    check("改 start → 409", r.status_code == 409)
    check("冲突后基线未被改动", (await baseline_physical_count(no1)) == 5)

    print("场景 B6：篡改后核验 → 改写/消失/新增三分类")
    await pg_exec("UPDATE readings SET water_level = water_level + 10 WHERE id = %s",
                  (r1,))
    await pg_exec("UPDATE readings SET observed_at = '2010-07-01T00:00:00+00' "
                  "WHERE id = %s", (r2,))  # 改到时间段外
    await pg_exec("DELETE FROM readings WHERE id = %s", (r3,))
    await pg_exec("UPDATE readings SET quality = 'BAD', discharge = discharge + 5 "
                  "WHERE id = %s", (r4,))
    await pg_exec("UPDATE readings SET station_id = 2 WHERE id = %s", (r5,))
    await pg_exec(
        "INSERT INTO readings (station_id, observed_at, water_level, quality) "
        "VALUES (1, '2010-06-03T00:00:00+00', 50.0, 'GOOD')"  # 窗口外插入
    )
    v = http(BASE2, "GET", f"/baselines/{no1}/verify").json()
    rw_ids = sorted(x["reading_id"] for x in v["rewritten"])
    check("改写集合 = {r1, r2(改出窗口), r4, r5(改站号)}",
          rw_ids == sorted([r1, r2, r4, r5]), str(rw_ids))
    rw1 = next(x for x in v["rewritten"] if x["reading_id"] == r1)
    check("r1 改写含基线/现值",
          abs(rw1["baseline"]["water_level"] + 10 - rw1["current"]["water_level"]) < 1e-6,
          str(rw1))
    rw2 = next(x for x in v["rewritten"] if x["reading_id"] == r2)
    check("r2 改到时间段外仍算改写（observed_at 已变）",
          rw2["current"]["observed_at"].startswith("2010-07-01"), str(rw2))
    check("r2 不算消失也不算新增",
          r2 not in v["disappeared_ids"] and r2 not in v["new_ids"])
    rw4 = next(x for x in v["rewritten"] if x["reading_id"] == r4)
    check("r4 质量/流量变化被捕捉",
          rw4["current"]["quality"] == "BAD" and
          abs(rw4["baseline"]["discharge"] + 5 - rw4["current"]["discharge"]) < 1e-6)
    rw5 = next(x for x in v["rewritten"] if x["reading_id"] == r5)
    check("r5 站号被改仍算改写（不算消失/新增）",
          rw5["baseline"]["station_id"] == 1 and rw5["current"]["station_id"] == 2
          and r5 not in v["disappeared_ids"] and r5 not in v["new_ids"], str(rw5))
    check("消失集合 = {r3}", v["disappeared_ids"] == [r3], str(v["disappeared_ids"]))
    check("新增集合 = {迟到行}（窗口外插入不计）", v["new_ids"] == [late_id],
          str(v["new_ids"]))
    check("counts 与列表一致",
          v["counts"] == {"rewritten": 4, "disappeared": 1, "new": 1}, str(v["counts"]))

    print("场景 B7：双实例并发冻结（同号同参 ×8）→ 恰好一次 201，其余 200，无半份")
    no2 = f"BT-{TAG}-W2"
    await seed_rows(1, [("2010-08-01T06:00:00Z", 30.0),
                        ("2010-08-01T07:00:00Z", 30.1),
                        ("2010-08-01T08:00:00Z", 30.2)])
    def do_freeze(i: int) -> httpx.Response:
        return freeze(BASE1 if i % 2 == 0 else BASE2, no2, 1, W2_S, W2_E)
    with ThreadPoolExecutor(max_workers=8) as ex:
        rs = list(ex.map(do_freeze, range(8)))
    codes = sorted(r.status_code for r in rs)
    check("状态码 = 1×201 + 7×200", codes == [200] * 7 + [201], str(codes))
    bodies = [r.json() for r in rs]
    check("所有响应 item_count 一致且=3",
          all(b["item_count"] == 3 for b in bodies), str(bodies))
    check("所有响应 frozen_at 一致",
          len({b["frozen_at"] for b in bodies}) == 1)
    check("恰有一次 reused=false", sum(1 for b in bodies if not b["reused"]) == 1)
    check("库中仅一份基线", (await pg_exec(
        "SELECT count(*) AS n FROM reading_baselines WHERE baseline_no = %s",
        (no2,)))[0]["n"] == 1)
    check("头计数 == 物理行数（无半份基线）", await baseline_physical_count(no2) == 3)

    print("场景 B8：双实例并发冻结（同号不同参）→ 一胜一负 409")
    no3 = f"BT-{TAG}-W3"
    await seed_rows(1, [("2010-08-10T06:00:00Z", 31.0)])
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(freeze, BASE1, no3, 1, W3_S, W3_E)
        f2 = ex.submit(freeze, BASE2, no3, 1, W3_S, "2010-08-11T01:00:00Z")
        codes = sorted([f1.result().status_code, f2.result().status_code])
    check("状态码 = [201, 409]", codes == [201, 409], str(codes))
    check("落败方未覆盖胜出基线", (await pg_exec(
        "SELECT range_end FROM reading_baselines WHERE baseline_no = %s",
        (no3,)))[0]["range_end"].isoformat().startswith("2010-08-1"))

    print("场景 B9：冻结与水位写入并发 → 原子快照，核验自洽")
    no4 = f"BT-{TAG}-W4"
    await seed_rows(1, [("2010-09-01T06:00:00Z", 40.0),
                        ("2010-09-01T07:00:00Z", 40.1)])
    stop_writer = threading.Event()
    writer_errors: list[str] = []

    def writer() -> None:
        n = 0
        while not stop_writer.is_set():
            try:
                http(BASE2, "POST", "/readings", json={
                    "station_id": 1,
                    "observed_at": f"2010-09-02T00:{n % 60:02d}:00Z",
                    "water_level": 41.0 + n * 0.001,
                }).raise_for_status()
                n += 1
            except Exception as exc:  # noqa: BLE001
                writer_errors.append(str(exc))
            time.sleep(0.03)

    wt = threading.Thread(target=writer, daemon=True)
    wt.start()
    time.sleep(0.2)  # 让写入先跑起来
    r = freeze(BASE1, no4, 1, W4_S, W4_E)
    stop_writer.set()
    wt.join(timeout=10)
    check("并发写入期间冻结成功 201", r.status_code == 201, r.text[:200])
    check("写入线程无报错", not writer_errors, str(writer_errors[:2]))
    fr = r.json()
    check("头计数 == 物理行数（原子提交）",
          await baseline_physical_count(no4) == fr["item_count"],
          f"header={fr['item_count']} physical={await baseline_physical_count(no4)}")
    v = http(BASE2, "GET", f"/baselines/{no4}/verify").json()
    check("无篡改 → 改写/消失为空",
          v["rewritten"] == [] and v["disappeared_ids"] == [], str(v)[:300])
    total_now = await current_count(1, W4_S, W4_E)
    check("核验自洽：当前条数 == 基线条数 + 新增条数",
          total_now == fr["item_count"] + len(v["new_ids"]),
          f"now={total_now} base={fr['item_count']} new={len(v['new_ids'])}")

    print("场景 B10：SIGKILL 重启后同号重试不重采样")
    no5 = f"BT-{TAG}-W5"
    await seed_rows(1, [("2010-10-01T06:00:00Z", 45.0),
                        ("2010-10-01T07:00:00Z", 45.1)])
    r = freeze(BASE1, no5, 1, W5_S, W5_E)
    check("重启前冻结 201", r.status_code == 201)
    frozen_at0 = r.json()["frozen_at"]
    await pg_exec(
        "INSERT INTO readings (station_id, observed_at, water_level, quality) "
        "VALUES (1, '2010-10-01T08:00:00+00', 46.0, 'GOOD')"
    )
    srv1.stop(sig=signal.SIGKILL)
    srv1 = ApiServer(8011)
    srv1.start()
    r = freeze(BASE1, no5, 1, W5_S, W5_E)
    check("重启后同号重试 200（幂等）", r.status_code == 200, r.text[:200])
    check("item_count 仍为 2（未重采样）", r.json()["item_count"] == 2, str(r.json()))
    check("frozen_at 保持首次值", r.json()["frozen_at"] == frozen_at0)
    check("物理行数仍为 2", await baseline_physical_count(no5) == 2)

    print("场景 B11：原有水位读写与索引批次回归")
    r = http(BASE1, "POST", "/readings", json={
        "station_id": 2, "observed_at": "2026-11-15T08:00:00Z", "water_level": 19.5})
    check("写入 readings 201", r.status_code == 201, r.text[:200])
    new_rid = r.json()["id"]
    rr = http(BASE2, "GET",
              "/readings?station_id=2&start=2026-11-15T07:59:00Z"
              "&end=2026-11-15T08:01:00Z&limit=10")
    check("查询 readings 200 且含新行",
          rr.status_code == 200 and any(x["id"] == new_rid for x in rr.json()))
    check("explain 可用",
          http(BASE1, "GET", "/readings/explain?station_id=2").status_code == 200)
    await pg_exec(f"DROP INDEX CONCURRENTLY IF EXISTS {IDX}")
    # 等无活动批次（双实例下由持锁 worker 驱动）
    for _ in range(60):
        if http(BASE1, "GET", "/health").json()["accepting_new_batch"]:
            break
        time.sleep(0.5)
    r = http(BASE2, "POST", "/index-batches", json={"note": "baseline-regression"})
    check("受理索引批次 201", r.status_code == 201, r.text[:200])
    bid = r.json()["id"]
    cr = http(BASE1, "POST", f"/index-batches/{bid}/cancel")
    check("取消批次受理", cr.status_code == 200, cr.text[:200])
    deadline = time.time() + 90
    st = ""
    while time.time() < deadline:
        st = http(BASE1, "GET", f"/index-batches/{bid}").json()["state"]
        if st == "CANCELLED":
            break
        time.sleep(0.5)
    check("批次终态 CANCELLED", st == "CANCELLED", st)

    srv1.stop()
    srv2.stop()
    print(f"\n结果：{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
