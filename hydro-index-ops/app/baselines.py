"""质控基线：冻结（一次性原子快照）与核验（单一已提交快照比对）。

一致性设计：
- 冻结 = 同一事务内的「头行 INSERT + 明细 INSERT...SELECT + 计数 UPDATE」，
  要么整体提交、要么整体回滚；任何时刻对外要么没有该基线、要么是完整基线，
  绝不发布“半份基线”。进程崩溃/重启只会留下回滚痕迹（即什么都没有），
  同号重试会重新走完整冻结流程——只有已提交的完整基线才参与幂等。
- 明细由**单条** INSERT...SELECT 写入：该语句自身只看到一个已提交快照，
  不会混用不同时间点提交的 readings 记录。
- baseline_no 全局唯一（主键）。并发同号冻结时，唯一约束会等待对方事务
  结束才放行/报错：胜出者提交完整基线，落败者重读后——参数一致则幂等
  返回该基线（不重新采样），参数不同则 409。已提交基线永远不被改写，
  失败与重启后的同号重试也不会重采样。
- 核验是**单条 SQL**（CTE）：被改写 / 消失 / 新增三类结果来自同一个
  已提交快照；基线头提交后不可变，其读取不影响一致性。
- 语义约定：核验一律按记录 ID 比对——原记录被改到时间段外仍算“改写”；
  冻结后入库且落在基线时间段内的记录（含迟到记录）只算“新增”。
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from psycopg_pool import AsyncConnectionPool

from . import db
from .models import (
    BaselineFreezeIn,
    BaselineItemOut,
    BaselineOut,
    BaselineVerifyOut,
    ReadingValues,
    RewrittenItem,
    VerifyCounts,
)

log = logging.getLogger("hydro.baseline")

router = APIRouter(prefix="/baselines", tags=["baselines"])

# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #

SQL_GET_HEADER = """
SELECT baseline_no, station_id, range_start, range_end, item_count, frozen_at
FROM reading_baselines
WHERE baseline_no = %(no)s
"""

SQL_LIST_HEADERS = """
SELECT baseline_no, station_id, range_start, range_end, item_count, frozen_at
FROM reading_baselines
ORDER BY frozen_at DESC, baseline_no
LIMIT %(limit)s
"""

SQL_INSERT_HEADER = """
INSERT INTO reading_baselines
    (baseline_no, station_id, range_start, range_end, item_count)
VALUES (%(no)s, %(sid)s, %(start)s, %(end)s, 0)
"""

# 单条 INSERT...SELECT：语句级快照保证所有明细来自同一个已提交快照
SQL_INSERT_ITEMS = """
INSERT INTO reading_baseline_items
    (baseline_no, reading_id, station_id, observed_at, water_level, discharge, quality)
SELECT %(no)s, r.id, r.station_id, r.observed_at, r.water_level, r.discharge, r.quality
FROM readings r
WHERE r.station_id = %(sid)s
  AND r.observed_at >= %(start)s
  AND r.observed_at <= %(end)s
"""

SQL_UPDATE_COUNT = """
UPDATE reading_baselines b
SET item_count = (
    SELECT count(*) FROM reading_baseline_items i
    WHERE i.baseline_no = %(no)s
)
WHERE b.baseline_no = %(no)s
"""

SQL_LIST_ITEMS = """
SELECT reading_id, station_id, observed_at, water_level, discharge, quality
FROM reading_baseline_items
WHERE baseline_no = %(no)s
ORDER BY reading_id
LIMIT %(limit)s OFFSET %(offset)s
"""

# 核验：单条语句 => 单一已提交快照。
#   cmp       基线明细 LEFT JOIN 当前 readings（按记录 ID，不受时间段限制：
#             原记录被改到时间段外仍算改写）
#   new_rows  当前库中落在基线站号+时间段内、但不在基线中的记录（迟到只算新增）
SQL_VERIFY = """
WITH hdr AS (
    SELECT baseline_no, station_id, range_start, range_end, item_count, frozen_at
    FROM reading_baselines
    WHERE baseline_no = %(no)s
),
base AS (
    SELECT i.reading_id, i.station_id, i.observed_at, i.water_level,
           i.discharge, i.quality
    FROM reading_baseline_items i
    WHERE i.baseline_no = %(no)s
),
cmp AS (
    SELECT b.reading_id,
           b.station_id  AS b_station_id,
           b.observed_at AS b_observed_at,
           b.water_level AS b_water_level,
           b.discharge   AS b_discharge,
           b.quality     AS b_quality,
           r.station_id  AS c_station_id,
           r.observed_at AS c_observed_at,
           r.water_level AS c_water_level,
           r.discharge   AS c_discharge,
           r.quality     AS c_quality,
           (r.id IS NULL) AS disappeared,
           (r.id IS NOT NULL AND (
                r.station_id  IS DISTINCT FROM b.station_id OR
                r.observed_at IS DISTINCT FROM b.observed_at OR
                r.water_level IS DISTINCT FROM b.water_level OR
                r.discharge   IS DISTINCT FROM b.discharge OR
                r.quality     IS DISTINCT FROM b.quality)) AS rewritten
    FROM base b
    LEFT JOIN readings r ON r.id = b.reading_id
),
new_rows AS (
    SELECT r.id
    FROM readings r
    WHERE r.station_id = (SELECT station_id FROM hdr)
      AND r.observed_at >= (SELECT range_start FROM hdr)
      AND r.observed_at <= (SELECT range_end FROM hdr)
      AND NOT EXISTS (SELECT 1 FROM base b WHERE b.reading_id = r.id)
)
SELECT
    (SELECT jsonb_build_object(
        'baseline_no', baseline_no, 'station_id', station_id,
        'start', range_start, 'end', range_end,
        'item_count', item_count, 'frozen_at', frozen_at)
     FROM hdr) AS header,
    (SELECT coalesce(jsonb_agg(payload ORDER BY reading_id), '[]'::jsonb)
     FROM (
        SELECT reading_id,
               jsonb_build_object(
                   'reading_id', reading_id,
                   'baseline', jsonb_build_object(
                       'station_id', b_station_id, 'observed_at', b_observed_at,
                       'water_level', b_water_level, 'discharge', b_discharge,
                       'quality', b_quality),
                   'current', jsonb_build_object(
                       'station_id', c_station_id, 'observed_at', c_observed_at,
                       'water_level', c_water_level, 'discharge', c_discharge,
                       'quality', c_quality)
               ) AS payload
        FROM cmp WHERE rewritten
     ) t) AS rewritten,
    (SELECT coalesce(jsonb_agg(reading_id ORDER BY reading_id), '[]'::jsonb)
     FROM cmp WHERE disappeared) AS disappeared_ids,
    (SELECT coalesce(jsonb_agg(id ORDER BY id), '[]'::jsonb)
     FROM new_rows) AS new_ids
"""


# --------------------------------------------------------------------------- #
# 依赖与辅助
# --------------------------------------------------------------------------- #

def get_pool() -> AsyncConnectionPool:
    if db.pool is None:  # pragma: no cover
        raise HTTPException(503, "database pool not initialized")
    return db.pool


def _header_to_out(row: dict, *, reused: bool) -> BaselineOut:
    return BaselineOut(
        baseline_no=row["baseline_no"],
        station_id=row["station_id"],
        start=row["range_start"],
        end=row["range_end"],
        item_count=row["item_count"],
        frozen_at=row["frozen_at"],
        reused=reused,
    )


def _same_params(row: dict, body: BaselineFreezeIn) -> bool:
    """已提交基线与本次请求的参数是否完全一致（时刻按绝对时间比较）。"""
    return (
        row["station_id"] == body.station_id
        and row["range_start"] == body.start
        and row["range_end"] == body.end
    )


def _reuse_or_conflict(row: dict, body: BaselineFreezeIn, response: Response) -> BaselineOut:
    if not _same_params(row, body):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "error": "baseline number already frozen with different parameters",
                "baseline_no": body.baseline_no,
                "existing": {
                    "station_id": row["station_id"],
                    "start": row["range_start"].isoformat(),
                    "end": row["range_end"].isoformat(),
                },
                "requested": {
                    "station_id": body.station_id,
                    "start": body.start.isoformat(),
                    "end": body.end.isoformat(),
                },
            },
        )
    # 同号同站同范围：幂等返回首次完整基线，绝不重新采样
    response.status_code = status.HTTP_200_OK
    return _header_to_out(row, reused=True)


async def _fetch_header(conn: psycopg.AsyncConnection, baseline_no: str) -> dict | None:
    cur = await conn.execute(SQL_GET_HEADER, {"no": baseline_no})
    return await cur.fetchone()


# --------------------------------------------------------------------------- #
# 冻结
# --------------------------------------------------------------------------- #
@router.post(
    "",
    response_model=BaselineOut,
    status_code=status.HTTP_201_CREATED,
    summary="冻结基线：保存该站该 UTC 时间段内当前已提交的 readings 快照",
    description=(
        "按基线号+站号+UTC 起止时刻，把**当前已提交**的 readings 记录"
        "（ID、站号、观测时刻、水位、流量、质量）原子地保存为比对基线。\n\n"
        "- 头行与全部明细在同一事务提交，任何时刻都不会出现半份基线；\n"
        "- 同号同站同范围的重试（含并发、失败/重启后）返回首次完整基线，"
        "不重新采样（`reused=true`，HTTP 200）；\n"
        "- 同号但参数（站号/起止）不同返回 409；\n"
        "- 已提交基线不可变，不提供修改/删除接口。"
    ),
)
async def freeze_baseline(
    body: BaselineFreezeIn,
    response: Response,
    p: AsyncConnectionPool = Depends(get_pool),
) -> BaselineOut:
    params = {
        "no": body.baseline_no,
        "sid": body.station_id,
        "start": body.start,
        "end": body.end,
    }
    async with p.connection() as conn:
        # 快路径：已存在完整基线 → 幂等返回或 409
        existing = await _fetch_header(conn, body.baseline_no)
        await conn.commit()
        if existing is not None:
            return _reuse_or_conflict(existing, body, response)

        try:
            async with conn.transaction():
                await conn.execute(SQL_INSERT_HEADER, params)
                await conn.execute(SQL_INSERT_ITEMS, params)
                await conn.execute(SQL_UPDATE_COUNT, params)
        except psycopg.errors.UniqueViolation:
            # 并发同号冻结：唯一约束会等对方事务结束才报错，此刻重读
            # 必然能看到对方已提交的完整基线（绝不重采样、绝不发半份）
            existing = await _fetch_header(conn, body.baseline_no)
            await conn.commit()
            if existing is None:  # pragma: no cover - 对方回滚的极端竞态
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    detail="concurrent freeze rolled back; please retry",
                )
            log.info("baseline %s: lost concurrent freeze race, replaying committed one",
                     body.baseline_no)
            return _reuse_or_conflict(existing, body, response)

        row = await _fetch_header(conn, body.baseline_no)
        await conn.commit()
    log.info(
        "baseline %s frozen: station=%s range=[%s, %s] items=%s",
        body.baseline_no, body.station_id, body.start, body.end, row["item_count"],
    )
    return _header_to_out(row, reused=False)


# --------------------------------------------------------------------------- #
# 查询
# --------------------------------------------------------------------------- #
@router.get(
    "",
    response_model=list[BaselineOut],
    summary="基线列表（最新冻结在前）",
)
async def list_baselines(
    limit: int = Query(50, ge=1, le=200),
    p: AsyncConnectionPool = Depends(get_pool),
) -> list[BaselineOut]:
    async with p.connection() as conn:
        cur = await conn.execute(SQL_LIST_HEADERS, {"limit": limit})
        rows = await cur.fetchall()
        await conn.commit()
    return [_header_to_out(r, reused=False) for r in rows]


@router.get(
    "/{baseline_no}",
    response_model=BaselineOut,
    summary="查询基线头（参数/条数/冻结时刻）",
)
async def get_baseline(
    baseline_no: str, p: AsyncConnectionPool = Depends(get_pool)
) -> BaselineOut:
    async with p.connection() as conn:
        row = await _fetch_header(conn, baseline_no)
        await conn.commit()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "baseline not found")
    return _header_to_out(row, reused=False)


@router.get(
    "/{baseline_no}/items",
    response_model=list[BaselineItemOut],
    summary="查看基线明细（冻结时保存的记录快照）",
)
async def list_baseline_items(
    baseline_no: str,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    p: AsyncConnectionPool = Depends(get_pool),
) -> list[BaselineItemOut]:
    async with p.connection() as conn:
        header = await _fetch_header(conn, baseline_no)
        if header is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "baseline not found")
        cur = await conn.execute(
            SQL_LIST_ITEMS, {"no": baseline_no, "limit": limit, "offset": offset}
        )
        rows = await cur.fetchall()
        await conn.commit()
    return [BaselineItemOut(**r) for r in rows]


# --------------------------------------------------------------------------- #
# 核验
# --------------------------------------------------------------------------- #
@router.get(
    "/{baseline_no}/verify",
    response_model=BaselineVerifyOut,
    summary="按基线核验：同一已提交快照内列出被改写/消失/后来入库的记录",
    description=(
        "在**同一个已提交快照**内（单条 SQL）比对基线与当前 readings：\n\n"
        "- `rewritten`：基线记录仍在但被改动（含被改到时间段外；附基线值与当前值）；\n"
        "- `disappeared_ids`：基线记录已消失（被删除）；\n"
        "- `new_ids`：冻结后新入库且落在基线时间段内的记录（迟到记录只算新增）。"
    ),
)
async def verify_baseline(
    baseline_no: str, p: AsyncConnectionPool = Depends(get_pool)
) -> BaselineVerifyOut:
    async with p.connection() as conn:
        cur = await conn.execute(SQL_VERIFY, {"no": baseline_no})
        row = await cur.fetchone()
        await conn.commit()
    if row is None or row["header"] is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "baseline not found")

    header = row["header"]
    rewritten = [
        RewrittenItem(
            reading_id=item["reading_id"],
            baseline=ReadingValues(**item["baseline"]),
            current=ReadingValues(**item["current"]),
        )
        for item in row["rewritten"]
    ]
    disappeared = [int(x) for x in row["disappeared_ids"]]
    new_ids = [int(x) for x in row["new_ids"]]
    return BaselineVerifyOut(
        baseline_no=header["baseline_no"],
        station_id=header["station_id"],
        start=header["start"],
        end=header["end"],
        item_count=header["item_count"],
        verified_at=datetime.now(timezone.utc),
        rewritten=rewritten,
        disappeared_ids=disappeared,
        new_ids=new_ids,
        counts=VerifyCounts(
            rewritten=len(rewritten), disappeared=len(disappeared), new=len(new_ids)
        ),
    )
