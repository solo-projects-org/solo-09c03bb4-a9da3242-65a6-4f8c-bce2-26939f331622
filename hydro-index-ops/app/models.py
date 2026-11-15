"""Pydantic 出入参模型。"""
from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator, model_validator


class BatchCreate(BaseModel):
    note: str | None = Field(default=None, max_length=500, description="备注（可选）")


class BatchOut(BaseModel):
    id: int
    state: str
    cancel_requested: bool
    build_pid: int | None
    phase: str | None
    error: str | None
    cleanup_attempts: int
    created_at: datetime
    updated_at: datetime


class ReadingIn(BaseModel):
    station_id: int = Field(gt=0, description="站号")
    observed_at: datetime = Field(description="观测时间")
    water_level: float = Field(description="水位（米）")
    discharge: float | None = Field(default=None, description="流量（可选）")
    quality: str = Field(default="GOOD", description="GOOD/SUSPECT/BAD")


class ReadingOut(ReadingIn):
    id: int
    ingested_at: datetime


class LongTxOut(BaseModel):
    pid: int
    seconds: int
    state: str
    message: str


# --------------------------------------------------------------------------- #
# 质控基线：冻结与核验
# --------------------------------------------------------------------------- #

# 基线号：字母数字开头，可含 . _ -，最长 64
BASELINE_NO_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"


def _require_tz_utc(v: datetime) -> datetime:
    """起止时刻必须带时区偏移；统一归一到 UTC。"""
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise ValueError(
            "起止时刻必须携带时区偏移（UTC 时刻，如 2026-11-14T00:00:00Z）"
        )
    return v.astimezone(timezone.utc)


class BaselineFreezeIn(BaseModel):
    baseline_no: str = Field(
        pattern=BASELINE_NO_PATTERN,
        description="基线号（全局唯一；同号重试幂等，参数变化报 409）",
        examples=["BL-20261115-01"],
    )
    station_id: int = Field(gt=0, description="站号")
    start: datetime = Field(description="起始观测时刻（含，UTC）")
    end: datetime = Field(description="截止观测时刻（含，UTC）")

    @field_validator("start", "end")
    @classmethod
    def _tz(cls, v: datetime) -> datetime:
        return _require_tz_utc(v)

    @model_validator(mode="after")
    def _order(self) -> "BaselineFreezeIn":
        if self.start >= self.end:
            raise ValueError("start 必须严格早于 end")
        return self


class BaselineOut(BaseModel):
    baseline_no: str
    station_id: int
    start: datetime
    end: datetime
    item_count: int = Field(description="基线冻结的记录条数")
    frozen_at: datetime = Field(description="基线提交时刻")
    reused: bool = Field(
        description="true=本次为幂等重放（返回首次完整基线，未重新采样）；false=新建"
    )


class BaselineItemOut(BaseModel):
    reading_id: int
    station_id: int
    observed_at: datetime
    water_level: float
    discharge: float | None
    quality: str


class ReadingValues(BaseModel):
    """基线或当前库中一条记录的快照值。"""

    station_id: int
    observed_at: datetime
    water_level: float
    discharge: float | None
    quality: str


class RewrittenItem(BaseModel):
    reading_id: int
    baseline: ReadingValues = Field(description="冻结时的值")
    current: ReadingValues = Field(description="当前库中的值")


class VerifyCounts(BaseModel):
    rewritten: int
    disappeared: int
    new: int


class BaselineVerifyOut(BaseModel):
    baseline_no: str
    station_id: int
    start: datetime
    end: datetime
    item_count: int
    verified_at: datetime = Field(description="本次核验发起时刻")
    rewritten: list[RewrittenItem] = Field(
        description="被改写的记录（按记录 ID 比对；被改到时间段外仍算改写）"
    )
    disappeared_ids: list[int] = Field(description="已消失（被删除）的记录 ID")
    new_ids: list[int] = Field(
        description="冻结后新入库且落在基线时间段内的记录 ID（迟到记录只算新增）"
    )
    counts: VerifyCounts
