"""请求/响应模型与 UTC 整点等基础校验。"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    field_validator,
)

ROLLOVER = 10_000
MAX_HOURLY_USAGE = 500
MAX_PUBLISH_HOURS = 48

HOUR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:00:00(?:Z|\+00:00)$")
METER_ID_PATTERN = re.compile(r"^[^\s/\\\x00-\x1f\x7f]{1,64}$")


def normalize_utc_hour(value: datetime) -> datetime:
    """只接受 UTC 时区、分秒为零的整点。"""
    if not isinstance(value, datetime):
        raise ValueError("必须是 ISO 8601 日期时间")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("必须使用 UTC 时区（Z 或 +00:00）")
    if value.minute or value.second or value.microsecond:
        raise ValueError("必须是 UTC 整点，分、秒、微秒均为 0")
    return value.astimezone(timezone.utc)


def parse_utc_hour(value: object) -> datetime:
    """严格解析：只接受 `YYYY-MM-DDTHH:00:00Z` 或同等的 `+00:00` 写法。"""
    if isinstance(value, datetime):
        return normalize_utc_hour(value)
    if not isinstance(value, str) or not HOUR_PATTERN.fullmatch(value):
        raise ValueError(
            "必须是 UTC 整点字符串，格式如 2026-11-16T00:00:00Z（分秒为 0、时区为 Z/+00:00）"
        )
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return normalize_utc_hour(parsed)


UtcHour = Annotated[datetime, BeforeValidator(parse_utc_hour)]


def validate_meter_id(value: str) -> str:
    if not isinstance(value, str) or not METER_ID_PATTERN.fullmatch(value):
        raise ValueError("电表编号须为 1～64 个字符，且不能包含空白、斜杠或控制字符")
    return value


class MeterCreate(BaseModel):
    meter_id: str
    name: str | None = Field(default=None, max_length=128)

    @field_validator("meter_id")
    @classmethod
    def _meter_id(cls, value: str) -> str:
        return validate_meter_id(value)


class ReadingInput(BaseModel):
    hour: UtcHour
    value: int = Field(strict=True, ge=0, le=9_999)
    revision: int = Field(strict=True, gt=0, le=9_223_372_036_854_775_807)


class ReadingBatch(BaseModel):
    readings: list[ReadingInput] = Field(min_length=1)


class SettlementCreate(BaseModel):
    start_hour: UtcHour
    hours: int = Field(strict=True, ge=1, le=MAX_PUBLISH_HOURS)


class MeterView(BaseModel):
    meter_id: str
    name: str | None
    created_at: str
    hour_count: int
    reading_versions: int
    latest_hour: str | None


class MeterList(BaseModel):
    meters: list[MeterView]
    count: int


class MeterCreateResult(BaseModel):
    created: bool
    meter: MeterView


class ReadingView(BaseModel):
    hour: str
    value: int
    revision: int
    received_at: str


class ReadingList(BaseModel):
    meter_id: str
    include_history: bool
    readings: list[ReadingView]
    count: int


class IngestItemResult(BaseModel):
    hour: str
    value: int
    revision: int
    status: Literal["accepted", "unchanged"]


class IngestResponse(BaseModel):
    meter_id: str
    accepted: int
    unchanged: int
    readings: list[IngestItemResult]


class ReadingSnapshot(BaseModel):
    value: int
    revision: int


class SettlementInterval(BaseModel):
    seq: int
    hour_start: str
    hour_end: str
    usage: int
    start_reading: ReadingSnapshot
    end_reading: ReadingSnapshot


class SettlementSummary(BaseModel):
    settlement_no: str
    meter_id: str
    start_hour: str
    end_hour: str
    hours: int
    total_usage: int
    published_at: str


class SettlementDetail(SettlementSummary):
    intervals: list[SettlementInterval]


class SettlementList(BaseModel):
    items: list[SettlementSummary]
    count: int


class PublishProblem(BaseModel):
    code: Literal["missing_reading", "hourly_usage_exceeds_limit"]
    start_hour: str
    end_hour: str
    message: str
    missing_readings: list[str] | None = None
    usage: int | None = None
    limit: int | None = None


class ConflictItem(BaseModel):
    hour: str
    reason: Literal[
        "stale_revision",
        "same_revision_different_value",
        "duplicate_hour_in_batch",
    ]
    provided_revision: int
    provided_value: int
    current_revision: int | None = None
    current_value: int | None = None
