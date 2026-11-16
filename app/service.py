"""业务规则：读数版本裁决、逐小时结算生成与查询。"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel

from .database import read_tx, write_tx
from .models import (
    MAX_HOURLY_USAGE,
    ROLLOVER,
    ConflictItem,
    DifferenceHour,
    DifferenceReportCreate,
    DifferenceReportDetail,
    DifferenceReportSummary,
    IngestItemResult,
    IngestResponse,
    MeterView,
    PublishProblem,
    ReadingInput,
    ReadingSnapshot,
    ReadingView,
    ReportMeterRef,
    ReportProblem,
    SettlementDetail,
    SettlementInterval,
    SettlementSummary,
    StaleBoundary,
    normalize_utc_hour,
)


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str, **extra: object):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra

    def detail(self) -> dict:
        return {"code": self.code, "message": self.message, **self._jsonable(self.extra)}

    @staticmethod
    def _jsonable(value: object) -> object:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json", exclude_none=True)
        if isinstance(value, dict):
            return {k: ApiError._jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [ApiError._jsonable(v) for v in value]
        return value


def utc_hour_text(value: datetime) -> str:
    return normalize_utc_hour(value).strftime("%Y-%m-%dT%H:00:00Z")


def parse_hour_text(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc)


def utc_now_text() -> str:
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _meter_not_found(meter_id: str) -> ApiError:
    return ApiError(404, "meter_not_found", f"电表 {meter_id!r} 不存在", meter_id=meter_id)


def _meter_view(row: sqlite3.Row) -> MeterView:
    return MeterView(
        meter_id=row["meter_id"],
        name=row["name"],
        created_at=row["created_at"],
        hour_count=row["hour_count"],
        reading_versions=row["reading_versions"],
        latest_hour=row["latest_hour"],
    )


_METER_VIEW_SQL = """
SELECT m.meter_id, m.name, m.created_at,
       COUNT(DISTINCT r.hour) AS hour_count,
       COUNT(r.hour)         AS reading_versions,
       MAX(r.hour)           AS latest_hour
FROM meters m
LEFT JOIN readings r ON r.meter_id = m.meter_id
"""


def _fetch_meter_view(conn: sqlite3.Connection, meter_id: str) -> MeterView | None:
    row = conn.execute(
        _METER_VIEW_SQL + " WHERE m.meter_id = ? GROUP BY m.meter_id",
        (meter_id,),
    ).fetchone()
    return _meter_view(row) if row else None


def create_meter(
    conn: sqlite3.Connection, meter_id: str, name: str | None
) -> tuple[bool, MeterView]:
    now = utc_now_text()
    with write_tx(conn):
        cursor = conn.execute(
            "INSERT INTO meters (meter_id, name, created_at) VALUES (?, ?, ?) "
            "ON CONFLICT(meter_id) DO NOTHING",
            (meter_id, name, now),
        )
        created = cursor.rowcount == 1
        view = _fetch_meter_view(conn, meter_id)
    assert view is not None
    return created, view


def get_meter(conn: sqlite3.Connection, meter_id: str) -> MeterView:
    with read_tx(conn):
        view = _fetch_meter_view(conn, meter_id)
    if view is None:
        raise _meter_not_found(meter_id)
    return view


def list_meters(conn: sqlite3.Connection) -> list[MeterView]:
    with read_tx(conn):
        rows = conn.execute(
            _METER_VIEW_SQL + " GROUP BY m.meter_id ORDER BY m.meter_id"
        ).fetchall()
    return [_meter_view(row) for row in rows]


def ingest_readings(
    conn: sqlite3.Connection, meter_id: str, readings: list[ReadingInput]
) -> IngestResponse:
    """整批原子写入：任一旧版/同版异值/批内同时刻异内容都会使整批回滚。

    批内完全相同 (hour, revision, value) 的重复条目按一条幂等条目处理。
    """
    grouped: dict[str, list[ReadingInput]] = {}
    for item in readings:
        grouped.setdefault(utc_hour_text(item.hour), []).append(item)

    conflicting_hours = {
        hour_text
        for hour_text, items in grouped.items()
        if len({(i.revision, i.value) for i in items}) > 1
    }

    with write_tx(conn):
        if conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)

        conflicts: list[ConflictItem] = []
        current_by_hour: dict[str, sqlite3.Row] = {}
        for hour_text, items in grouped.items():
            if hour_text in conflicting_hours:
                continue
            item = items[0]
            current = conn.execute(
                "SELECT revision, value FROM readings "
                "WHERE meter_id = ? AND hour = ? "
                "ORDER BY revision DESC LIMIT 1",
                (meter_id, hour_text),
            ).fetchone()
            if current is None:
                continue
            current_by_hour[hour_text] = current
            if item.revision < current["revision"]:
                conflicts.append(
                    ConflictItem(
                        hour=hour_text,
                        reason="stale_revision",
                        provided_revision=item.revision,
                        provided_value=item.value,
                        current_revision=current["revision"],
                        current_value=current["value"],
                    )
                )
            elif item.revision == current["revision"] and item.value != current["value"]:
                conflicts.append(
                    ConflictItem(
                        hour=hour_text,
                        reason="same_revision_different_value",
                        provided_revision=item.revision,
                        provided_value=item.value,
                        current_revision=current["revision"],
                        current_value=current["value"],
                    )
                )

        for hour_text in conflicting_hours:
            first = grouped[hour_text][0]
            conflicts.append(
                ConflictItem(
                    hour=hour_text,
                    reason="duplicate_hour_in_batch",
                    provided_revision=first.revision,
                    provided_value=first.value,
                )
            )

        if conflicts:
            conflicts.sort(key=lambda c: (c.hour, c.reason))
            raise ApiError(
                409,
                "reading_conflict",
                "读数批次与已提交版本冲突，整批未写入",
                atomic=True,
                conflicts=conflicts,
            )

        received_at = utc_now_text()
        results: list[IngestItemResult] = []
        accepted = 0
        unchanged = 0
        for hour_text, items in grouped.items():
            item = items[0]
            current = current_by_hour.get(hour_text)
            if current is None:
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (meter_id, hour_text, item.revision, item.value, received_at),
                )
                status = "accepted"
                accepted += 1
            elif item.revision == current["revision"]:
                # 同版同值重传：幂等，不新增版本。
                status = "unchanged"
                unchanged += 1
            else:
                # 更高修订：追加新版本，旧版本保留但不再是当前值。
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (meter_id, hour_text, item.revision, item.value, received_at),
                )
                status = "accepted"
                accepted += 1
            results.append(
                IngestItemResult(
                    hour=hour_text,
                    value=item.value,
                    revision=item.revision,
                    status=status,
                )
            )

    results.sort(key=lambda r: r.hour)
    return IngestResponse(
        meter_id=meter_id,
        accepted=accepted,
        unchanged=unchanged,
        readings=results,
    )


def _current_readings(
    conn: sqlite3.Connection,
    meter_id: str,
    start_hour: str,
    end_hour: str,
) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT r.hour, r.value, r.revision
        FROM readings r
        JOIN (
            SELECT hour, MAX(revision) AS revision
            FROM readings
            WHERE meter_id = ? AND hour BETWEEN ? AND ?
            GROUP BY hour
        ) latest ON latest.hour = r.hour AND latest.revision = r.revision
        WHERE r.meter_id = ?
        """,
        (meter_id, start_hour, end_hour, meter_id),
    ).fetchall()
    return {row["hour"]: row for row in rows}


def _missing_problems(
    required: list[datetime], present: dict[str, sqlite3.Row]
) -> list[PublishProblem]:
    """把连续缺失的读数时刻合并成半开区间。"""
    problems: list[PublishProblem] = []
    run: list[datetime] = []
    for hour in required + [None]:  # 哨兵，冲刷最后一段
        if hour is not None and utc_hour_text(hour) not in present:
            run.append(hour)
            continue
        if run:
            first, last = run[0], run[-1] + timedelta(hours=1)
            problems.append(
                PublishProblem(
                    code="missing_reading",
                    start_hour=utc_hour_text(first),
                    end_hour=utc_hour_text(last),
                    message=(
                        f"缺少 {utc_hour_text(first)} 至 {utc_hour_text(last)} "
                        "区间内的小时读数"
                    ),
                    missing_readings=[utc_hour_text(h) for h in run],
                )
            )
            run = []
    return problems


def publish_settlement(
    conn: sqlite3.Connection,
    meter_id: str,
    start: datetime,
    hours: int,
) -> SettlementDetail:
    start_dt = normalize_utc_hour(start)
    end_dt = start_dt + timedelta(hours=hours)
    start_text = utc_hour_text(start_dt)
    end_text = utc_hour_text(end_dt)
    required = [start_dt + timedelta(hours=i) for i in range(hours + 1)]

    with write_tx(conn):
        if conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)

        # 发布与录入共用同一个写锁；这里看到的是某一个完整提交状态。
        present = _current_readings(conn, meter_id, start_text, end_text)
        problems = _missing_problems(required, present)

        intervals: list[SettlementInterval] = []
        total = 0
        for seq in range(1, hours + 1):
            hour_start = start_dt + timedelta(hours=seq - 1)
            hour_end = hour_start + timedelta(hours=1)
            start_row = present.get(utc_hour_text(hour_start))
            end_row = present.get(utc_hour_text(hour_end))
            if start_row is None or end_row is None:
                continue

            start_value = start_row["value"]
            end_value = end_row["value"]
            usage = (
                end_value - start_value
                if end_value >= start_value
                else end_value + ROLLOVER - start_value
            )
            total += usage
            if usage > MAX_HOURLY_USAGE:
                problems.append(
                    PublishProblem(
                        code="hourly_usage_exceeds_limit",
                        start_hour=utc_hour_text(hour_start),
                        end_hour=utc_hour_text(hour_end),
                        message=(
                            f"{utc_hour_text(hour_start)} 至 {utc_hour_text(hour_end)} "
                            f"用量 {usage} 超过单小时上限 {MAX_HOURLY_USAGE}"
                        ),
                        usage=usage,
                        limit=MAX_HOURLY_USAGE,
                    )
                )

            intervals.append(
                SettlementInterval(
                    seq=seq,
                    hour_start=utc_hour_text(hour_start),
                    hour_end=utc_hour_text(hour_end),
                    usage=usage,
                    start_reading=ReadingSnapshot(
                        value=start_value, revision=start_row["revision"]
                    ),
                    end_reading=ReadingSnapshot(
                        value=end_value, revision=end_row["revision"]
                    ),
                )
            )

        if problems:
            problems.sort(key=lambda p: (p.start_hour, p.code))
            raise ApiError(
                409,
                "publish_rejected",
                "结算未发布：存在缺失读数或超限用量",
                atomic=True,
                meter_id=meter_id,
                start_hour=start_text,
                end_hour=end_text,
                hours=hours,
                problems=problems,
            )

        published_at = utc_now_text()
        settlement_no = (
            "STL-" + start_dt.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex
        )
        conn.execute(
            "INSERT INTO settlements "
            "(settlement_no, meter_id, start_hour, end_hour, hours, total_usage, published_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                settlement_no,
                meter_id,
                start_text,
                end_text,
                hours,
                total,
                published_at,
            ),
        )
        conn.executemany(
            "INSERT INTO settlement_intervals "
            "(settlement_no, seq, hour_start, hour_end, usage, "
            " start_value, start_revision, end_value, end_revision) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    settlement_no,
                    item.seq,
                    item.hour_start,
                    item.hour_end,
                    item.usage,
                    item.start_reading.value,
                    item.start_reading.revision,
                    item.end_reading.value,
                    item.end_reading.revision,
                )
                for item in intervals
            ],
        )

    return SettlementDetail(
        settlement_no=settlement_no,
        meter_id=meter_id,
        start_hour=start_text,
        end_hour=end_text,
        hours=hours,
        total_usage=total,
        published_at=published_at,
        intervals=intervals,
    )


def list_readings(
    conn: sqlite3.Connection,
    meter_id: str,
    start: datetime | None,
    end: datetime | None,
    include_history: bool,
) -> list[ReadingView]:
    if start is not None and end is not None and start > end:
        raise ApiError(422, "invalid_hour_range", "start_hour 不能晚于 end_hour")

    clauses = ["meter_id = ?"]
    params: list[object] = [meter_id]
    if start is not None:
        clauses.append("hour >= ?")
        params.append(utc_hour_text(start))
    if end is not None:
        clauses.append("hour <= ?")
        params.append(utc_hour_text(end))
    where = " AND ".join(clauses)

    with read_tx(conn):
        if conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)

        if include_history:
            rows = conn.execute(
                f"SELECT hour, value, revision, received_at FROM readings "
                f"WHERE {where} ORDER BY hour, revision",
                params,
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT r.hour, r.value, r.revision, r.received_at
                FROM readings r
                JOIN (
                    SELECT hour, MAX(revision) AS revision
                    FROM readings WHERE {where} GROUP BY hour
                ) latest ON latest.hour = r.hour AND latest.revision = r.revision
                WHERE r.meter_id = ?
                ORDER BY r.hour
                """,
                (*params, meter_id),
            ).fetchall()

    return [
        ReadingView(
            hour=row["hour"],
            value=row["value"],
            revision=row["revision"],
            received_at=row["received_at"],
        )
        for row in rows
    ]


def get_settlement(conn: sqlite3.Connection, settlement_no: str) -> SettlementDetail:
    with read_tx(conn):
        row = conn.execute(
            "SELECT * FROM settlements WHERE settlement_no = ?", (settlement_no,)
        ).fetchone()
        if row is None:
            raise ApiError(
                404,
                "settlement_not_found",
                f"结算号 {settlement_no!r} 不存在",
                settlement_no=settlement_no,
            )
        interval_rows = conn.execute(
            "SELECT * FROM settlement_intervals "
            "WHERE settlement_no = ? ORDER BY seq",
            (settlement_no,),
        ).fetchall()

    return SettlementDetail(
        settlement_no=row["settlement_no"],
        meter_id=row["meter_id"],
        start_hour=row["start_hour"],
        end_hour=row["end_hour"],
        hours=row["hours"],
        total_usage=row["total_usage"],
        published_at=row["published_at"],
        intervals=[
            SettlementInterval(
                seq=item["seq"],
                hour_start=item["hour_start"],
                hour_end=item["hour_end"],
                usage=item["usage"],
                start_reading=ReadingSnapshot(
                    value=item["start_value"], revision=item["start_revision"]
                ),
                end_reading=ReadingSnapshot(
                    value=item["end_value"], revision=item["end_revision"]
                ),
            )
            for item in interval_rows
        ],
    )


def list_settlements(
    conn: sqlite3.Connection, meter_id: str | None, limit: int
) -> list[SettlementSummary]:
    with read_tx(conn):
        if meter_id is not None and conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)
        if meter_id is None:
            rows = conn.execute(
                "SELECT * FROM settlements "
                "ORDER BY published_at DESC, settlement_no DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM settlements WHERE meter_id = ? "
                "ORDER BY published_at DESC, settlement_no DESC LIMIT ?",
                (meter_id, limit),
            ).fetchall()
    return [
        SettlementSummary(
            settlement_no=row["settlement_no"],
            meter_id=row["meter_id"],
            start_hour=row["start_hour"],
            end_hour=row["end_hour"],
            hours=row["hours"],
            total_usage=row["total_usage"],
            published_at=row["published_at"],
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# 差额报告
# ---------------------------------------------------------------------------


def _load_settlement_refs(
    conn: sqlite3.Connection, refs: list[str]
) -> dict[str, dict]:
    """按结算号读取结算头与全部区间（调用方已持有事务）。"""
    result: dict[str, dict] = {}
    for ref in refs:
        header = conn.execute(
            "SELECT * FROM settlements WHERE settlement_no = ?", (ref,)
        ).fetchone()
        if header is None:
            continue
        intervals = conn.execute(
            "SELECT * FROM settlement_intervals WHERE settlement_no = ? ORDER BY seq",
            (ref,),
        ).fetchall()
        result[ref] = {"header": header, "intervals": intervals}
    return result


def _current_revisions_for_pairs(
    conn: sqlite3.Connection, pairs: list[tuple[str, str]]
) -> dict[tuple[str, str], int]:
    """批量返回每个 (meter_id, hour) 的当前修订号；该时刻已无读数时键缺失。"""
    hours_by_meter: dict[str, set[str]] = {}
    for meter_id, hour in pairs:
        hours_by_meter.setdefault(meter_id, set()).add(hour)

    current: dict[tuple[str, str], int] = {}
    for meter_id, hours in hours_by_meter.items():
        ordered = sorted(hours)
        placeholders = ",".join("?" for _ in ordered)
        rows = conn.execute(
            f"SELECT hour, MAX(revision) AS revision FROM readings "
            f"WHERE meter_id = ? AND hour IN ({placeholders}) GROUP BY hour",
            (meter_id, *ordered),
        ).fetchall()
        for row in rows:
            current[(meter_id, row["hour"])] = row["revision"]
    return current


def _stale_from_map(
    boundaries: list[tuple[str, str, int]],
    current_revisions: dict[tuple[str, str], int],
) -> list[StaleBoundary]:
    """边界读数当前修订与报告所用修订不一致（含读数缺失）即判过期。"""
    stale: list[StaleBoundary] = []
    for meter_id, hour, used_revision in boundaries:
        current_revision = current_revisions.get((meter_id, hour))
        if current_revision != used_revision:
            stale.append(
                StaleBoundary(
                    meter_id=meter_id,
                    hour=hour,
                    used_revision=used_revision,
                    current_revision=current_revision,
                )
            )
    stale.sort(key=lambda b: (b.meter_id, b.hour))
    return stale


def _collect_stale_boundaries(
    conn: sqlite3.Connection, boundaries: list[tuple[str, str, int]]
) -> list[StaleBoundary]:
    """边界读数当前修订与报告所用修订不一致（含读数缺失）即判过期。"""
    current_revisions = _current_revisions_for_pairs(
        conn, [(meter_id, hour) for meter_id, hour, _ in boundaries]
    )
    return _stale_from_map(boundaries, current_revisions)


def create_difference_report(
    conn: sqlite3.Connection, payload: DifferenceReportCreate
) -> DifferenceReportDetail:
    refs = [payload.master_settlement_no, *payload.sub_settlement_nos]
    problems: list[ReportProblem] = []

    with write_tx(conn):
        # 与读数更正共用同一写锁：提交顺序即裁决顺序。
        loaded = _load_settlement_refs(conn, refs)
        missing = [ref for ref in refs if ref not in loaded]
        for ref in missing:
            problems.append(
                ReportProblem(
                    code="settlement_not_found",
                    message=f"结算号 {ref!r} 不存在",
                    settlement_no=ref,
                )
            )

        present_refs = [ref for ref in refs if ref in loaded]

        # 电表编号互异（总表与各分表不得是同一块表）。
        meter_of = {
            ref: loaded[ref]["header"]["meter_id"] for ref in present_refs
        }
        seen_meters: dict[str, str] = {}
        duplicated_meters: set[str] = set()
        for ref in present_refs:
            meter_id = meter_of[ref]
            if meter_id in seen_meters:
                duplicated_meters.add(meter_id)
            else:
                seen_meters[meter_id] = ref
        for meter_id in sorted(duplicated_meters):
            dup_refs = [ref for ref in present_refs if meter_of[ref] == meter_id]
            problems.append(
                ReportProblem(
                    code="duplicate_meter",
                    message=(
                        f"电表 {meter_id!r} 在总表与分表中重复出现，"
                        "各结算必须来自互异电表"
                    ),
                    meter_id=meter_id,
                    settlement_no=dup_refs[0],
                )
            )

        # 所有结算必须覆盖同一段连续小时。
        master_ref = payload.master_settlement_no
        if master_ref in loaded:
            master_header = loaded[master_ref]["header"]
            master_start = master_header["start_hour"]
            master_end = master_header["end_hour"]
            master_hours = master_header["hours"]
            for ref in present_refs:
                header = loaded[ref]["header"]
                if (
                    header["start_hour"] != master_start
                    or header["end_hour"] != master_end
                    or header["hours"] != master_hours
                ):
                    problems.append(
                        ReportProblem(
                            code="interval_mismatch",
                            message=(
                                f"结算 {ref!r} 覆盖 {header['start_hour']} 至 "
                                f"{header['end_hour']}（{header['hours']} 小时），"
                                f"与总表结算 {master_start} 至 {master_end}"
                                f"（{master_hours} 小时）不一致"
                            ),
                            settlement_no=ref,
                            meter_id=header["meter_id"],
                            start_hour=header["start_hour"],
                            end_hour=header["end_hour"],
                            hours=header["hours"],
                            expected_start_hour=master_start,
                            expected_end_hour=master_end,
                        )
                    )
        else:
            # 总表缺失时无法锚定公共区间，后续校验跳过。
            master_start = master_end = ""
            master_hours = 0

        interval_aligned = set(present_refs)
        if master_ref in loaded:
            for ref in present_refs:
                header = loaded[ref]["header"]
                if (
                    header["start_hour"] != master_start
                    or header["end_hour"] != master_end
                    or header["hours"] != master_hours
                ):
                    interval_aligned.discard(ref)

        # 所用边界读数必须仍是各表当前修订。
        boundary_pairs: list[tuple[str, str]] = []
        used_revision: dict[tuple[str, str], int] = {}
        for ref in interval_aligned:
            meter_id = loaded[ref]["header"]["meter_id"]
            for interval in loaded[ref]["intervals"]:
                for boundary, revision in (
                    (interval["hour_start"], interval["start_revision"]),
                    (interval["hour_end"], interval["end_revision"]),
                ):
                    key = (meter_id, boundary)
                    # 同一 (meter, hour) 在相邻区间重复出现，且快照内修订必然一致。
                    used_revision.setdefault(key, revision)
                    if key not in boundary_pairs:
                        boundary_pairs.append(key)

        current_revisions = _current_revisions_for_pairs(conn, boundary_pairs)
        for meter_id, hour in boundary_pairs:
            used = used_revision[(meter_id, hour)]
            current = current_revisions.get((meter_id, hour))
            if current != used:
                problems.append(
                    ReportProblem(
                        code="boundary_revision_superseded",
                        message=(
                            f"电表 {meter_id!r} 在 {hour} 的边界读数修订 {used} "
                            f"已不是当前修订（当前：{current}），请基于最新结算重试"
                        ),
                        meter_id=meter_id,
                        hour=hour,
                        used_revision=used,
                        current_revision=current,
                    )
                )

        if problems:
            problems.sort(key=lambda p: (p.code, p.settlement_no or "", p.hour or ""))
            raise ApiError(
                409,
                "difference_report_rejected",
                "差额报告未生成：结算引用或边界读数校验未通过",
                atomic=True,
                problems=problems,
            )

        # 校验全部通过：逐小时计算总表用量减去分表用量之和。
        master_intervals = loaded[master_ref]["intervals"]
        sub_usage_by_seq: dict[int, int] = {
            seq: 0 for seq in range(1, master_hours + 1)
        }
        for ref in payload.sub_settlement_nos:
            for interval in loaded[ref]["intervals"]:
                sub_usage_by_seq[interval["seq"]] += interval["usage"]

        hours_detail: list[DifferenceHour] = []
        anomaly_count = 0
        for interval in master_intervals:
            seq = interval["seq"]
            master_usage = interval["usage"]
            sub_total = sub_usage_by_seq[seq]
            difference = master_usage - sub_total
            if difference < 0:
                is_anomaly, reason = True, "negative"
            elif difference > payload.tolerance:
                is_anomaly, reason = True, "exceeds_tolerance"
            else:
                is_anomaly, reason = False, "ok"
            anomaly_count += int(is_anomaly)
            hours_detail.append(
                DifferenceHour(
                    seq=seq,
                    hour_start=interval["hour_start"],
                    hour_end=interval["hour_end"],
                    master_usage=master_usage,
                    sub_usage_total=sub_total,
                    difference=difference,
                    tolerance=payload.tolerance,
                    is_anomaly=is_anomaly,
                    reason=reason,
                )
            )

        created_at = utc_now_text()
        report_no = (
            "RPT-"
            + parse_hour_text(master_start).strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + uuid.uuid4().hex
        )
        conn.execute(
            "INSERT INTO difference_reports "
            "(report_no, master_settlement_no, start_hour, end_hour, hours, "
            " tolerance, anomaly_count, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                report_no,
                master_ref,
                master_start,
                master_end,
                master_hours,
                payload.tolerance,
                anomaly_count,
                created_at,
            ),
        )

        meter_rows: list[tuple[str, str, int, str, str]] = [
            (
                report_no,
                "master",
                0,
                loaded[master_ref]["header"]["meter_id"],
                master_ref,
            )
        ]
        for index, ref in enumerate(payload.sub_settlement_nos, start=1):
            meter_rows.append(
                (
                    report_no,
                    "sub",
                    index,
                    loaded[ref]["header"]["meter_id"],
                    ref,
                )
            )
        conn.executemany(
            "INSERT INTO difference_report_meters "
            "(report_no, role, seq, meter_id, settlement_no) "
            "VALUES (?, ?, ?, ?, ?)",
            meter_rows,
        )

        conn.executemany(
            "INSERT INTO difference_report_boundaries "
            "(report_no, meter_id, hour, used_revision) VALUES (?, ?, ?, ?)",
            [
                (report_no, meter_id, hour, used_revision[(meter_id, hour)])
                for meter_id, hour in boundary_pairs
            ],
        )
        conn.executemany(
            "INSERT INTO difference_report_hours "
            "(report_no, seq, hour_start, hour_end, master_usage, "
            " sub_usage_total, difference, is_anomaly, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    report_no,
                    item.seq,
                    item.hour_start,
                    item.hour_end,
                    item.master_usage,
                    item.sub_usage_total,
                    item.difference,
                    int(item.is_anomaly),
                    item.reason,
                )
                for item in hours_detail
            ],
        )

        meter_refs: list[ReportMeterRef] = [
            ReportMeterRef(
                role="master",
                meter_id=loaded[master_ref]["header"]["meter_id"],
                settlement_no=master_ref,
                seq=0,
            )
        ]
        for index, ref in enumerate(payload.sub_settlement_nos, start=1):
            meter_refs.append(
                ReportMeterRef(
                    role="sub",
                    meter_id=loaded[ref]["header"]["meter_id"],
                    settlement_no=ref,
                    seq=index,
                )
            )

        return DifferenceReportDetail(
            report_no=report_no,
            master_settlement_no=master_ref,
            sub_settlement_nos=list(payload.sub_settlement_nos),
            meter_ids=[row[3] for row in meter_rows],
            start_hour=master_start,
            end_hour=master_end,
            hours=master_hours,
            tolerance=payload.tolerance,
            anomaly_count=anomaly_count,
            created_at=created_at,
            meters=meter_refs,
            hours_detail=hours_detail,
            stale_boundaries=[],
            expired=False,
        )


def _report_not_found(report_no: str) -> ApiError:
    return ApiError(
        404,
        "difference_report_not_found",
        f"差额报告 {report_no!r} 不存在",
        report_no=report_no,
    )


def get_difference_report(
    conn: sqlite3.Connection, report_no: str
) -> DifferenceReportDetail:
    with read_tx(conn):
        header = conn.execute(
            "SELECT * FROM difference_reports WHERE report_no = ?", (report_no,)
        ).fetchone()
        if header is None:
            raise _report_not_found(report_no)
        meter_rows = conn.execute(
            "SELECT * FROM difference_report_meters WHERE report_no = ? "
            "ORDER BY seq",
            (report_no,),
        ).fetchall()
        hour_rows = conn.execute(
            "SELECT * FROM difference_report_hours WHERE report_no = ? ORDER BY seq",
            (report_no,),
        ).fetchall()
        boundary_rows = conn.execute(
            "SELECT * FROM difference_report_boundaries WHERE report_no = ?",
            (report_no,),
        ).fetchall()
        stale = _collect_stale_boundaries(
            conn,
            [
                (row["meter_id"], row["hour"], row["used_revision"])
                for row in boundary_rows
            ],
        )

    sub_refs = [row["settlement_no"] for row in meter_rows if row["role"] == "sub"]
    meter_ids = [row["meter_id"] for row in meter_rows]
    return DifferenceReportDetail(
        report_no=header["report_no"],
        master_settlement_no=header["master_settlement_no"],
        sub_settlement_nos=sub_refs,
        meter_ids=meter_ids,
        start_hour=header["start_hour"],
        end_hour=header["end_hour"],
        hours=header["hours"],
        tolerance=header["tolerance"],
        anomaly_count=header["anomaly_count"],
        created_at=header["created_at"],
        meters=[
            ReportMeterRef(
                role=row["role"],
                meter_id=row["meter_id"],
                settlement_no=row["settlement_no"],
                seq=row["seq"],
            )
            for row in meter_rows
        ],
        hours_detail=[
            DifferenceHour(
                seq=row["seq"],
                hour_start=row["hour_start"],
                hour_end=row["hour_end"],
                master_usage=row["master_usage"],
                sub_usage_total=row["sub_usage_total"],
                difference=row["difference"],
                tolerance=header["tolerance"],
                is_anomaly=bool(row["is_anomaly"]),
                reason=row["reason"],
            )
            for row in hour_rows
        ],
        stale_boundaries=stale,
        expired=bool(stale),
    )


def list_difference_reports(
    conn: sqlite3.Connection, limit: int
) -> list[DifferenceReportSummary]:
    with read_tx(conn):
        headers = conn.execute(
            "SELECT * FROM difference_reports "
            "ORDER BY created_at DESC, report_no DESC LIMIT ?",
            (limit,),
        ).fetchall()
        if not headers:
            return []
        report_nos = [row["report_no"] for row in headers]
        placeholders = ",".join("?" for _ in report_nos)
        meter_rows = conn.execute(
            f"SELECT * FROM difference_report_meters "
            f"WHERE report_no IN ({placeholders})",
            report_nos,
        ).fetchall()
        boundary_rows = conn.execute(
            f"SELECT * FROM difference_report_boundaries "
            f"WHERE report_no IN ({placeholders})",
            report_nos,
        ).fetchall()

        meters_by_report: dict[str, list[sqlite3.Row]] = {}
        for row in meter_rows:
            meters_by_report.setdefault(row["report_no"], []).append(row)
        for rows in meters_by_report.values():
            rows.sort(key=lambda r: r["seq"])

        boundaries_by_report: dict[str, list[tuple[str, str, int]]] = {}
        all_pairs: list[tuple[str, str]] = []
        for row in boundary_rows:
            triple = (row["meter_id"], row["hour"], row["used_revision"])
            boundaries_by_report.setdefault(row["report_no"], []).append(triple)
            all_pairs.append((row["meter_id"], row["hour"]))

        # 列表内所有报告涉及的边界一次性查当前修订，避免按报告重复查询。
        current_revisions = _current_revisions_for_pairs(conn, all_pairs)

        summaries: list[DifferenceReportSummary] = []
        for header in headers:
            report_no = header["report_no"]
            stale = _stale_from_map(
                boundaries_by_report.get(report_no, []), current_revisions
            )
            refs = meters_by_report[report_no]
            sub_refs = [
                row["settlement_no"] for row in refs if row["role"] == "sub"
            ]
            summaries.append(
                DifferenceReportSummary(
                    report_no=report_no,
                    master_settlement_no=header["master_settlement_no"],
                    sub_settlement_nos=sub_refs,
                    meter_ids=[row["meter_id"] for row in refs],
                    start_hour=header["start_hour"],
                    end_hour=header["end_hour"],
                    hours=header["hours"],
                    tolerance=header["tolerance"],
                    anomaly_count=header["anomaly_count"],
                    created_at=header["created_at"],
                    expired=bool(stale),
                )
            )
    return summaries
