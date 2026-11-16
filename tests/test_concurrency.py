"""并发录入/发布裁决与事务原子性。"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone

import pytest
from conftest import register
from fastapi.testclient import TestClient

from app.database import connect, init_db
from app.main import create_app
from app.models import DifferenceReportCreate, ReadingInput
from app.service import (
    ApiError,
    create_difference_report,
    ingest_readings,
    publish_settlement,
)

HOUR = datetime(2026, 11, 16, tzinfo=timezone.utc)


def run_threads(targets):
    errors: list[BaseException] = []

    def wrapper(fn):
        try:
            fn()
        except BaseException as exc:  # noqa: BLE001 - 收集所有线程异常
            errors.append(exc)

    threads = [threading.Thread(target=wrapper, args=(fn,)) for fn in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert all(not t.is_alive() for t in threads)
    if errors:
        raise errors[0]


def test_concurrent_same_revision_ingest_has_single_winner(db_path):
    init_db(db_path)
    conn = connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-001', NULL, 'x')"
            )
    finally:
        conn.close()

    results: list[tuple[str, object]] = []
    barrier = threading.Barrier(6)

    def attempt(value: int):
        def run():
            own = connect(db_path)
            try:
                barrier.wait(timeout=10)
                item = ReadingInput(hour=HOUR, value=value, revision=1)
                try:
                    ingest_readings(own, "M-001", [item])
                    results.append(("ok", value))
                except ApiError as exc:
                    results.append(("conflict", exc.status_code))
            finally:
                own.close()

        return run

    run_threads([attempt(v) for v in range(100, 106)])

    assert [r[0] for r in results].count("ok") == 1
    assert [r[0] for r in results].count("conflict") == 5
    assert {r[1] for r in results if r[0] == "conflict"} == {409}

    check = connect(db_path)
    try:
        rows = check.execute(
            "SELECT value, revision FROM readings WHERE meter_id = 'M-001'"
        ).fetchall()
    finally:
        check.close()
    winner_value = next(v for status, v in results if status == "ok")
    assert [(row["value"], row["revision"]) for row in rows] == [(winner_value, 1)]


def test_concurrent_increasing_revisions_serialize(db_path):
    """不同修订并发提交：较小修订若后到会被拒绝，最终当前版本是成功提交中的最大者。"""
    init_db(db_path)
    conn = connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-001', NULL, 'x')"
            )
    finally:
        conn.close()

    outcomes: dict[int, str] = {}
    barrier = threading.Barrier(4)
    lock = threading.Lock()

    def attempt(rev: int):
        def run():
            own = connect(db_path)
            try:
                barrier.wait(timeout=10)
                try:
                    ingest_readings(
                        own, "M-001", [ReadingInput(hour=HOUR, value=rev * 10, revision=rev)]
                    )
                    outcome = "ok"
                except ApiError:
                    outcome = "conflict"
                with lock:
                    outcomes[rev] = outcome
            finally:
                own.close()

        return run

    run_threads([attempt(rev) for rev in (1, 2, 3, 4)])

    check = connect(db_path)
    try:
        current = check.execute(
            "SELECT revision, value FROM readings WHERE meter_id = 'M-001' "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        versions = check.execute(
            "SELECT COUNT(*) AS n FROM readings WHERE meter_id = 'M-001'"
        ).fetchone()["n"]
    finally:
        check.close()

    max_ok = max(rev for rev, result in outcomes.items() if result == "ok")
    assert current["revision"] == max_ok
    # 每个成功提交恰好留下一个版本，被拒绝的修订不留下任何数据
    assert versions == sum(1 for result in outcomes.values() if result == "ok")


def test_publish_waits_for_in_flight_ingest_and_sees_committed_state(db_path):
    """发布遇到未提交的录入时等待写锁，随后使用该完整提交状态的读数。"""
    init_db(db_path)
    conn = connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-001', NULL, 'x')"
            )
            for hour, value in ((0, 100), (1, 200)):
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES ('M-001', ?, 1, ?, 'x')",
                    (f"2026-11-16T{hour:02d}:00:00Z", value),
                )
    finally:
        conn.close()

    writer = connect(db_path)
    writer.execute("BEGIN IMMEDIATE")
    writer.execute(
        "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
        "VALUES ('M-001', '2026-11-16T01:00:00Z', 2, 240, 'x')"
    )
    lock_held = threading.Event()
    release = threading.Event()
    publish_result: list[object] = []

    def hold_then_commit():
        lock_held.set()
        assert release.wait(timeout=10)
        writer.commit()

    def publish():
        own = connect(db_path)
        try:
            lock_held.wait(timeout=10)
            publish_result.append(publish_settlement(own, "M-001", HOUR, 1))
        finally:
            own.close()

    holder = threading.Thread(target=hold_then_commit)
    holder.start()
    publisher = threading.Thread(target=publish)
    publisher.start()
    assert lock_held.wait(timeout=10)
    # 发布线程此刻在 BEGIN IMMEDIATE 上等待，未提交的修订 2 不可见。
    publisher.join(timeout=0.5)
    assert publisher.is_alive(), "发布不应绕过未提交的写事务"
    release.set()
    holder.join(timeout=10)
    publisher.join(timeout=10)
    assert not publisher.is_alive()

    settlement = publish_result[0]
    assert settlement.intervals[0].usage == 140
    assert settlement.intervals[0].start_reading.revision == 1
    assert settlement.intervals[0].end_reading.revision == 2
    writer.close()


def test_concurrent_publishes_are_distinct_and_complete(db_path):
    init_db(db_path)
    conn = connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-001', NULL, 'x')"
            )
            for hour, value in ((0, 0), (1, 10), (2, 30)):
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES ('M-001', ?, 1, ?, 'x')",
                    (f"2026-11-16T{hour:02d}:00:00Z", value),
                )
    finally:
        conn.close()

    results: list[object] = []
    lock = threading.Lock()
    barrier = threading.Barrier(4)

    def publish():
        def run():
            own = connect(db_path)
            try:
                barrier.wait(timeout=10)
                result = publish_settlement(own, "M-001", HOUR, 2)
                with lock:
                    results.append(result)
            finally:
                own.close()

        return run

    run_threads([publish() for _ in range(4)])

    assert len(results) == 4
    assert len({r.settlement_no for r in results}) == 4
    for result in results:
        assert [i.usage for i in result.intervals] == [10, 20]
        assert result.total_usage == 30

    check = connect(db_path)
    try:
        settlements = check.execute("SELECT COUNT(*) AS n FROM settlements").fetchone()["n"]
        intervals = check.execute(
            "SELECT COUNT(*) AS n FROM settlement_intervals"
        ).fetchone()["n"]
        assert settlements == 4
        assert intervals == 8
    finally:
        check.close()


def test_failed_settlement_insert_rolls_back_header(db_path):
    """明细写入失败时结算头也回滚，不留下可查询的半份结算。"""
    init_db(db_path)
    conn = connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-001', NULL, 'x')"
            )
            conn.execute("CREATE TRIGGER block_intervals BEFORE INSERT ON settlement_intervals "
                         "BEGIN SELECT RAISE(ABORT, 'simulated failure'); END;")
            conn.execute(
                "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                "VALUES ('M-001', '2026-11-16T00:00:00Z', 1, 0, 'x')"
            )
            conn.execute(
                "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                "VALUES ('M-001', '2026-11-16T01:00:00Z', 1, 10, 'x')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            publish_settlement(conn, "M-001", HOUR, 1)
        assert conn.execute("SELECT COUNT(*) FROM settlements").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM settlement_intervals").fetchone()[0] == 0
    finally:
        conn.close()


def test_concurrent_http_ingest_and_publish_never_half_visible(db_path, monkeypatch):
    """HTTP 层并发：录入与发布交错后，每份能查到的结算都完整且引用同一次提交状态的读数。"""
    monkeypatch.setenv("ENERGY_DB_PATH", db_path)
    app = create_app(db_path)
    with TestClient(app) as client:
        register(client)
        base = [{"hour": f"2026-11-16T{h:02d}:00:00Z", "value": h * 10, "revision": 1}
                for h in range(3)]
        assert client.post("/meters/M-001/readings", json={"readings": base}).status_code == 200

        errors: list[BaseException] = []

        def reader():
            try:
                for _ in range(10):
                    listing = client.get("/meters/M-001/settlements").json()
                    for summary in listing["items"]:
                        detail = client.get(
                            f"/settlements/{summary['settlement_no']}"
                        ).json()
                        assert len(detail["intervals"]) == detail["hours"]
                        assert detail["total_usage"] == sum(
                            i["usage"] for i in detail["intervals"]
                        )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def corrector():
            try:
                for rev in range(2, 7):
                    response = client.post(
                        "/meters/M-001/readings",
                        json={"readings": [
                            {"hour": "2026-11-16T01:00:00Z", "value": 10 + rev, "revision": rev}
                        ]},
                    )
                    assert response.status_code == 200
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def publisher():
            try:
                for _ in range(10):
                    response = client.post(
                        "/meters/M-001/settlements",
                        json={"start_hour": "2026-11-16T00:00:00Z", "hours": 2},
                    )
                    assert response.status_code == 201, response.text
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        run_threads([reader, corrector, publisher])
        if errors:
            raise errors[0]


def _seed_two_meters_with_settlements(db_path: str):
    """返回 (总表结算号, 分表结算号)。"""
    init_db(db_path)
    conn = connect(db_path)
    try:
        with conn:
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-MASTER', NULL, 'x')"
            )
            conn.execute(
                "INSERT INTO meters (meter_id, name, created_at) VALUES ('M-SUB1', NULL, 'x')"
            )
            for hour, value in ((0, 0), (1, 10), (2, 30)):
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES ('M-MASTER', ?, 1, ?, 'x')",
                    (f"2026-11-16T{hour:02d}:00:00Z", value),
                )
            for hour, value in ((0, 0), (1, 4), (2, 9)):
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES ('M-SUB1', ?, 1, ?, 'x')",
                    (f"2026-11-16T{hour:02d}:00:00Z", value),
                )
    finally:
        conn.close()

    service_conn = connect(db_path)
    try:
        master = publish_settlement(service_conn, "M-MASTER", HOUR, 2).settlement_no
        sub = publish_settlement(service_conn, "M-SUB1", HOUR, 2).settlement_no
    finally:
        service_conn.close()
    return master, sub


def test_report_waits_for_in_flight_correction_and_rejects_stale_snapshot(db_path):
    """读数更正持锁时报告生成必须等待；提交后报告因边界修订过期而拒绝。"""
    master, sub = _seed_two_meters_with_settlements(db_path)

    writer = connect(db_path)
    writer.execute("BEGIN IMMEDIATE")
    writer.execute(
        "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
        "VALUES ('M-MASTER', '2026-11-16T01:00:00Z', 2, 11, 'x')"
    )
    lock_held = threading.Event()
    release = threading.Event()
    outcomes: list[object] = []

    def hold_then_commit():
        lock_held.set()
        assert release.wait(timeout=10)
        writer.commit()

    def make_report():
        own = connect(db_path)
        try:
            lock_held.wait(timeout=10)
            try:
                create_difference_report(
                    own,
                    DifferenceReportCreate(
                        master_settlement_no=master,
                        sub_settlement_nos=[sub],
                        tolerance=5,
                    ),
                )
                outcomes.append("ok")
            except ApiError as exc:
                outcomes.append((exc.status_code, exc.code))
        finally:
            own.close()

    holder = threading.Thread(target=hold_then_commit)
    reporter = threading.Thread(target=make_report)
    holder.start()
    reporter.start()
    assert lock_held.wait(timeout=10)
    reporter.join(timeout=0.5)
    assert reporter.is_alive(), "报告生成不应绕过未提交的读数更正"
    release.set()
    holder.join(timeout=10)
    reporter.join(timeout=10)
    assert not reporter.is_alive()
    writer.close()

    assert outcomes == [(409, "difference_report_rejected")]
    check = connect(db_path)
    try:
        assert check.execute(
            "SELECT COUNT(*) AS n FROM difference_reports"
        ).fetchone()["n"] == 0
        assert check.execute(
            "SELECT COUNT(*) AS n FROM difference_report_hours"
        ).fetchone()["n"] == 0
    finally:
        check.close()


def test_concurrent_reports_serialize_and_store_complete_rows(db_path):
    """并发报告生成按写锁串行化，每份报告的头/表/边界/小时行都完整。"""
    master, sub = _seed_two_meters_with_settlements(db_path)
    payload = DifferenceReportCreate(
        master_settlement_no=master,
        sub_settlement_nos=[sub],
        tolerance=5,
    )
    results: list[object] = []
    lock = threading.Lock()
    barrier = threading.Barrier(5)

    def make_report():
        def run():
            own = connect(db_path)
            try:
                barrier.wait(timeout=10)
                report = create_difference_report(own, payload)
                with lock:
                    results.append(report)
            finally:
                own.close()

        return run

    run_threads([make_report() for _ in range(5)])

    assert len(results) == 5
    report_nos = {r.report_no for r in results}
    assert len(report_nos) == 5
    for report in results:
        assert len(report.hours_detail) == 2
        assert report.expired is False

    check = connect(db_path)
    try:
        assert check.execute(
            "SELECT COUNT(*) AS n FROM difference_reports"
        ).fetchone()["n"] == 5
        assert check.execute(
            "SELECT COUNT(*) AS n FROM difference_report_meters"
        ).fetchone()["n"] == 10
        assert check.execute(
            "SELECT COUNT(*) AS n FROM difference_report_hours"
        ).fetchone()["n"] == 10
        # 每份报告 2 个表 x 3 个边界整点 = 6 行
        assert check.execute(
            "SELECT COUNT(*) AS n FROM difference_report_boundaries"
        ).fetchone()["n"] == 30
    finally:
        check.close()


def test_failed_report_insert_rolls_back_everything(db_path):
    """小时行写入失败时，报告头与其他子表整体回滚，不留半份报告。"""
    master, sub = _seed_two_meters_with_settlements(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            "CREATE TRIGGER block_report_hours BEFORE INSERT ON difference_report_hours "
            "BEGIN SELECT RAISE(ABORT, 'simulated failure'); END;"
        )
        with pytest.raises(sqlite3.Error):
            create_difference_report(
                conn,
                DifferenceReportCreate(
                    master_settlement_no=master,
                    sub_settlement_nos=[sub],
                    tolerance=5,
                ),
            )
        assert conn.execute(
            "SELECT COUNT(*) FROM difference_reports"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM difference_report_meters"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM difference_report_boundaries"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM difference_report_hours"
        ).fetchone()[0] == 0
        # 原结算与读数未受影响
        assert conn.execute(
            "SELECT COUNT(*) FROM settlements"
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_http_concurrent_correction_and_report_never_half_visible(db_path, monkeypatch):
    """HTTP 层并发：更正与报告生成交错，可查到的报告始终完整，过期判定与修订状态一致。"""
    monkeypatch.setenv("ENERGY_DB_PATH", db_path)
    app = create_app(db_path)
    with TestClient(app) as client:
        for meter_id, values in (
            ("M-MASTER", [0, 10, 30]),
            ("M-SUB1", [0, 4, 9]),
        ):
            client.post("/meters", json={"meter_id": meter_id})
            client.post(
                f"/meters/{meter_id}/readings",
                json={"readings": [
                    {"hour": f"2026-11-16T{h:02d}:00:00Z", "value": v, "revision": 1}
                    for h, v in enumerate(values)
                ]},
            )
        master = client.post(
            "/meters/M-MASTER/settlements",
            json={"start_hour": "2026-11-16T00:00:00Z", "hours": 2},
        ).json()["settlement_no"]
        sub = client.post(
            "/meters/M-SUB1/settlements",
            json={"start_hour": "2026-11-16T00:00:00Z", "hours": 2},
        ).json()["settlement_no"]

        errors: list[BaseException] = []

        def corrector():
            try:
                for rev in range(2, 7):
                    response = client.post(
                        "/meters/M-MASTER/readings",
                        json={"readings": [
                            {"hour": "2026-11-16T01:00:00Z",
                             "value": 10 + rev, "revision": rev}
                        ]},
                    )
                    assert response.status_code == 200
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def reporter():
            try:
                for _ in range(10):
                    response = client.post(
                        "/difference-reports",
                        json={
                            "master_settlement_no": master,
                            "sub_settlement_nos": [sub],
                            "tolerance": 5,
                        },
                    )
                    # 更正已提交后，基于旧快照的报告必须被拒绝
                    assert response.status_code in (201, 409)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def reader():
            try:
                for _ in range(10):
                    listing = client.get("/difference-reports").json()
                    for summary in listing["items"]:
                        detail = client.get(
                            f"/difference-reports/{summary['report_no']}"
                        ).json()
                        assert len(detail["hours_detail"]) == detail["hours"]
                        assert len(detail["meters"]) == 2
                        # 同一响应内过期标记与过期边界列表必须一致
                        assert detail["expired"] == bool(detail["stale_boundaries"])
                        assert summary["anomaly_count"] == detail["anomaly_count"]
                        # 列表与详情是两个独立读事务，期间可能有更正提交，
                        # 因此只校验单调方向：详情看到的过期不会比摘要更少。
                        if summary["expired"]:
                            assert detail["expired"] is True
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        run_threads([corrector, reporter, reader])
        if errors:
            raise errors[0]
