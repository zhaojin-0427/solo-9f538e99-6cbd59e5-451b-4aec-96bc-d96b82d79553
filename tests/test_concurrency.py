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
from app.models import ReadingInput
from app.service import ApiError, ingest_readings, publish_settlement

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
