"""差额报告：生成校验、逐小时差额、异常分类与过期标记。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from conftest import post_readings, reading, register
from fastapi.testclient import TestClient

BASE = datetime(2026, 11, 16, tzinfo=timezone.utc)


def fmt(offset: int) -> str:
    return (BASE + timedelta(hours=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")


def seed_meter(client: TestClient, meter_id: str, values: list[int],
               revision: int = 1) -> None:
    register(client, meter_id)
    response = post_readings(
        client,
        [
            {"hour": fmt(i), "value": value, "revision": revision}
            for i, value in enumerate(values)
        ],
        meter_id=meter_id,
    )
    assert response.status_code == 200, response.text


def publish(client: TestClient, meter_id: str, hours: int = 2,
            start_hour: int = 0) -> str:
    response = client.post(
        f"/meters/{meter_id}/settlements",
        json={"start_hour": fmt(start_hour), "hours": hours},
    )
    assert response.status_code == 201, response.text
    return response.json()["settlement_no"]


def create_report(client: TestClient, master: str, subs: list[str],
                  tolerance: int = 5):
    return client.post(
        "/difference-reports",
        json={
            "master_settlement_no": master,
            "sub_settlement_nos": subs,
            "tolerance": tolerance,
        },
    )


def three_meters(client: TestClient):
    # 总表用量：10、20；分表 S1：10、5；分表 S2：0、30
    seed_meter(client, "M-MASTER", [0, 10, 30])
    seed_meter(client, "M-S1", [0, 10, 15])
    seed_meter(client, "M-S2", [0, 0, 30])
    master = publish(client, "M-MASTER")
    s1 = publish(client, "M-S1")
    s2 = publish(client, "M-S2")
    return master, s1, s2


def test_creates_report_with_hourly_differences_and_anomalies(client):
    master, s1, s2 = three_meters(client)
    response = create_report(client, master, [s1, s2], tolerance=5)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["report_no"].startswith("RPT-")
    assert body["master_settlement_no"] == master
    assert body["sub_settlement_nos"] == [s1, s2]
    assert body["meter_ids"] == ["M-MASTER", "M-S1", "M-S2"]
    assert body["start_hour"] == fmt(0)
    assert body["end_hour"] == fmt(2)
    assert body["hours"] == 2
    assert body["tolerance"] == 5
    assert body["anomaly_count"] == 1
    assert body["expired"] is False
    assert body["stale_boundaries"] == []

    first, second = body["hours_detail"]
    # 小时 1：总 10 - (10 + 0) = 0，正常
    assert first["seq"] == 1
    assert first["master_usage"] == 10
    assert first["sub_usage_total"] == 10
    assert first["difference"] == 0
    assert first["is_anomaly"] is False
    assert first["reason"] == "ok"
    # 小时 2：总 20 - (5 + 30) = -15，负值异常
    assert second["seq"] == 2
    assert second["master_usage"] == 20
    assert second["sub_usage_total"] == 35
    assert second["difference"] == -15
    assert second["is_anomaly"] is True
    assert second["reason"] == "negative"

    assert [m["role"] for m in body["meters"]] == ["master", "sub", "sub"]
    assert [m["meter_id"] for m in body["meters"]] == [
        "M-MASTER",
        "M-S1",
        "M-S2",
    ]
    assert [m["settlement_no"] for m in body["meters"]] == [master, s1, s2]
    assert [m["seq"] for m in body["meters"]] == [0, 1, 2]


def test_tolerance_boundary_equal_is_ok_over_is_anomaly(client):
    # 总表用量 5、10；分表用量 0、0
    seed_meter(client, "M-MASTER", [0, 5, 15])
    seed_meter(client, "M-S1", [0, 0, 0])
    master = publish(client, "M-MASTER")
    sub = publish(client, "M-S1")

    ok = create_report(client, master, [sub], tolerance=10)
    assert ok.status_code == 201
    details = ok.json()["hours_detail"]
    # 差额恰等于容差 10 的时段正常；超过（不存在此例）则异常
    assert [(d["difference"], d["is_anomaly"], d["reason"]) for d in details] == [
        (5, False, "ok"),
        (10, False, "ok"),
    ]
    assert ok.json()["anomaly_count"] == 0

    tight = create_report(client, master, [sub], tolerance=4)
    assert tight.status_code == 201
    details = tight.json()["hours_detail"]
    assert [(d["difference"], d["is_anomaly"], d["reason"]) for d in details] == [
        (5, True, "exceeds_tolerance"),
        (10, True, "exceeds_tolerance"),
    ]
    assert tight.json()["anomaly_count"] == 2


def test_negative_and_exceeds_tolerance_classified_separately(client):
    seed_meter(client, "M-MASTER", [0, 4, 6])   # 用量 4、2
    seed_meter(client, "M-S1", [0, 0, 10])      # 用量 0、10
    master = publish(client, "M-MASTER")
    sub = publish(client, "M-S1")
    body = create_report(client, master, [sub], tolerance=1).json()
    assert [(d["difference"], d["reason"]) for d in body["hours_detail"]] == [
        (4, "exceeds_tolerance"),
        (-8, "negative"),
    ]
    assert body["anomaly_count"] == 2


def test_one_to_three_sub_meters_accepted(client):
    seed_meter(client, "M-MASTER", [0, 10])
    master = publish(client, "M-MASTER", hours=1)
    subs = []
    for i in range(3):
        meter_id = f"M-SUB{i}"
        seed_meter(client, meter_id, [0, 1])
        subs.append(publish(client, meter_id, hours=1))

    for count in (1, 2, 3):
        response = create_report(client, master, subs[:count], tolerance=99)
        assert response.status_code == 201, response.text
        assert len(response.json()["meters"]) == count + 1

    zero = client.post(
        "/difference-reports",
        json={
            "master_settlement_no": master,
            "sub_settlement_nos": [],
            "tolerance": 1,
        },
    )
    assert zero.status_code == 422

    seed_meter(client, "M-SUB3", [0, 1])
    subs.append(publish(client, "M-SUB3", hours=1))
    four = create_report(client, master, subs, tolerance=99)
    assert four.status_code == 422


def test_tolerance_must_be_non_negative_integer(client):
    master, s1, _ = three_meters(client)
    for bad in (-1, "1", 1.5, None):
        response = client.post(
            "/difference-reports",
            json={
                "master_settlement_no": master,
                "sub_settlement_nos": [s1],
                "tolerance": bad,
            },
        )
        assert response.status_code == 422, bad


def test_duplicate_sub_settlement_no_rejected_422(client):
    master, s1, _ = three_meters(client)
    response = create_report(client, master, [s1, s1])
    assert response.status_code == 422


def test_unknown_settlement_no_rejected_without_half_report(client):
    master, s1, _ = three_meters(client)
    response = create_report(client, "STL-nope", [s1])
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "difference_report_rejected"
    assert body["atomic"] is True
    assert body["problems"][0]["code"] == "settlement_not_found"
    assert body["problems"][0]["settlement_no"] == "STL-nope"
    assert client.get("/difference-reports").json()["count"] == 0

    both_unknown = create_report(client, "STL-a", ["STL-b", "STL-c"])
    assert both_unknown.status_code == 409
    assert {p["settlement_no"] for p in both_unknown.json()["problems"]} == {
        "STL-a",
        "STL-b",
        "STL-c",
    }


def test_same_meter_in_master_and_sub_rejected(client):
    seed_meter(client, "M-MASTER", [0, 10, 30])
    first = publish(client, "M-MASTER")
    second = publish(client, "M-MASTER")  # 同表不同结算号
    response = create_report(client, first, [second])
    assert response.status_code == 409
    problem = response.json()["problems"][0]
    assert problem["code"] == "duplicate_meter"
    assert problem["meter_id"] == "M-MASTER"
    assert client.get("/difference-reports").json()["count"] == 0


def test_identical_settlement_ref_in_master_and_sub_rejected(client):
    master, _, _ = three_meters(client)
    # 同一个结算号同时充当总表和分表：电表必然重复
    response = create_report(client, master, [master])
    assert response.status_code == 409
    codes = {p["code"] for p in response.json()["problems"]}
    assert "duplicate_meter" in codes
    assert client.get("/difference-reports").json()["count"] == 0


def test_interval_mismatch_rejected(client):
    seed_meter(client, "M-MASTER", [0, 10, 30, 60])
    seed_meter(client, "M-S1", [0, 5, 12, 20])
    master_2h = publish(client, "M-MASTER", hours=2)
    sub_1h = publish(client, "M-S1", hours=1)
    sub_2h = publish(client, "M-S1", hours=2)

    response = create_report(client, master_2h, [sub_1h])
    assert response.status_code == 409
    problem = response.json()["problems"][0]
    assert problem["code"] == "interval_mismatch"
    assert problem["settlement_no"] == sub_1h
    assert problem["expected_start_hour"] == fmt(0)
    assert problem["expected_end_hour"] == fmt(2)
    assert client.get("/difference-reports").json()["count"] == 0

    # 同长度但起点不同也算区间不一致
    shifted = client.post(
        "/meters/M-S1/settlements",
        json={"start_hour": fmt(1), "hours": 2},
    ).json()["settlement_no"]
    shifted_response = create_report(client, master_2h, [shifted])
    assert shifted_response.status_code == 409
    assert shifted_response.json()["problems"][0]["code"] == "interval_mismatch"

    # 区间一致后可正常生成
    ok = create_report(client, master_2h, [sub_2h])
    assert ok.status_code == 201


def test_creation_rejected_when_boundary_revision_superseded(client):
    master_no, s1, _ = three_meters(client)

    # 更正总表 01 点读数（影响结算所用边界），原结算快照不变
    corrected = post_readings(client, [reading(1, 11, 2)], meter_id="M-MASTER")
    assert corrected.status_code == 200

    response = create_report(client, master_no, [s1])
    assert response.status_code == 409
    problem = response.json()["problems"][0]
    assert problem["code"] == "boundary_revision_superseded"
    assert problem["meter_id"] == "M-MASTER"
    assert problem["hour"] == fmt(1)
    assert problem["used_revision"] == 1
    assert problem["current_revision"] == 2
    assert client.get("/difference-reports").json()["count"] == 0

    # 基于更正后的当前修订重新发布结算，再生成报告即成功
    new_master = publish(client, "M-MASTER")
    ok = create_report(client, new_master, [s1])
    assert ok.status_code == 201, ok.text


def test_report_marked_expired_after_new_revision_even_same_value(client):
    master_no, s1, _ = three_meters(client)
    created = create_report(client, master_no, [s1], tolerance=5)
    assert created.status_code == 201
    report_no = created.json()["report_no"]

    # 同值新修订：读数内容没变，但出现了新修订版本
    resubmit = post_readings(
        client,
        [{"hour": fmt(1), "value": 10, "revision": 2}],
        meter_id="M-MASTER",
    )
    assert resubmit.status_code == 200
    assert resubmit.json()["readings"][0]["status"] == "accepted"

    detail = client.get(f"/difference-reports/{report_no}").json()
    assert detail["expired"] is True
    stale = detail["stale_boundaries"]
    assert len(stale) == 1
    assert stale[0] == {
        "meter_id": "M-MASTER",
        "hour": fmt(1),
        "used_revision": 1,
        "current_revision": 2,
    }
    # 原差额明细仍可追溯，未被改写
    assert detail["hours_detail"] == created.json()["hours_detail"]
    assert detail["anomaly_count"] == created.json()["anomaly_count"]

    listing = client.get("/difference-reports").json()
    assert listing["count"] == 1
    assert listing["items"][0]["report_no"] == report_no
    assert listing["items"][0]["expired"] is True


def test_fresh_report_not_expired_and_listing_order(client):
    master, s1, s2 = three_meters(client)
    first = create_report(client, master, [s1]).json()
    second = create_report(client, master, [s1, s2], tolerance=99).json()

    items = client.get("/difference-reports").json()["items"]
    assert [i["report_no"] for i in items] == [
        second["report_no"],
        first["report_no"],
    ]
    assert all(i["expired"] is False for i in items)
    summary = items[0]
    assert "hours_detail" not in summary
    assert "stale_boundaries" not in summary
    assert summary["sub_settlement_nos"] == [s1, s2]
    assert summary["meter_ids"] == ["M-MASTER", "M-S1", "M-S2"]


def test_get_unknown_report_404(client):
    response = client.get("/difference-reports/RPT-nope")
    assert response.status_code == 404
    assert response.json()["code"] == "difference_report_not_found"


def test_report_persists_across_app_restart(db_path):
    from app.main import create_app

    with TestClient(create_app(db_path)) as first:
        master, s1, _ = three_meters(first)
        report = create_report(first, master, [s1]).json()

    with TestClient(create_app(db_path)) as second:
        fetched = second.get(
            f"/difference-reports/{report['report_no']}"
        ).json()
        assert fetched["report_no"] == report["report_no"]
        assert fetched["hours_detail"] == report["hours_detail"]
        assert fetched["expired"] is False
        assert second.get("/difference-reports").json()["count"] == 1


def test_multiple_stale_boundaries_across_meters_all_listed(client):
    master_no, s1, s2 = three_meters(client)
    report = create_report(client, master_no, [s1, s2]).json()["report_no"]

    # 总表与两个分表各更正一个边界时刻（同值新修订也算）
    post_readings(client, [{"hour": fmt(0), "value": 0, "revision": 2}],
                  meter_id="M-MASTER")
    post_readings(client, [{"hour": fmt(1), "value": 10, "revision": 2}],
                  meter_id="M-S1")
    post_readings(client, [{"hour": fmt(2), "value": 30, "revision": 2}],
                  meter_id="M-S2")

    detail = client.get(f"/difference-reports/{report}").json()
    assert detail["expired"] is True
    assert {(b["meter_id"], b["hour"], b["used_revision"], b["current_revision"])
            for b in detail["stale_boundaries"]} == {
        ("M-MASTER", fmt(0), 1, 2),
        ("M-S1", fmt(1), 1, 2),
        ("M-S2", fmt(2), 1, 2),
    }
    # 列表摘要同样标记过期
    assert client.get("/difference-reports").json()["items"][0]["expired"] is True


def test_superseded_boundary_on_unused_hour_does_not_expire(client):
    # 只更正结算区间之外的时刻，不影响报告所用边界
    master_no, s1, _ = three_meters(client)
    report = create_report(client, master_no, [s1]).json()["report_no"]
    post_readings(
        client,
        [{"hour": fmt(5), "value": 999, "revision": 9}],
        meter_id="M-MASTER",
    )
    detail = client.get(f"/difference-reports/{report}").json()
    assert detail["expired"] is False
    assert detail["stale_boundaries"] == []


def test_48_hour_report_supported(client):
    seed_meter(client, "M-MASTER", [i * 10 for i in range(49)])
    seed_meter(client, "M-S1", [i * 5 for i in range(49)])
    master = publish(client, "M-MASTER", hours=48)
    sub = publish(client, "M-S1", hours=48)
    body = create_report(client, master, [sub], tolerance=1000).json()
    assert body["hours"] == 48
    assert len(body["hours_detail"]) == 48
    assert body["end_hour"] == (BASE + timedelta(hours=48)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def test_report_endpoints_in_openapi_without_conn_leak(client):
    spec = client.get("/openapi.json").json()
    assert "/difference-reports" in spec["paths"]
    assert "/difference-reports/{report_no}" in spec["paths"]
    for path in ("/difference-reports", "/difference-reports/{report_no}"):
        for operation in spec["paths"][path].values():
            names = {p["name"] for p in operation.get("parameters", [])}
            assert "conn" not in names
