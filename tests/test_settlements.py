"""逐小时结算发布、校验与历史快照。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from conftest import post_readings, reading, register


def hour_at(index: int, day: int = 16) -> str:
    base = datetime(2026, 11, day, tzinfo=timezone.utc) + timedelta(hours=index)
    return base.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_hour(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def seed(client, values, revision: int = 1, meter_id: str = "M-001",
         start: datetime | None = None):
    """values 从 start（默认 11-16 00:00）开始逐小时写入；None 表示该小时缺失。"""
    register(client, meter_id)
    base = start or datetime(2026, 11, 16, tzinfo=timezone.utc)
    readings = [
        {
            "hour": fmt_hour(base + timedelta(hours=i)),
            "value": value,
            "revision": revision,
        }
        for i, value in enumerate(values)
        if value is not None
    ]
    if not readings:
        return
    response = post_readings(client, readings, meter_id=meter_id)
    assert response.status_code == 200, response.text


def publish(client, hours, start_hour: int = 0, meter_id: str = "M-001"):
    return client.post(
        f"/meters/{meter_id}/settlements",
        json={
            "start_hour": f"2026-11-16T{start_hour:02d}:00:00Z",
            "hours": hours,
        },
    )


def test_publishes_hourly_differences(client):
    seed(client, [100, 110, 130])
    response = publish(client, 2)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["meter_id"] == "M-001"
    assert body["start_hour"] == "2026-11-16T00:00:00Z"
    assert body["end_hour"] == "2026-11-16T02:00:00Z"
    assert body["hours"] == 2
    assert body["total_usage"] == 30
    assert [i["usage"] for i in body["intervals"]] == [10, 20]
    assert body["intervals"][0] == {
        "seq": 1,
        "hour_start": "2026-11-16T00:00:00Z",
        "hour_end": "2026-11-16T01:00:00Z",
        "usage": 10,
        "start_reading": {"value": 100, "revision": 1},
        "end_reading": {"value": 110, "revision": 1},
    }
    assert body["settlement_no"].startswith("STL-")


def test_rollover_is_added_once(client):
    seed(client, [9500, 9999, 100, 499])
    body = publish(client, 3).json()
    # 9999 -> 100 回卷：100 + 10000 - 9999 = 101
    assert [i["usage"] for i in body["intervals"]] == [499, 101, 399]
    assert body["total_usage"] == 999


def test_usage_exactly_500_allowed_501_rejected(client):
    seed(client, [0, 500])
    ok = publish(client, 1)
    assert ok.status_code == 201
    assert ok.json()["total_usage"] == 500

    seed(client, [7000, 7501], meter_id="M-002")
    rejected = publish(client, 1, meter_id="M-002")
    assert rejected.status_code == 409
    problem = rejected.json()["problems"][0]
    assert problem["code"] == "hourly_usage_exceeds_limit"
    assert problem["usage"] == 501
    assert problem["limit"] == 500
    assert problem["start_hour"] == "2026-11-16T00:00:00Z"
    assert problem["end_hour"] == "2026-11-16T01:00:00Z"


def test_rollover_can_look_like_normal_difference(client):
    # 9999 -> 0 回卷用量为 1，而不是 -9999。
    seed(client, [9999, 0])
    body = publish(client, 1).json()
    assert body["intervals"][0]["usage"] == 1


def test_missing_readings_reported_without_publishing(client):
    seed(client, [100, 110, None, 150, None, None])
    response = publish(client, 5)
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "publish_rejected"
    assert body["atomic"] is True
    codes = [p["code"] for p in body["problems"]]
    assert codes == ["missing_reading", "missing_reading"]
    missing = body["problems"]
    assert missing[0]["missing_readings"] == ["2026-11-16T02:00:00Z"]
    # 04、05 点连续缺失被合并为一个问题区间
    assert missing[1]["start_hour"] == "2026-11-16T04:00:00Z"
    assert missing[1]["end_hour"] == "2026-11-16T06:00:00Z"
    assert missing[1]["missing_readings"] == [
        "2026-11-16T04:00:00Z",
        "2026-11-16T05:00:00Z",
    ]

    # 未发布任何结算，也查不到半份数据
    listing = client.get("/meters/M-001/settlements").json()
    assert listing["count"] == 0
    assert client.get("/settlements").json()["count"] == 0


def test_consecutive_missing_readings_merged_into_one_interval(client):
    seed(client, [100, None, None, None, 200])
    body = publish(client, 4).json()
    assert [p["code"] for p in body["problems"]] == ["missing_reading"]
    problem = body["problems"][0]
    assert problem["start_hour"] == "2026-11-16T01:00:00Z"
    assert problem["end_hour"] == "2026-11-16T04:00:00Z"
    assert problem["missing_readings"] == [
        "2026-11-16T01:00:00Z",
        "2026-11-16T02:00:00Z",
        "2026-11-16T03:00:00Z",
    ]


def test_missing_boundary_reading(client):
    seed(client, [100, 110])
    body = publish(client, 2).json()
    assert body["problems"][0]["missing_readings"] == ["2026-11-16T02:00:00Z"]


def test_missing_and_over_limit_reported_together(client):
    seed(client, [0, 9000, None])
    response = publish(client, 2)
    assert response.status_code == 409
    problems = response.json()["problems"]
    assert [p["code"] for p in problems] == [
        "hourly_usage_exceeds_limit",
        "missing_reading",
    ]


def test_hours_bounds(client):
    seed(client, [i * 10 for i in range(49)])
    for hours in (0, 49):
        assert publish(client, hours).status_code == 422
    assert publish(client, 48).status_code == 201
    # 48 小时需 49 个边界读数，第二天 23 点只是第 48 个区间终点
    assert publish(client, 48).status_code == 201


def test_settlement_uses_current_committed_revisions(client):
    seed(client, [100, 110, 130])
    # 更正 01 点读数，01 点两个相邻区间都随之变化
    corrected = post_readings(client, [reading(1, 120, 2)])
    assert corrected.status_code == 200
    body = publish(client, 2).json()
    assert [i["usage"] for i in body["intervals"]] == [20, 10]
    assert body["intervals"][0]["end_reading"] == {"value": 120, "revision": 2}
    assert body["intervals"][1]["start_reading"] == {"value": 120, "revision": 2}
    assert body["total_usage"] == 30


def test_published_settlement_is_immutable_snapshot(client):
    seed(client, [100, 110, 130])
    first = publish(client, 2).json()

    # 发布后补录/更正，历史结算号仍返回原明细和原修订
    post_readings(client, [reading(1, 120, 2), reading(2, 150, 2)])
    fetched = client.get(f"/settlements/{first['settlement_no']}").json()
    assert fetched == first
    assert [i["start_reading"]["revision"] for i in fetched["intervals"]] == [1, 1]

    # 新发布使用新提交状态的读数
    second = publish(client, 2).json()
    assert second["settlement_no"] != first["settlement_no"]
    assert [i["usage"] for i in second["intervals"]] == [20, 30]


def test_settlement_listing_filters_by_meter(client):
    seed(client, [0, 10], meter_id="M-001")
    seed(client, [0, 20], meter_id="M-002")
    a = publish(client, 1, meter_id="M-001").json()
    b = publish(client, 1, meter_id="M-002").json()

    all_items = client.get("/settlements").json()
    assert {i["settlement_no"] for i in all_items["items"]} == {
        a["settlement_no"],
        b["settlement_no"],
    }
    only_a = client.get("/meters/M-001/settlements").json()
    assert [i["settlement_no"] for i in only_a["items"]] == [a["settlement_no"]]
    assert "intervals" not in only_a["items"][0]
    assert client.get("/meters/UNKNOWN/settlements").status_code == 404
    assert client.get("/settlements/STL-nope").status_code == 404


def test_publish_unknown_meter_404(client):
    response = publish(client, 1)
    assert response.status_code == 404
    assert response.json()["code"] == "meter_not_found"


def test_readings_query_range_and_history(client):
    seed(client, [0, 10, 20])
    post_readings(client, [reading(1, 15, 2)])

    current = client.get(
        "/meters/M-001/readings",
        params={"start_hour": "2026-11-16T01:00:00Z", "end_hour": "2026-11-16T02:00:00Z"},
    ).json()
    assert [(r["hour"], r["value"], r["revision"]) for r in current["readings"]] == [
        ("2026-11-16T01:00:00Z", 15, 2),
        ("2026-11-16T02:00:00Z", 20, 1),
    ]

    history = client.get(
        "/meters/M-001/readings",
        params={"include_history": True, "start_hour": "2026-11-16T01:00:00Z"},
    ).json()
    assert [(r["value"], r["revision"]) for r in history["readings"]] == [
        (10, 1),
        (15, 2),
        (20, 1),
    ]
    assert history["include_history"] is True

    bad_order = client.get(
        "/meters/M-001/readings",
        params={"start_hour": "2026-11-16T03:00:00Z", "end_hour": "2026-11-16T01:00:00Z"},
    )
    assert bad_order.status_code == 422


def test_meter_summary_counts(client):
    seed(client, [0, 10, 20])
    post_readings(client, [reading(1, 11, 2)])
    view = client.get("/meters/M-001").json()
    assert view["hour_count"] == 3
    assert view["reading_versions"] == 4
    assert view["latest_hour"] == "2026-11-16T02:00:00Z"
    assert view["name"] == "测试表"


def test_multi_day_publish(client):
    seed(client, [i * 10 for i in range(49)], meter_id="M-003")
    body = publish(client, 48, start_hour=0, meter_id="M-003").json()
    assert body["hours"] == 48
    assert body["end_hour"] == "2026-11-18T00:00:00Z"
    assert len(body["intervals"]) == 48
    assert body["total_usage"] == 480


def test_docs_and_openapi_available(client):
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    assert spec["info"]["title"]
    assert "/meters/{meter_id}/settlements" in spec["paths"]
    # OpenAPI 参数中不应泄露数据库连接依赖
    for operations in spec["paths"].values():
        for operation in operations.values():
            names = {p["name"] for p in operation.get("parameters", [])}
            assert "conn" not in names


def test_data_survives_app_restart(db_path):
    from app.main import create_app

    with TestClient(create_app(db_path)) as first:
        register(first)
        seed_values = [100, 120, 150]
        base = datetime(2026, 11, 16, tzinfo=timezone.utc)
        response = post_readings(
            first,
            [
                {"hour": fmt_hour(base + timedelta(hours=i)), "value": v, "revision": 1}
                for i, v in enumerate(seed_values)
            ],
        )
        assert response.status_code == 200
        settlement = first.post(
            "/meters/M-001/settlements",
            json={"start_hour": fmt_hour(base), "hours": 2},
        ).json()

    with TestClient(create_app(db_path)) as second:
        fetched = second.get(f"/settlements/{settlement['settlement_no']}").json()
        assert fetched == settlement
        assert fetched["total_usage"] == 50
