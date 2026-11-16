"""读数录入与版本裁决。"""

from __future__ import annotations

from conftest import post_readings, reading, register


def test_register_is_idempotent(client):
    first = client.post("/meters", json={"meter_id": "M-001", "name": "一号表"})
    second = client.post("/meters", json={"meter_id": "M-001", "name": "别的名字"})
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["created"] is True
    assert second.json()["created"] is False
    assert second.json()["meter"]["name"] == "一号表"

    listing = client.get("/meters").json()
    assert listing["count"] == 1


def test_meter_id_validation(client):
    assert client.post("/meters", json={"meter_id": "bad id"}).status_code == 422
    assert client.post("/meters", json={"meter_id": ""}).status_code == 422
    assert client.post("/meters", json={"meter_id": "a/b"}).status_code == 422


def test_unknown_meter_returns_404(client):
    response = post_readings(client, [reading(0, 100)])
    assert response.status_code == 404
    assert response.json()["code"] == "meter_not_found"


def test_accepts_z_and_rejects_offset_non_hour_or_naive(client):
    register(client)
    accepted = post_readings(
        client,
        [{"hour": "2026-11-16T00:00:00Z", "value": 1, "revision": 1}],
    )
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["readings"][0]["hour"] == "2026-11-16T00:00:00Z"

    for bad in [
        "2026-11-16T01:00:00+01:00",  # 虽等价于整点，但输入必须是 UTC
        "2026-11-16T00:30:00Z",
        "2026-11-16T00:00:01Z",
        "2026-11-16T00:00:00",
        "2026-11-16 00:00:00Z",
    ]:
        response = post_readings(
            client, [{"hour": bad, "value": 1, "revision": 1}]
        )
        assert response.status_code == 422, bad


def test_value_and_revision_bounds(client):
    register(client)
    for payload in [
        {"hour": "2026-11-16T00:00:00Z", "value": -1, "revision": 1},
        {"hour": "2026-11-16T00:00:00Z", "value": 10000, "revision": 1},
        {"hour": "2026-11-16T00:00:00Z", "value": 1, "revision": 0},
        {"hour": "2026-11-16T00:00:00Z", "value": 1, "revision": 1.5},
        {"hour": "2026-11-16T00:00:00Z", "value": "12", "revision": 1},
    ]:
        assert post_readings(client, [payload]).status_code == 422, payload


def test_same_revision_same_value_is_idempotent(client):
    register(client)
    first = post_readings(client, [reading(0, 100, 1)])
    second = post_readings(client, [reading(0, 100, 1)])
    assert first.status_code == second.status_code == 200
    assert first.json()["accepted"] == 1
    assert second.json()["accepted"] == 0
    assert second.json()["unchanged"] == 1
    readings = client.get("/meters/M-001/readings").json()
    assert readings["count"] == 1
    history = client.get(
        "/meters/M-001/readings", params={"include_history": True}
    ).json()
    assert history["count"] == 1


def test_higher_revision_supersedes(client):
    register(client)
    post_readings(client, [reading(0, 100, 1)])
    second = post_readings(client, [reading(0, 105, 2)])
    assert second.status_code == 200
    current = client.get("/meters/M-001/readings").json()["readings"]
    assert current == [
        {
            "hour": "2026-11-16T00:00:00Z",
            "value": 105,
            "revision": 2,
            "received_at": current[0]["received_at"],
        }
    ]
    history = client.get(
        "/meters/M-001/readings", params={"include_history": True}
    ).json()
    assert [(r["revision"], r["value"]) for r in history["readings"]] == [
        (1, 100),
        (2, 105),
    ]


def test_stale_revision_rejects_whole_batch(client):
    register(client)
    post_readings(client, [reading(0, 100, 2), reading(1, 150, 1)])

    response = post_readings(
        client,
        [
            reading(0, 999, 1),   # 旧版
            reading(2, 300, 1),   # 本来可以写入
        ],
    )
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "reading_conflict"
    assert body["atomic"] is True
    assert [c["reason"] for c in body["conflicts"]] == ["stale_revision"]
    assert body["conflicts"][0]["current_revision"] == 2

    # 整批回滚：02 点没有被写入
    hours = [
        r["hour"]
        for r in client.get("/meters/M-001/readings").json()["readings"]
    ]
    assert hours == ["2026-11-16T00:00:00Z", "2026-11-16T01:00:00Z"]


def test_same_revision_different_value_rejects_batch(client):
    register(client)
    post_readings(client, [reading(0, 100, 1)])
    response = post_readings(client, [reading(0, 101, 1), reading(1, 200, 1)])
    assert response.status_code == 409
    conflict = response.json()["conflicts"][0]
    assert conflict["reason"] == "same_revision_different_value"
    assert conflict["current_value"] == 100
    assert len(client.get("/meters/M-001/readings").json()["readings"]) == 1


def test_duplicate_hour_in_batch_conflicts_without_partial_write(client):
    register(client)
    response = post_readings(
        client, [reading(0, 100, 1), reading(0, 100, 2), reading(1, 150, 1)]
    )
    assert response.status_code == 409
    assert response.json()["conflicts"][0]["reason"] == "duplicate_hour_in_batch"
    assert client.get("/meters/M-001/readings").json()["count"] == 0


def test_conflict_reports_all_bad_items(client):
    register(client)
    post_readings(client, [reading(0, 100, 3), reading(1, 200, 3)])
    response = post_readings(
        client, [reading(0, 1, 2), reading(1, 201, 3)]
    )
    reasons = sorted(c["reason"] for c in response.json()["conflicts"])
    assert reasons == ["same_revision_different_value", "stale_revision"]


def test_large_revision_jump_and_boundary_value(client):
    register(client)
    payload = post_readings(
        client,
        [reading(0, 0, 9_223_372_036_854_775_807)],
    )
    assert payload.status_code == 200, payload.text
    assert client.get("/meters/M-001/readings").json()["readings"][0]["value"] == 0


def test_identical_duplicate_items_in_batch_collapse(client):
    register(client)
    response = post_readings(
        client, [reading(0, 100, 1), reading(0, 100, 1)]
    )
    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 1
    assert client.get("/meters/M-001/readings").json()["count"] == 1


def test_batch_with_empty_list_rejected(client):
    register(client)
    assert post_readings(client, []).status_code == 422


def test_reading_filters_accept_only_utc_hour(client):
    register(client)
    for bad in ["2026-11-16T00:30:00Z", "2026-11-16"]:
        response = client.get(
            "/meters/M-001/readings", params={"start_hour": bad}
        )
        assert response.status_code == 422, bad
