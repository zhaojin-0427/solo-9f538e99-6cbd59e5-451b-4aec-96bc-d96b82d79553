"""pytest 共享夹具。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.main import create_app  # noqa: E402


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "energy-test.sqlite3")


@pytest.fixture()
def client(db_path, monkeypatch):
    monkeypatch.setenv("ENERGY_DB_PATH", db_path)
    app = create_app(db_path)
    with TestClient(app) as test_client:
        yield test_client


def register(client: TestClient, meter_id: str = "M-001") -> None:
    response = client.post("/meters", json={"meter_id": meter_id, "name": "测试表"})
    assert response.status_code in (200, 201), response.text


def post_readings(client: TestClient, readings, meter_id: str = "M-001"):
    return client.post(f"/meters/{meter_id}/readings", json={"readings": readings})


def reading(hour: int, value: int, revision: int = 1, day: int = 16) -> dict:
    return {
        "hour": f"2026-11-{day:02d}T{hour:02d}:00:00Z",
        "value": value,
        "revision": revision,
    }
