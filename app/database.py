"""SQLite 连接、事务与表结构。"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DEFAULT_DB_PATH = "./data/energy.sqlite3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meters (
    meter_id   TEXT PRIMARY KEY,
    name       TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS readings (
    meter_id    TEXT    NOT NULL REFERENCES meters(meter_id) ON DELETE RESTRICT,
    hour        TEXT    NOT NULL,
    revision    INTEGER NOT NULL CHECK (revision > 0),
    value       INTEGER NOT NULL CHECK (value BETWEEN 0 AND 9999),
    received_at TEXT    NOT NULL,
    PRIMARY KEY (meter_id, hour, revision)
);

CREATE INDEX IF NOT EXISTS idx_readings_meter_hour_revision
    ON readings (meter_id, hour, revision DESC);

CREATE TABLE IF NOT EXISTS settlements (
    settlement_no TEXT    PRIMARY KEY,
    meter_id      TEXT    NOT NULL REFERENCES meters(meter_id) ON DELETE RESTRICT,
    start_hour    TEXT    NOT NULL,
    end_hour      TEXT    NOT NULL,
    hours         INTEGER NOT NULL CHECK (hours BETWEEN 1 AND 48),
    total_usage   INTEGER NOT NULL CHECK (total_usage >= 0),
    published_at  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_settlements_meter_published
    ON settlements (meter_id, published_at DESC, settlement_no DESC);

CREATE TABLE IF NOT EXISTS settlement_intervals (
    settlement_no  TEXT    NOT NULL REFERENCES settlements(settlement_no) ON DELETE CASCADE,
    seq            INTEGER NOT NULL CHECK (seq >= 0),
    hour_start     TEXT    NOT NULL,
    hour_end       TEXT    NOT NULL,
    usage          INTEGER NOT NULL CHECK (usage BETWEEN 0 AND 9999),
    start_value    INTEGER NOT NULL CHECK (start_value BETWEEN 0 AND 9999),
    start_revision INTEGER NOT NULL CHECK (start_revision > 0),
    end_value      INTEGER NOT NULL CHECK (end_value BETWEEN 0 AND 9999),
    end_revision   INTEGER NOT NULL CHECK (end_revision > 0),
    PRIMARY KEY (settlement_no, seq)
);
"""


def resolve_db_path(path: str | None = None) -> str:
    """环境变量 ENERGY_DB_PATH 优先，其次显式参数，最后默认值。"""
    return os.environ.get("ENERGY_DB_PATH") or path or DEFAULT_DB_PATH


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_path,
        timeout=30.0,
        isolation_level=None,  # 事务由 write_tx/read_tx 显式控制
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: str) -> None:
    if db_path != ":memory:":
        parent = Path(db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        # executescript 会隐式提交，故按分号拆成独立语句，保持建表原子性。
        statements = [s.strip() for s in SCHEMA.split(";") if s.strip()]
        conn.execute("BEGIN IMMEDIATE")
        try:
            for statement in statements:
                conn.execute(statement)
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """立即取得写锁；提交顺序即并发写入的裁决顺序。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        try:
            conn.execute("COMMIT")
        except BaseException:
            # 提交失败时事务可能仍然打开；仅在仍处于事务中时回滚。
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise


@contextmanager
def read_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """读事务提供单次提交状态的一致快照。"""
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    else:
        try:
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
