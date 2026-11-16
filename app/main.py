"""FastAPI 应用装配与 HTTP 接口。"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Annotated, Iterator

from fastapi import Depends, FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AfterValidator
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__
from .database import connect, init_db, resolve_db_path
from .models import (
    IngestResponse,
    MeterCreate,
    MeterCreateResult,
    MeterList,
    MeterView,
    ReadingBatch,
    ReadingList,
    SettlementCreate,
    SettlementDetail,
    SettlementList,
    UtcHour,
    validate_meter_id,
)
from .service import (
    ApiError,
    create_meter,
    get_meter,
    get_settlement,
    ingest_readings,
    list_meters,
    list_readings,
    list_settlements,
    publish_settlement,
)

DESCRIPTION = """
园区电表累计值 → 逐小时结算明细。

**读数录入**
- 以 `(meter_id, hour, revision)` 唯一标识一次读数版本，版本只追加、不覆盖。
- 同一小时只接受比当前修订号更高的修订；同版同值重传幂等；旧版或同版异值会使整批 409 并全部回滚。
- 同一批次内同一小时不得出现不同内容。

**结算发布**
- 选择连续 1～48 个整点小时，需要 `hours + 1` 个边界读数。
- 相邻读数求差；后值小于前值视为一次 10000 回卷（`end + 10000 - start`）。
- 缺少读数或单小时用量大于 500 时返回 409 和问题区间，不落任何结算数据。
- 发布在单个 SQLite `BEGIN IMMEDIATE` 事务中读取当前提交状态，并把各区间所用读数版本固化进明细。

**已发布结算**
- 结算号查询永远返回发布时的明细和读数修订，后续补录/更正不影响历史结果。
"""


def _get_conn(request: Request) -> Iterator[sqlite3.Connection]:
    conn = connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()


Conn = Annotated[sqlite3.Connection, Depends(_get_conn)]
MeterId = Annotated[
    str, Path(min_length=1, max_length=64), AfterValidator(validate_meter_id)
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(app.state.db_path)
    yield


def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(
        title="园区电表逐小时结算 API",
        description=DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
    )
    app.state.db_path = resolve_db_path(db_path)

    @app.exception_handler(ApiError)
    async def _api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.detail())

    @app.exception_handler(RequestValidationError)
    async def _validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = []
        for error in exc.errors():
            errors.append(
                {
                    "type": error.get("type"),
                    "loc": list(error.get("loc", ())),
                    "msg": error.get("msg"),
                }
            )
        return JSONResponse(
            status_code=422,
            content={
                "code": "validation_error",
                "message": "请求参数校验失败",
                "errors": errors,
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error_handler(
        _: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        if isinstance(exc.detail, dict):
            content = exc.detail
        else:
            content = {"code": "http_error", "message": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=content)

    @app.exception_handler(sqlite3.Error)
    async def _sqlite_error_handler(_: Request, __: sqlite3.Error) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={"code": "database_error", "message": "数据库操作失败，未提交任何更改"},
        )

    @app.get("/healthz", tags=["运维"], summary="健康检查")
    def healthz(conn: Conn) -> dict:
        conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "version": __version__}

    @app.get("/", tags=["运维"], include_in_schema=False)
    def index() -> dict:
        return {
            "name": "园区电表逐小时结算 API",
            "version": __version__,
            "docs": "/docs",
            "openapi": "/openapi.json",
        }

    @app.post(
        "/meters",
        response_model=MeterCreateResult,
        status_code=201,
        tags=["电表"],
        summary="登记电表（重复登记幂等）",
        responses={
            200: {
                "model": MeterCreateResult,
                "description": "电表已存在，返回现有记录（created=false）",
            },
        },
    )
    def post_meter(payload: MeterCreate, conn: Conn) -> JSONResponse:
        created, view = create_meter(conn, payload.meter_id, payload.name)
        result = MeterCreateResult(created=created, meter=view)
        return JSONResponse(
            status_code=201 if created else 200,
            content=result.model_dump(mode="json"),
        )

    @app.get(
        "/meters",
        response_model=MeterList,
        tags=["电表"],
        summary="列出电表",
    )
    def get_meters(conn: Conn) -> MeterList:
        views = list_meters(conn)
        return MeterList(meters=views, count=len(views))

    @app.get(
        "/meters/{meter_id}",
        response_model=MeterView,
        tags=["电表"],
        summary="电表概况",
    )
    def get_meter_detail(meter_id: MeterId, conn: Conn) -> MeterView:
        return get_meter(conn, meter_id)

    @app.post(
        "/meters/{meter_id}/readings",
        response_model=IngestResponse,
        tags=["读数"],
        summary="批量录入累计读数（整批原子）",
        responses={
            409: {
                "description": "批次与当前读数版本冲突，整批未写入",
                "content": {"application/json": {"example": {
                    "code": "reading_conflict",
                    "message": "读数批次与已提交版本冲突，整批未写入",
                    "atomic": True,
                    "conflicts": [{
                        "hour": "2026-11-16T01:00:00Z",
                        "reason": "stale_revision",
                        "provided_revision": 1,
                        "provided_value": 120,
                        "current_revision": 2,
                        "current_value": 125,
                    }],
                }}},
            },
        },
    )
    def post_readings(
        meter_id: MeterId, payload: ReadingBatch, conn: Conn
    ) -> IngestResponse:
        return ingest_readings(conn, meter_id, payload.readings)

    @app.get(
        "/meters/{meter_id}/readings",
        response_model=ReadingList,
        tags=["读数"],
        summary="查询读数（默认每时刻当前版本）",
    )
    def get_readings(
        meter_id: MeterId,
        conn: Conn,
        start_hour: UtcHour | None = Query(default=None, description="起始 UTC 整点（含）"),
        end_hour: UtcHour | None = Query(default=None, description="结束 UTC 整点（含）"),
        include_history: bool = Query(
            default=False, description="true 时返回同一时刻的全部历史修订"
        ),
    ) -> ReadingList:
        readings = list_readings(conn, meter_id, start_hour, end_hour, include_history)
        return ReadingList(
            meter_id=meter_id,
            include_history=include_history,
            readings=readings,
            count=len(readings),
        )

    @app.post(
        "/meters/{meter_id}/settlements",
        response_model=SettlementDetail,
        status_code=201,
        tags=["结算"],
        summary="发布逐小时结算（连续 1～48 小时）",
        responses={
            409: {
                "description": "存在缺失读数或超限用量，未发布任何结算",
                "content": {"application/json": {"example": {
                    "code": "publish_rejected",
                    "message": "结算未发布：存在缺失读数或超限用量",
                    "atomic": True,
                    "meter_id": "M-001",
                    "start_hour": "2026-11-16T00:00:00Z",
                    "end_hour": "2026-11-16T02:00:00Z",
                    "hours": 2,
                    "problems": [{
                        "code": "missing_reading",
                        "start_hour": "2026-11-16T01:00:00Z",
                        "end_hour": "2026-11-16T02:00:00Z",
                        "message": "缺少 2026-11-16T01:00:00Z 至 2026-11-16T02:00:00Z 区间内的小时读数",
                        "missing_readings": ["2026-11-16T01:00:00Z"],
                    }],
                }}},
            },
        },
    )
    def post_settlement(
        meter_id: MeterId, payload: SettlementCreate, conn: Conn
    ) -> SettlementDetail:
        return publish_settlement(
            conn, meter_id, payload.start_hour, payload.hours
        )

    @app.get(
        "/meters/{meter_id}/settlements",
        response_model=SettlementList,
        tags=["结算"],
        summary="按电表列结算摘要",
    )
    def get_meter_settlements(
        meter_id: MeterId,
        conn: Conn,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> SettlementList:
        items = list_settlements(conn, meter_id, limit)
        return SettlementList(items=items, count=len(items))

    @app.get(
        "/settlements",
        response_model=SettlementList,
        tags=["结算"],
        summary="列出全部结算摘要",
    )
    def get_settlements(
        conn: Conn,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> SettlementList:
        items = list_settlements(conn, None, limit)
        return SettlementList(items=items, count=len(items))

    @app.get(
        "/settlements/{settlement_no}",
        response_model=SettlementDetail,
        tags=["结算"],
        summary="查询结算明细（历史快照，不受后续更正影响）",
    )
    def get_settlement_detail(settlement_no: str, conn: Conn) -> SettlementDetail:
        return get_settlement(conn, settlement_no)

    return app


app = create_app()
