"""Integration tests: multipart form with File() + Form() on attach_http_endpoint."""

from typing import Any

import attrs
from fastapi import APIRouter, FastAPI, UploadFile
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.execution import (
    Deps,
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
    UsecasesFacade,
    facade_op,
)
from forze_fastapi.endpoints.http import (
    HttpRequestDTO,
    HttpRequestSpec,
    attach_http_endpoint,
    build_http_endpoint_spec,
)

# ----------------------- #

UP_OP = "multipart.upload"
BATCH_OP = "multipart.batch"

# ....................... #


class UploadBody(BaseModel):
    file: UploadFile
    note: str = ""


class UploadIn(BaseModel):
    name: str
    size: int
    note: str


class UploadOut(BaseModel):
    name: str
    size: int
    note: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadUsecase(Usecase[UploadIn, UploadOut]):
    async def main(self, args: UploadIn) -> UploadOut:
        return UploadOut(
            name=args.name,
            size=args.size,
            note=args.note,
        )


# ....................... #


class BatchBody(BaseModel):
    files: list[UploadFile]


class BatchIn(BaseModel):
    count: int
    total_size: int


class BatchOut(BaseModel):
    count: int
    total_size: int


@attrs.define(slots=True, kw_only=True, frozen=True)
class BatchUsecase(Usecase[BatchIn, BatchOut]):
    async def main(self, args: BatchIn) -> BatchOut:
        return BatchOut(count=args.count, total_size=args.total_size)


# ....................... #


def _reg() -> UsecaseRegistry:
    reg = UsecaseRegistry(
        defaults={
            UP_OP: lambda ctx: UploadUsecase(ctx=ctx),
            BATCH_OP: lambda ctx: BatchUsecase(ctx=ctx),
        }
    )
    reg.finalize("mp", inplace=True)
    return reg


async def _map_upload(
    dto: HttpRequestDTO[Any, Any, Any, Any, UploadBody],
    /,
    *,
    ctx: ExecutionContext | None = None,
) -> UploadIn:
    assert dto.body is not None
    b = dto.body
    raw = await b.file.read()
    return UploadIn(
        name=b.file.filename or "unnamed",
        size=len(raw),
        note=b.note,
    )


async def _map_batch(
    dto: HttpRequestDTO[Any, Any, Any, Any, BatchBody],
    /,
    *,
    ctx: ExecutionContext | None = None,
) -> BatchIn:
    assert dto.body is not None
    b = dto.body
    n = 0
    t = 0
    for uf in b.files:
        t += len(await uf.read())
        n += 1
    return BatchIn(count=n, total_size=t)


# ....................... #


class UploadFacade(UsecasesFacade):
    upload = facade_op(UP_OP, uc=UploadUsecase)
    batch = facade_op(BATCH_OP, uc=BatchUsecase)


# ----------------------- #


class TestMultipartHttpEndpoint:
    def test_file_and_text_form_fields(self) -> None:
        _request: HttpRequestSpec[Any, Any, Any, Any, UploadBody] = {
            "body_type": UploadBody,
            "body_mode": "form",
        }
        spec = build_http_endpoint_spec(
            UploadFacade,
            UploadFacade.upload,
            http={"method": "POST", "path": "/upload"},
            request=_request,
            response=UploadOut,
            mapper=_map_upload,
        )
        reg = _reg()
        app = FastAPI()
        r = APIRouter()

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        attach_http_endpoint(
            r,
            spec=spec,
            registry=reg,
            ctx_dep=ctx_dep,
        )
        app.include_router(r)
        client = TestClient(app)

        res = client.post(
            "/upload",
            data={"note": "hi"},
            files={"file": ("f.txt", b"abc", "text/plain")},
        )
        assert res.status_code == 200
        assert res.json() == {"name": "f.txt", "size": 3, "note": "hi"}

    def test_list_upload_file_fields(self) -> None:
        _request: HttpRequestSpec[Any, Any, Any, Any, BatchBody] = {
            "body_type": BatchBody,
            "body_mode": "form",
        }
        spec = build_http_endpoint_spec(
            UploadFacade,
            UploadFacade.batch,
            http={"method": "POST", "path": "/batch"},
            request=_request,
            response=BatchOut,
            mapper=_map_batch,
        )
        reg = _reg()
        app = FastAPI()
        r = APIRouter()

        def ctx_dep() -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        attach_http_endpoint(
            r,
            spec=spec,
            registry=reg,
            ctx_dep=ctx_dep,
        )
        app.include_router(r)
        client = TestClient(app)

        res = client.post(
            "/batch",
            files=[
                ("files", ("a.txt", b"xx", "text/plain")),
                ("files", ("b.txt", b"yy", "text/plain")),
            ],
        )
        assert res.status_code == 200
        assert res.json() == {"count": 2, "total_size": 4}
