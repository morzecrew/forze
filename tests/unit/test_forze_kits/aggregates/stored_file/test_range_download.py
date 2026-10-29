"""Unit tests for the stored-file ranged download and its FastAPI route."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fastapi import APIRouter, FastAPI
from pydantic import ValidationError

from forze.application.contracts.storage import RANGE_NOT_SATISFIABLE_CODE, StorageQueryPort
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.hooks.authn import AuthnRequired
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_stored_file_download_route
from forze_kits.aggregates.stored_file import (
    DownloadStoredFileRange,
    DownloadStoredFileRangeRequestDTO,
    SoftDeleteStoredFile,
    StoredFileFacade,
    StoredFileIdRevDTO,
    StoredFileKernelOp,
    UploadStoredFile,
    UploadStoredFileRequestDTO,
    bind_stored_file_writes,
    build_stored_file_registry,
    freeze_stored_file_registry,
)
from forze_kits.domain.stored_file import (
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)
from forze_mock import MockDepsModule, MockRouteConfig, MockState

# ----------------------- #

_KIT = StoredFileKitSpec(name="files", stream_downloads=True, range_downloads=True)
_NS = _KIT.document.default_namespace
_KEY = _NS.key
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")

_RANGE = _KEY(StoredFileKernelOp.DOWNLOAD_RANGE)
_DATA = bytes(range(256)) * 4  # 1 KiB


async def _ready(ctx: ExecutionContext, data: bytes = _DATA) -> StoredFileRead:
    created = await run_operation(
        _REG,
        _KEY(StoredFileKernelOp.UPLOAD),
        UploadStoredFileRequestDTO(filename="report.bin", data=data),
        ctx,
    )
    return await ctx.doc.query(_KIT.document).get(created.id)


async def _not_ready(ctx: ExecutionContext, status: StoredFileStatus) -> StoredFileRead:
    doc = ctx.doc.command(_KIT.document)
    pending = await UploadStoredFile(doc=doc)(
        UploadStoredFileRequestDTO(filename="report.bin", data=b"hello")
    )

    if status is StoredFileStatus.FAILED:
        return await doc.update(
            pk=pending.id, rev=pending.rev, dto=StoredFileUpdateCmd(status=status)
        )

    if status is StoredFileStatus.DELETED:
        return await SoftDeleteStoredFile(doc=doc)(
            StoredFileIdRevDTO(id=pending.id, rev=pending.rev)
        )

    return pending


def _args(file_id, start: int = 0, end: int | None = None) -> DownloadStoredFileRangeRequestDTO:
    return DownloadStoredFileRangeRequestDTO(id=file_id, start=start, end=end)


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(state: MockState) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state))


# ....................... #


class TestDownloadRangeOptIn:
    def test_it_is_not_registered_by_default(self) -> None:
        catalog = build_stored_file_registry(StoredFileKitSpec(name="files")).freeze().catalog()

        assert _RANGE not in catalog

    def test_the_flag_registers_only_the_range(self) -> None:
        kit = StoredFileKitSpec(name="files", range_downloads=True)
        catalog = build_stored_file_registry(kit).freeze().catalog()

        assert _RANGE in catalog
        assert _KEY(StoredFileKernelOp.DOWNLOAD_STREAM) not in catalog
        assert catalog[_RANGE].is_read_only

    def test_the_facade_refuses_it_when_left_off(self, ctx: ExecutionContext) -> None:
        kit = StoredFileKitSpec(name="files")
        facade = StoredFileFacade(
            ctx=ctx, registry=build_stored_file_registry(kit).freeze(), namespace=_NS
        )

        with pytest.raises(CoreException, match="range_downloads") as ei:
            _ = facade.download_range

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestDownloadStoredFileRange:
    async def test_a_ready_file_returns_the_window(self, ctx: ExecutionContext) -> None:
        file = await _ready(ctx)

        result = await run_operation(_REG, _RANGE, _args(file.id, 10, 19), ctx)

        assert result.file.id == file.id
        assert result.data == _DATA[10:20]
        assert result.content_range == f"bytes 10-19/{len(_DATA)}"
        assert result.total_size == len(_DATA)

    async def test_an_open_end_reads_to_the_end(self, ctx: ExecutionContext) -> None:
        file = await _ready(ctx)

        result = await run_operation(_REG, _RANGE, _args(file.id, 1000), ctx)

        assert result.data == _DATA[1000:]

    async def test_a_start_past_the_end_is_unsatisfiable(self, ctx: ExecutionContext) -> None:
        file = await _ready(ctx)

        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _RANGE, _args(file.id, len(_DATA)), ctx)

        assert ei.value.code == RANGE_NOT_SATISFIABLE_CODE

    def test_offsets_are_not_negative(self) -> None:
        with pytest.raises(ValidationError):
            _args(uuid4(), -1)

        with pytest.raises(ValidationError):
            _args(uuid4(), 0, -1)

    @pytest.mark.parametrize(
        ("status", "kind"),
        [
            (StoredFileStatus.PENDING, ExceptionKind.PRECONDITION),
            (StoredFileStatus.FAILED, ExceptionKind.PRECONDITION),
            (StoredFileStatus.DELETED, ExceptionKind.NOT_FOUND),
        ],
    )
    async def test_no_range_is_read_for_a_file_that_is_not_ready(
        self, ctx: ExecutionContext, status: StoredFileStatus, kind: ExceptionKind
    ) -> None:
        file = await _not_ready(ctx, status)
        storage = MagicMock(spec=StorageQueryPort)
        storage.download_range = AsyncMock()
        handler = DownloadStoredFileRange(doc=ctx.doc.query(_KIT.document), storage=storage)

        with pytest.raises(CoreException) as ei:
            await handler(_args(file.id))

        assert ei.value.kind is kind
        storage.download_range.assert_not_awaited()

    async def test_an_unknown_file_is_not_found(self, ctx: ExecutionContext) -> None:
        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _RANGE, _args(uuid4()), ctx)

        assert ei.value.kind is ExceptionKind.NOT_FOUND

    async def test_another_tenants_file_is_not_found(self, state: MockState) -> None:
        ctx = context_from_modules(
            MockDepsModule(state=state, routes={"files": MockRouteConfig(tenant_aware=True)})
        )

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            file = await _ready(ctx)

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            with pytest.raises(CoreException) as ei:
                await run_operation(_REG, _RANGE, _args(file.id), ctx)

        assert ei.value.kind is ExceptionKind.NOT_FOUND

    async def test_a_guard_bound_on_the_range_runs_first(self, ctx: ExecutionContext) -> None:
        guarded = (
            bind_stored_file_writes(_KIT, tx_route="mock")
            .bind(_RANGE)
            .bind_outer()
            .before(AuthnRequired().to_step())
            .finish(deep=True)
            .freeze()
        )
        file = await _ready(ctx)

        with pytest.raises(CoreException) as ei:
            await run_operation(guarded, _RANGE, _args(file.id), ctx)

        assert ei.value.kind is ExceptionKind.AUTHENTICATION


# ....................... #


class TestStoredFileDownloadRoute:
    @pytest.fixture
    def client(self, ctx: ExecutionContext) -> httpx.AsyncClient:
        router = APIRouter(prefix="/files")
        attach_stored_file_download_route(
            router, registry=_REG, ns=_NS, ctx_dep=lambda: ctx, max_range_bytes=100
        )
        app = FastAPI()
        app.include_router(router)
        register_exception_handlers(app)

        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

    async def test_a_plain_get_streams_the_whole_file(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content")

        assert resp.status_code == 200
        assert resp.content == _DATA
        assert resp.headers["content-disposition"] == 'attachment; filename="report.bin"'
        assert resp.headers["accept-ranges"] == "bytes"
        assert resp.headers["etag"]

    async def test_a_range_answers_206_under_the_stored_filename(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content", headers={"Range": "bytes=10-19"})

        assert resp.status_code == 206
        assert resp.content == _DATA[10:20]
        assert resp.headers["content-range"] == f"bytes 10-19/{len(_DATA)}"
        assert resp.headers["content-disposition"] == 'attachment; filename="report.bin"'
        assert resp.headers["etag"]

    async def test_a_suffix_range_reads_the_tail(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content", headers={"Range": "bytes=-24"})

        assert resp.status_code == 206
        assert resp.content == _DATA[-24:]

    async def test_a_wide_range_is_capped(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content", headers={"Range": "bytes=0-"})

        assert resp.status_code == 206
        assert resp.content == _DATA[:100]
        assert resp.headers["content-range"] == f"bytes 0-99/{len(_DATA)}"

    async def test_a_range_past_the_end_answers_416(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content", headers={"Range": "bytes=5000-"})

        assert resp.status_code == 416
        assert resp.headers["content-range"] == f"bytes */{len(_DATA)}"

    async def test_a_matching_etag_answers_304(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)
        etag = (await client.get(f"/files/{file.id}/content")).headers["etag"]

        resp = await client.get(f"/files/{file.id}/content", headers={"If-None-Match": etag})

        assert resp.status_code == 304
        assert resp.content == b""
        assert resp.headers["etag"] == etag

    async def test_a_wildcard_answers_304(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content", headers={"If-None-Match": "*"})

        assert resp.status_code == 304

    async def test_a_stale_etag_gets_the_body(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _ready(ctx)

        resp = await client.get(f"/files/{file.id}/content", headers={"If-None-Match": '"old"'})

        assert resp.status_code == 200
        assert resp.content == _DATA

    async def test_a_pending_file_is_refused(
        self, ctx: ExecutionContext, client: httpx.AsyncClient
    ) -> None:
        file = await _not_ready(ctx, StoredFileStatus.PENDING)

        resp = await client.get(f"/files/{file.id}/content", headers={"Range": "bytes=0-1"})

        assert resp.status_code == 400  # precondition

    async def test_an_unknown_file_is_not_found(self, client: httpx.AsyncClient) -> None:
        resp = await client.get(f"/files/{uuid4()}/content")

        assert resp.status_code == 404

    def test_the_route_needs_both_operations(self) -> None:
        registry = freeze_stored_file_registry(
            StoredFileKitSpec(name="files", stream_downloads=True), tx_route="mock"
        )

        with pytest.raises(CoreException, match="range_downloads") as ei:
            attach_stored_file_download_route(
                APIRouter(), registry=registry, ns=_NS, ctx_dep=lambda: None
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize(("path", "max_range_bytes"), [("/content", 100), ("/{id}/content", 0)])
    def test_a_bad_path_or_cap_is_refused(self, path: str, max_range_bytes: int) -> None:
        with pytest.raises(CoreException) as ei:
            attach_stored_file_download_route(
                APIRouter(),
                registry=_REG,
                ns=_NS,
                ctx_dep=lambda: None,
                path=path,
                max_range_bytes=max_range_bytes,
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_the_route_is_named_after_the_stream_operation(self) -> None:
        router = attach_stored_file_download_route(
            APIRouter(), registry=_REG, ns=_NS, ctx_dep=lambda: None
        )

        assert [route.operation_id for route in router.routes] == [  # type: ignore[attr-defined]
            _KEY(StoredFileKernelOp.DOWNLOAD_STREAM)
        ]


# ....................... #


class TestRangeExposure:
    """``download_range`` returns raw bytes, so JSON surfaces leave it out."""

    _REG = freeze_stored_file_registry(
        StoredFileKitSpec(name="files", range_downloads=True), tx_route="mock"
    )

    def test_the_range_descriptor_streams(self) -> None:
        assert self._REG.catalog()[_RANGE].descriptor.streams is True

    def test_mcp_leaves_the_range_out_by_default(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        names = register_tools(fastmcp.FastMCP("files"), self._REG, lambda: None)

        assert _RANGE not in names
        assert _KEY(StoredFileKernelOp.GET) in names

    def test_forze_ops_leaves_the_range_out_by_default(self) -> None:
        from forze_cli.ops import AnonymousIdentity, OpsApp

        ops = OpsApp(registry=self._REG, runtime=lambda: None, identity=AnonymousIdentity())

        assert _RANGE not in ops.exposed
