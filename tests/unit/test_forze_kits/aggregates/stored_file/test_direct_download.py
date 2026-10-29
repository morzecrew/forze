"""Unit tests for the stored-file streaming and presigned downloads."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError
from pydantic_core import PydanticSerializationError

from forze.application.contracts.storage import StorageQueryPort, StreamedDownload
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.hooks.authn import AuthnRequired
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_fastapi.routes import streamed_download_response
from forze_kits.aggregates.stored_file import (
    DownloadStoredFileStream,
    PresignStoredFileDownload,
    PresignStoredFileDownloadRequestDTO,
    SoftDeleteStoredFile,
    StoredFileFacade,
    StoredFileIdDTO,
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
from forze_kits.integrations.agent_tools import operation_tools
from forze_mock import MockDepsModule, MockRouteConfig, MockState

# ----------------------- #

# Both download paths are opt-in; these tests run against a kit that enables them.
_KIT = StoredFileKitSpec(name="files", stream_downloads=True, presign_downloads=True)
_KEY = _KIT.document.default_namespace.key
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")

_STREAM = _KEY(StoredFileKernelOp.DOWNLOAD_STREAM)
_PRESIGN = _KEY(StoredFileKernelOp.PRESIGN_DOWNLOAD)


def _presign_args(file_id: UUID) -> PresignStoredFileDownloadRequestDTO:
    return PresignStoredFileDownloadRequestDTO(id=file_id, expires_in=timedelta(minutes=5))


async def _ready(ctx: ExecutionContext, data: bytes = b"hello") -> StoredFileRead:
    """Upload through the registry, so the after-commit stage stores the blob."""

    created = await run_operation(
        _REG,
        _KEY(StoredFileKernelOp.UPLOAD),
        UploadStoredFileRequestDTO(filename="report.txt", data=data),
        ctx,
    )
    return await ctx.doc.query(_KIT.document).get(created.id)


async def _not_ready(ctx: ExecutionContext, status: StoredFileStatus) -> StoredFileRead:
    """A row in *status* (pending, failed or deleted) — no blob behind it."""

    doc = ctx.doc.command(_KIT.document)
    pending = await UploadStoredFile(doc=doc)(
        UploadStoredFileRequestDTO(filename="report.txt", data=b"hello")
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


_NOT_READY = [
    (StoredFileStatus.PENDING, ExceptionKind.PRECONDITION),
    (StoredFileStatus.FAILED, ExceptionKind.PRECONDITION),
    (StoredFileStatus.DELETED, ExceptionKind.NOT_FOUND),
]


def _spy_storage() -> MagicMock:
    """A storage port whose download paths record whether they ran."""

    storage = MagicMock(spec=StorageQueryPort)
    storage.download_stream = AsyncMock()
    storage.presign_download = AsyncMock()
    return storage


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(state: MockState) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state))


# ....................... #


class TestDirectDownloadOptIn:
    def _ops(self, kit: StoredFileKitSpec) -> set[str]:
        catalog = build_stored_file_registry(kit).freeze().catalog()
        return {op for op in (_STREAM, _PRESIGN) if op in catalog}

    def test_neither_is_registered_by_default(self) -> None:
        assert self._ops(StoredFileKitSpec(name="files")) == set()

    @pytest.mark.parametrize(
        ("flags", "ops"),
        [
            ({"stream_downloads": True}, {_STREAM}),
            ({"presign_downloads": True}, {_PRESIGN}),
            ({"stream_downloads": True, "presign_downloads": True}, {_STREAM, _PRESIGN}),
        ],
        ids=["stream", "presign", "both"],
    )
    def test_each_flag_registers_only_its_operation(self, flags: dict, ops: set[str]) -> None:
        assert self._ops(StoredFileKitSpec(name="files", **flags)) == ops

    def test_the_facade_resolves_an_enabled_operation(self, ctx: ExecutionContext) -> None:
        facade = StoredFileFacade(ctx=ctx, registry=_REG, namespace=_KIT.document.default_namespace)

        assert facade.download_stream is not None
        assert facade.presign_download is not None

    @pytest.mark.parametrize("attr", ["download_stream", "presign_download"])
    def test_the_facade_refuses_an_operation_left_off(
        self, ctx: ExecutionContext, attr: str
    ) -> None:
        kit = StoredFileKitSpec(name="files")
        facade = StoredFileFacade(
            ctx=ctx,
            registry=build_stored_file_registry(kit).freeze(),
            namespace=kit.document.default_namespace,
        )

        with pytest.raises(CoreException) as ei:
            getattr(facade, attr)

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestDownloadStoredFileStream:
    async def test_a_ready_file_streams_its_bytes(self, ctx: ExecutionContext) -> None:
        file = await _ready(ctx, data=b"x" * 200_000)

        result = await run_operation(_REG, _STREAM, StoredFileIdDTO(id=file.id), ctx)

        assert result.file.id == file.id
        assert result.stream.size == 200_000
        assert result.stream.etag
        assert b"".join([chunk async for chunk in result.stream.chunks]) == b"x" * 200_000

    async def test_the_stream_is_named_after_the_stored_file(self, ctx: ExecutionContext) -> None:
        file = await _ready(ctx)

        async def _chunks():
            yield b"hello"

        storage = _spy_storage()
        storage.download_stream.return_value = StreamedDownload(
            content_type="text/plain",
            filename="0193f2.bin",  # the backend key's basename
            chunks=_chunks(),
        )
        handler = DownloadStoredFileStream(doc=ctx.doc.query(_KIT.document), storage=storage)

        result = await handler(StoredFileIdDTO(id=file.id))

        storage.download_stream.assert_awaited_once_with(file.storage_key)
        assert result.stream.filename == "report.txt"

    @pytest.mark.parametrize(("status", "kind"), _NOT_READY)
    async def test_no_stream_opens_for_a_file_that_is_not_ready(
        self,
        ctx: ExecutionContext,
        status: StoredFileStatus,
        kind: ExceptionKind,
    ) -> None:
        file = await _not_ready(ctx, status)
        storage = _spy_storage()
        handler = DownloadStoredFileStream(doc=ctx.doc.query(_KIT.document), storage=storage)

        with pytest.raises(CoreException) as ei:
            await handler(StoredFileIdDTO(id=file.id))

        assert ei.value.kind is kind
        storage.download_stream.assert_not_awaited()


# ....................... #


class TestPresignStoredFileDownload:
    async def test_a_ready_file_gets_a_url_for_its_key(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        file = await _ready(ctx)

        result = await run_operation(_REG, _PRESIGN, _presign_args(file.id), ctx)

        assert result.file.filename == "report.txt"
        assert result.presigned.method == "GET"
        assert [(p["key"], p["method"]) for p in state.storage_presigns] == [
            (file.storage_key, "GET")
        ]
        # The URL saves the download under the stored filename, not the key's basename.
        assert state.storage_presigns[0]["content_disposition"] == (
            'attachment; filename="report.txt"'
        )

    @pytest.mark.parametrize(("status", "kind"), _NOT_READY)
    async def test_no_url_is_minted_for_a_file_that_is_not_ready(
        self,
        ctx: ExecutionContext,
        status: StoredFileStatus,
        kind: ExceptionKind,
    ) -> None:
        file = await _not_ready(ctx, status)
        storage = _spy_storage()
        handler = PresignStoredFileDownload(doc=ctx.doc.query(_KIT.document), storage=storage)

        with pytest.raises(CoreException) as ei:
            await handler(_presign_args(file.id))

        assert ei.value.kind is kind
        storage.presign_download.assert_not_awaited()

    async def test_no_url_is_minted_for_an_unknown_file(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _PRESIGN, _presign_args(uuid4()), ctx)

        assert ei.value.kind is ExceptionKind.NOT_FOUND
        assert state.storage_presigns == []

    @pytest.mark.parametrize(
        "expires_in",
        [timedelta(0), timedelta(seconds=-1), timedelta(days=7, seconds=1)],
        ids=["zero", "negative", "over-7-days"],
    )
    def test_the_expiry_is_bounded(self, expires_in: timedelta) -> None:
        with pytest.raises(ValidationError):
            PresignStoredFileDownloadRequestDTO(id=uuid4(), expires_in=expires_in)


# ....................... #


class TestDirectDownloadTenancy:
    @pytest.fixture
    def ctx(self, state: MockState) -> ExecutionContext:
        return context_from_modules(
            MockDepsModule(state=state, routes={"files": MockRouteConfig(tenant_aware=True)})
        )

    async def test_another_tenants_file_is_not_found(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            file = await _ready(ctx)

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            for op, args in (
                (_STREAM, StoredFileIdDTO(id=file.id)),
                (_PRESIGN, _presign_args(file.id)),
            ):
                with pytest.raises(CoreException) as ei:
                    await run_operation(_REG, op, args, ctx)

                assert ei.value.kind is ExceptionKind.NOT_FOUND

        assert state.storage_presigns == []

    async def test_a_row_naming_another_tenants_key_streams_nothing(
        self, ctx: ExecutionContext
    ) -> None:
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            theirs = await _ready(ctx, data=b"tenant-a-only")

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            # A row of this tenant pointing at the other tenant's object, as a corrupted or
            # forged row would: the read stays inside this tenant's container.
            pending = await _not_ready(ctx, StoredFileStatus.PENDING)
            forged = await ctx.doc.command(_KIT.document).update(
                pk=pending.id,
                rev=pending.rev,
                dto=StoredFileUpdateCmd(
                    storage_key=theirs.storage_key, status=StoredFileStatus.READY
                ),
            )

            with pytest.raises(CoreException) as ei:
                await run_operation(_REG, _STREAM, StoredFileIdDTO(id=forged.id), ctx)

        assert ei.value.kind is ExceptionKind.NOT_FOUND

    async def test_the_url_is_scoped_to_the_owning_tenant(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        tenant = uuid4()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            file = await _ready(ctx)
            await run_operation(_REG, _PRESIGN, _presign_args(file.id), ctx)

        # The mock partitions a tenant's container as "{tenant_id}/{bucket}".
        assert [p["bucket"] for p in state.storage_presigns] == [f"{tenant}/files"]

    async def test_no_bound_tenant_fails_closed(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            file = await _ready(ctx)

        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _PRESIGN, _presign_args(file.id), ctx)

        assert ei.value.code == "tenant_required"
        assert state.storage_presigns == []


# ....................... #


class TestDirectDownloadGuards:
    @pytest.mark.parametrize("op", [_STREAM, _PRESIGN], ids=["stream", "presign"])
    async def test_a_guard_bound_on_the_op_runs_before_the_handler(
        self, ctx: ExecutionContext, state: MockState, op: str
    ) -> None:
        guarded = (
            bind_stored_file_writes(_KIT, tx_route="mock")
            .bind(op)
            .bind_outer()
            .before(AuthnRequired().to_step())
            .finish(deep=True)
            .freeze()
        )
        file = await _ready(ctx)
        args = StoredFileIdDTO(id=file.id) if op == _STREAM else _presign_args(file.id)

        with pytest.raises(CoreException) as ei:
            await run_operation(guarded, op, args, ctx)

        assert ei.value.kind is ExceptionKind.AUTHENTICATION
        assert state.storage_presigns == []


# ....................... #


class TestPresignExposure:
    """A presigned URL is a bearer credential, so generated surfaces refuse the operation."""

    def test_the_presign_descriptor_is_sensitive_and_the_stream_is_not(self) -> None:
        catalog = _REG.catalog()

        assert catalog[_PRESIGN].descriptor.sensitive is True
        assert catalog[_STREAM].descriptor.sensitive is False

    def test_mcp_refuses_a_registry_exposing_presign_by_default(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        with pytest.raises(CoreException, match="sensitive") as ei:
            register_tools(fastmcp.FastMCP("files"), _REG, lambda: None)

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_mcp_refuses_presign_named_in_an_allowlist(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        with pytest.raises(CoreException, match="sensitive"):
            register_tools(fastmcp.FastMCP("files"), _REG, lambda: None, operations=[_PRESIGN])

    def test_an_mcp_allowlist_without_presign_registers(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        names = register_tools(
            fastmcp.FastMCP("files"),
            _REG,
            lambda: None,
            operations=[_KEY(StoredFileKernelOp.GET), _KEY(StoredFileKernelOp.LIST)],
        )

        assert _PRESIGN not in names
        assert len(names) == 2

    def test_agent_tools_refuse_presign(self) -> None:
        with pytest.raises(CoreException) as ei:
            operation_tools(_REG, include=[_KEY(StoredFileKernelOp.GET), _PRESIGN])

        assert ei.value.code == "agent_tools_sensitive_operation"

    def test_agent_tools_without_presign_build(self) -> None:
        toolset = operation_tools(_REG, include=[_KEY(StoredFileKernelOp.GET)])

        assert [str(entry.op) for entry in toolset.bindings.values()] == [
            _KEY(StoredFileKernelOp.GET)
        ]


# ....................... #


class TestStreamExposure:
    """``download_stream`` returns a byte stream and ``download`` raw bytes, which no JSON
    surface can carry: a binary file's bytes are not UTF-8, so JSON encoding refuses them."""

    # Stream only: the sensitive presign would refuse every default projection on its own.
    _REG = freeze_stored_file_registry(
        StoredFileKitSpec(name="files", stream_downloads=True), tx_route="mock"
    )
    _DOWNLOAD = _KEY(StoredFileKernelOp.DOWNLOAD)
    _JSON_OPS = [_KEY(StoredFileKernelOp.GET), _KEY(StoredFileKernelOp.LIST)]

    def test_both_download_descriptors_stream(self) -> None:
        catalog = self._REG.catalog()

        assert catalog[_STREAM].descriptor.streams is True
        assert catalog[self._DOWNLOAD].descriptor.streams is True

    async def test_a_binary_download_is_not_json(self, ctx: ExecutionContext) -> None:
        ready = await _ready(ctx, data=b"\x89PNG\r\n\x1a\n\xff")
        downloaded = await run_operation(_REG, self._DOWNLOAD, StoredFileIdDTO(id=ready.id), ctx)

        with pytest.raises(PydanticSerializationError):
            downloaded.model_dump_json()

    def test_mcp_leaves_the_downloads_out_by_default(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        names = register_tools(fastmcp.FastMCP("files"), self._REG, lambda: None)

        assert not {_STREAM, self._DOWNLOAD} & set(names)
        assert set(self._JSON_OPS) <= set(names)

    def test_mcp_refuses_the_buffered_download_named_in_an_allowlist(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        with pytest.raises(CoreException, match="byte stream"):
            register_tools(
                fastmcp.FastMCP("files"), self._REG, lambda: None, operations=[self._DOWNLOAD]
            )

    def test_mcp_refuses_the_stream_named_in_an_allowlist(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        with pytest.raises(CoreException, match="byte stream") as ei:
            register_tools(
                fastmcp.FastMCP("files"), self._REG, lambda: None, operations=[_STREAM]
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_mcp_refuses_the_stream_as_a_resource_template(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp import ResourceTemplateSpec, register_resource_templates

        server = fastmcp.FastMCP("files")

        with pytest.raises(CoreException, match="byte stream") as ei:
            register_resource_templates(
                server, self._REG, lambda: None, [ResourceTemplateSpec(op=_STREAM, scheme="f")]
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_forze_ops_leaves_the_stream_out_by_default(self) -> None:
        from forze_cli.ops import AnonymousIdentity, OpsApp

        ops = OpsApp(registry=self._REG, runtime=lambda: None, identity=AnonymousIdentity())

        assert not {_STREAM, self._DOWNLOAD} & set(ops.exposed)
        assert set(self._JSON_OPS) <= set(ops.exposed)

    def test_forze_ops_refuses_the_stream_named_in_an_allowlist(self) -> None:
        from forze_cli.ops import AnonymousIdentity, OpsApp

        with pytest.raises(CoreException, match="byte stream") as ei:
            OpsApp(
                registry=self._REG,
                runtime=lambda: None,
                identity=AnonymousIdentity(),
                operations=[_STREAM],
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_agent_tools_refuse_the_stream(self) -> None:
        with pytest.raises(CoreException) as ei:
            operation_tools(self._REG, include=[_KEY(StoredFileKernelOp.GET), _STREAM])

        assert ei.value.code == "agent_tools_streaming_operation"

    def test_agent_tools_ops_and_generated_routes_refuse_the_buffered_download(self) -> None:
        from fastapi import APIRouter

        from forze_cli.ops import AnonymousIdentity, OpsApp
        from forze_fastapi.routes import RouteBinding, attach_operation_routes, id_endpoint

        with pytest.raises(CoreException) as tools:
            operation_tools(self._REG, include=[self._DOWNLOAD])

        with pytest.raises(CoreException, match="byte stream"):
            OpsApp(
                registry=self._REG,
                runtime=lambda: None,
                identity=AnonymousIdentity(),
                operations=[self._DOWNLOAD],
            )

        with pytest.raises(CoreException, match="byte stream"):
            attach_operation_routes(
                APIRouter(),
                registry=self._REG,
                ns=_KIT.document.default_namespace,
                ctx_dep=lambda: None,
                bindings={
                    StoredFileKernelOp.DOWNLOAD: RouteBinding(
                        method="GET", path="/{id}/bytes", build=id_endpoint
                    )
                },
            )

        assert tools.value.code == "agent_tools_streaming_operation"

    def test_agent_tools_project_the_json_reads(self) -> None:
        toolset = operation_tools(self._REG, include=self._JSON_OPS)

        assert [str(entry.op) for entry in toolset.bindings.values()] == self._JSON_OPS

    def test_generated_routes_refuse_the_stream(self) -> None:
        from fastapi import APIRouter

        from forze_fastapi.routes import RouteBinding, attach_operation_routes, id_endpoint

        with pytest.raises(CoreException, match="byte stream") as ei:
            attach_operation_routes(
                APIRouter(),
                registry=self._REG,
                ns=_KIT.document.default_namespace,
                ctx_dep=lambda: None,
                bindings={
                    StoredFileKernelOp.DOWNLOAD_STREAM: RouteBinding(
                        method="GET", path="/{id}/content", build=id_endpoint
                    )
                },
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestStreamedDownloadResponse:
    async def test_a_hand_written_route_streams_under_the_stored_filename(
        self, ctx: ExecutionContext
    ) -> None:
        file = await _ready(ctx, data=b"hello")
        app = FastAPI()

        @app.get("/files/{file_id}/content")
        async def content(file_id: UUID):
            result = await run_operation(_REG, _STREAM, StoredFileIdDTO(id=file_id), ctx)
            return streamed_download_response(result.stream)

        transport = httpx.ASGITransport(app=app)

        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/files/{file.id}/content")

        assert resp.status_code == 200
        assert resp.content == b"hello"
        assert resp.headers["content-disposition"] == 'attachment; filename="report.txt"'
        assert resp.headers["content-length"] == "5"
        assert resp.headers["etag"]
        assert resp.headers["last-modified"]
