"""Unit tests for stored-file direct uploads (presigned PUT and multipart) on the mock."""

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.storage import StorageCommandPort, UploadPart, UploadSession
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.serialization import PydanticModelCodec
from forze.testing import context_from_modules
from forze_kits.aggregates.storage.dto import UploadPartDTO
from forze_kits.aggregates.stored_file import (
    CompleteStoredFileUpload,
    CompleteStoredFileUploadRequestDTO,
    InitiateStoredFileUpload,
    InitiateStoredFileUploadRequestDTO,
    PresignStoredFileUploadPartRequestDTO,
    StoredFileFacade,
    StoredFileIdDTO,
    StoredFileKernelOp,
    UploadStoredFile,
    UploadStoredFileRequestDTO,
    build_stored_file_registry,
    freeze_stored_file_registry,
)
from forze_kits.domain.stored_file import (
    StoredFileEventType,
    StoredFileKitSpec,
    StoredFileOutboxPayload,
    StoredFileRead,
    StoredFileStatus,
)
from forze_mock import MockDepsModule, MockRouteConfig, MockState

# ----------------------- #

_KIT = StoredFileKitSpec(name="files", direct_uploads=True, max_bytes=1000)
_NS = _KIT.document.default_namespace
_KEY = _NS.key
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")

_INITIATE = _KEY(StoredFileKernelOp.INITIATE_UPLOAD)
_PART = _KEY(StoredFileKernelOp.PRESIGN_UPLOAD_PART)
_COMPLETE = _KEY(StoredFileKernelOp.COMPLETE_UPLOAD)
_DIRECT = (_INITIATE, _PART, _COMPLETE)


def _initiate_args(
    size: int = 5, content_type: str = "text/plain", **kw: Any
) -> InitiateStoredFileUploadRequestDTO:
    if not kw.get("multipart"):
        kw.setdefault("expires_in", timedelta(minutes=5))

    return InitiateStoredFileUploadRequestDTO(
        filename="notes.txt", size=size, content_type=content_type, **kw
    )


async def _put(ctx: ExecutionContext, key: str, data: bytes, content_type: str) -> None:
    """What the client's PUT to the presigned URL does to the store."""

    async def _chunks():
        yield data

    storage = ctx.storage.command(_KIT.resolved_storage)
    await storage.overwrite_stream(key, _chunks(), content_type=content_type)


async def _row(ctx: ExecutionContext, file_id: UUID) -> StoredFileRead:
    return await ctx.doc.query(_KIT.document).get(file_id)


async def _object_exists(ctx: ExecutionContext, key: str) -> bool:
    try:
        await ctx.storage.query(_KIT.resolved_storage).head(key)
    except CoreException as e:
        if e.kind is ExceptionKind.NOT_FOUND:
            return False
        raise
    return True


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(state: MockState) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state))


# ....................... #


class TestDirectUploadOptIn:
    def test_none_is_registered_by_default(self) -> None:
        catalog = build_stored_file_registry(StoredFileKitSpec(name="files")).freeze().catalog()

        assert not set(_DIRECT) & set(catalog)

    def test_the_flag_registers_three_commands(self) -> None:
        catalog = _REG.catalog()

        assert set(_DIRECT) <= set(catalog)
        assert not any(catalog[op].is_read_only for op in _DIRECT)

    def test_the_presign_results_are_sensitive_and_complete_is_not(self) -> None:
        catalog = _REG.catalog()

        assert catalog[_INITIATE].descriptor.sensitive is True
        assert catalog[_PART].descriptor.sensitive is True
        assert catalog[_COMPLETE].descriptor.sensitive is False

    @pytest.mark.parametrize("attr", ["initiate_upload", "presign_upload_part", "complete_upload"])
    def test_the_facade_refuses_them_when_left_off(self, ctx: ExecutionContext, attr: str) -> None:
        kit = StoredFileKitSpec(name="files")
        facade = StoredFileFacade(
            ctx=ctx, registry=build_stored_file_registry(kit).freeze(), namespace=_NS
        )

        with pytest.raises(CoreException, match="direct_uploads") as ei:
            getattr(facade, attr)

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestInitiateUpload:
    async def test_a_single_put_gets_a_url_for_a_key_the_kit_minted(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        result = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)

        file = result.file
        assert file.status is StoredFileStatus.PENDING
        assert file.storage_key == str(file.id)
        assert (file.size, file.content_type) == (5, "text/plain")
        assert result.upload_id is None
        assert result.presigned is not None
        assert result.presigned.method == "PUT"
        assert result.presigned.headers == {"Content-Type": "text/plain", "If-None-Match": "*"}
        assert [(p["key"], p["method"], p["content_type"]) for p in state.storage_presigns] == [
            (file.storage_key, "PUT", "text/plain")
        ]

    async def test_a_tenants_key_carries_the_tenant_segment(self, state: MockState) -> None:
        ctx = context_from_modules(
            MockDepsModule(state=state, routes={"files": MockRouteConfig(tenant_aware=True)})
        )
        tenant = uuid4()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            result = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)

        assert result.file.storage_key == f"tenant_{tenant}/{result.file.id}"

    async def test_no_bound_tenant_fails_closed_before_a_row(self, state: MockState) -> None:
        ctx = context_from_modules(
            MockDepsModule(state=state, routes={"files": MockRouteConfig(tenant_aware=True)})
        )

        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _INITIATE, _initiate_args(), ctx)

        assert ei.value.code == "tenant_required"
        assert state.storage_presigns == []

    async def test_a_declared_size_over_the_cap_is_refused_before_anything_is_minted(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _INITIATE, _initiate_args(size=1001), ctx)

        assert ei.value.code == "upload_too_large"
        assert state.storage_presigns == []
        assert await ctx.doc.query(_KIT.document).count() == 0

    async def test_a_storage_refusal_leaves_no_row(self, ctx: ExecutionContext) -> None:
        # What a client-side-encrypting adapter does: it refuses before signing anything.
        storage = MagicMock(spec=StorageCommandPort)
        storage.presign_upload = AsyncMock(side_effect=exc.precondition("encrypting route"))
        handler = InitiateStoredFileUpload(
            doc=ctx.doc.command(_KIT.document),
            storage=storage,
            uploads=ctx.storage.uploads(_KIT.resolved_storage),
            current_tenant=lambda: None,
        )

        with pytest.raises(CoreException) as ei:
            await handler(_initiate_args())

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert await ctx.doc.query(_KIT.document).count() == 0

    @pytest.mark.parametrize(
        "content_type",
        ["text/plain; charset=utf-8", "text/plain\r\nX-Injected: 1", "plain", ""],
    )
    def test_the_content_type_is_a_bare_media_type(self, content_type: str) -> None:
        with pytest.raises(ValidationError):
            _initiate_args(content_type=content_type)


# ....................... #


class TestCompleteSinglePutUpload:
    async def test_a_matching_object_makes_the_file_ready(self, ctx: ExecutionContext) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)
        key = started.file.storage_key
        await _put(ctx, key, b"hello", "text/plain")

        ready = await run_operation(
            _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
        )

        assert ready.status is StoredFileStatus.READY
        assert (ready.storage_key, ready.size, ready.content_type) == (key, 5, "text/plain")
        downloaded = await run_operation(
            _REG, _KEY(StoredFileKernelOp.DOWNLOAD), StoredFileIdDTO(id=ready.id), ctx
        )
        assert downloaded.data == b"hello"

    async def test_nothing_uploaded_yet_leaves_the_row_pending(self, ctx: ExecutionContext) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)

        with pytest.raises(CoreException) as ei:
            await run_operation(
                _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
            )

        assert ei.value.code == "upload_incomplete"
        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.PENDING

    @pytest.mark.parametrize(
        ("declared", "data", "content_type", "code"),
        [
            (5, b"hello!", "text/plain", "upload_mismatch"),
            (5, b"hello", "text/html", "upload_mismatch"),
            (5, b"x" * 1001, "text/plain", "upload_too_large"),
        ],
        ids=["size", "content-type", "over-the-cap"],
    )
    async def test_a_mismatch_deletes_the_object_and_fails_the_row(
        self,
        ctx: ExecutionContext,
        declared: int,
        data: bytes,
        content_type: str,
        code: str,
    ) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(size=declared), ctx)
        key = started.file.storage_key
        await _put(ctx, key, data, content_type)

        with pytest.raises(CoreException) as ei:
            await run_operation(
                _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
            )

        assert ei.value.code == code
        assert not await _object_exists(ctx, key)
        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED

    async def test_a_ready_file_cannot_complete_again(self, ctx: ExecutionContext) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)
        await _put(ctx, started.file.storage_key, b"hello", "text/plain")
        args = CompleteStoredFileUploadRequestDTO(id=started.file.id)
        await run_operation(_REG, _COMPLETE, args, ctx)

        with pytest.raises(CoreException) as ei:
            await run_operation(_REG, _COMPLETE, args, ctx)

        assert ei.value.kind is ExceptionKind.PRECONDITION

    async def test_a_buffered_uploads_pending_row_is_not_completable(
        self, ctx: ExecutionContext
    ) -> None:
        pending = await UploadStoredFile(doc=ctx.doc.command(_KIT.document))(
            UploadStoredFileRequestDTO(filename="a.txt", data=b"a")
        )

        with pytest.raises(CoreException) as ei:
            await run_operation(
                _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=pending.id), ctx
            )

        assert ei.value.kind is ExceptionKind.PRECONDITION

    async def test_another_tenants_upload_is_not_found(self, state: MockState) -> None:
        ctx = context_from_modules(
            MockDepsModule(state=state, routes={"files": MockRouteConfig(tenant_aware=True)})
        )

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            started = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)
            await _put(ctx, started.file.storage_key, b"hello", "text/plain")

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            with pytest.raises(CoreException) as ei:
                await run_operation(
                    _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
                )

        assert ei.value.kind is ExceptionKind.NOT_FOUND

    def test_parts_come_with_a_session_id(self) -> None:
        with pytest.raises(ValidationError):
            CompleteStoredFileUploadRequestDTO(id=uuid4(), upload_id="u")

        with pytest.raises(ValidationError):
            CompleteStoredFileUploadRequestDTO(id=uuid4(), parts=[UploadPartDTO(part_number=1)])


# ....................... #


class TestMultipartUpload:
    async def _deposit(
        self, ctx: ExecutionContext, key: str, upload_id: str, parts: list[bytes]
    ) -> list[UploadPartDTO]:
        """What the client's part PUTs do to the store (the mock's deposit seam)."""

        uploads = ctx.storage.uploads(_KIT.resolved_storage)
        session = UploadSession(key=key, upload_id=upload_id)
        landed: list[UploadPart] = [
            uploads.deposit_part(session, n, data)  # type: ignore[attr-defined]
            for n, data in enumerate(parts, start=1)
        ]
        return [UploadPartDTO(part_number=p.part_number, etag=p.etag) for p in landed]

    async def test_parts_assemble_into_a_ready_file(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(multipart=True), ctx)
        key = started.file.storage_key

        assert started.presigned is None
        assert started.upload_id

        url = await run_operation(
            _REG,
            _PART,
            PresignStoredFileUploadPartRequestDTO(
                id=started.file.id,
                upload_id=started.upload_id,
                part_number=1,
                expires_in=timedelta(minutes=5),
            ),
            ctx,
        )
        parts = await self._deposit(ctx, key, started.upload_id, [b"hel", b"lo"])

        ready = await run_operation(
            _REG,
            _COMPLETE,
            CompleteStoredFileUploadRequestDTO(
                id=started.file.id, upload_id=started.upload_id, parts=parts
            ),
            ctx,
        )

        assert url.method == "PUT"
        assert state.storage_presigns[-1]["key"] == key
        assert state.storage_presigns[-1]["upload_id"] == started.upload_id
        assert ready.status is StoredFileStatus.READY
        assert ready.size == 5

    async def test_an_assembled_mismatch_is_deleted_and_failed(self, ctx: ExecutionContext) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(multipart=True), ctx)
        key = started.file.storage_key
        parts = await self._deposit(ctx, key, started.upload_id, [b"hello", b" world"])

        with pytest.raises(CoreException) as ei:
            await run_operation(
                _REG,
                _COMPLETE,
                CompleteStoredFileUploadRequestDTO(
                    id=started.file.id, upload_id=started.upload_id, parts=parts
                ),
                ctx,
            )

        assert ei.value.code == "upload_mismatch"
        assert not await _object_exists(ctx, key)
        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED

    async def test_no_part_is_presigned_for_a_ready_file(self, ctx: ExecutionContext) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)
        await _put(ctx, started.file.storage_key, b"hello", "text/plain")
        await run_operation(
            _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
        )

        with pytest.raises(CoreException) as ei:
            await run_operation(
                _REG,
                _PART,
                PresignStoredFileUploadPartRequestDTO(
                    id=started.file.id,
                    upload_id="any",
                    part_number=1,
                    expires_in=timedelta(minutes=5),
                ),
                ctx,
            )

        assert ei.value.kind is ExceptionKind.PRECONDITION


# ....................... #


class TestDirectUploadOutbox:
    async def test_initiate_and_complete_stage_the_lifecycle_events(self, state: MockState) -> None:
        kit = StoredFileKitSpec(
            name="files",
            direct_uploads=True,
            outbox=OutboxSpec(name="files", codec=PydanticModelCodec(StoredFileOutboxPayload)),
        )
        registry = freeze_stored_file_registry(kit, tx_route="mock")
        ctx = context_from_modules(MockDepsModule(state=state))

        # Each in its own task, as two requests are: outbox staging is scoped per task.
        started = await asyncio.create_task(
            run_operation(registry, _INITIATE, _initiate_args(), ctx)
        )
        await _put(ctx, started.file.storage_key, b"hello", "text/plain")
        await asyncio.create_task(
            run_operation(
                registry, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
            )
        )

        rows = [row for rows in state.outbox_rows.values() for row in rows]
        assert [row.event_type for row in rows] == [
            StoredFileEventType.UPLOAD_PENDING,
            StoredFileEventType.UPLOADED,
        ]


# ....................... #


class TestDirectUploadExposure:
    def test_mcp_refuses_a_registry_with_writes_exposing_them(self) -> None:
        fastmcp = pytest.importorskip("fastmcp")
        from forze_mcp.registration import register_tools

        with pytest.raises(CoreException, match="sensitive"):
            register_tools(fastmcp.FastMCP("files"), _REG, lambda: None, include_writes=True)

    def test_generated_routes_refuse_initiate(self) -> None:
        from fastapi import APIRouter

        from forze_fastapi.routes import RouteBinding, attach_operation_routes, body_endpoint

        with pytest.raises(CoreException, match="sensitive"):
            attach_operation_routes(
                APIRouter(),
                registry=_REG,
                ns=_NS,
                ctx_dep=lambda: None,
                bindings={
                    StoredFileKernelOp.INITIATE_UPLOAD: RouteBinding(
                        method="POST", path="/uploads", build=body_endpoint
                    )
                },
            )


# ....................... #


def _complete_handler(
    ctx: ExecutionContext,
    *,
    reads: Any = None,
    uploads: Any = None,
    max_bytes: int | None = 1000,
) -> CompleteStoredFileUpload:
    storage = _KIT.resolved_storage

    return CompleteStoredFileUpload(
        doc=ctx.doc.command(_KIT.document),
        reads=reads if reads is not None else ctx.doc.query(_KIT.document),
        storage=ctx.storage.query(storage),
        blobs=ctx.storage.command(storage),
        uploads=uploads if uploads is not None else ctx.storage.uploads(storage),
        max_bytes=max_bytes,
    )


def _stale_reads(snapshot: StoredFileRead) -> Any:
    """A read port that answers the row as it was before a concurrent complete."""

    reads = MagicMock()
    reads.get = AsyncMock(return_value=snapshot)
    return reads


class TestCreateOnlyUploads:
    async def test_the_single_put_is_create_only(
        self, ctx: ExecutionContext, state: MockState
    ) -> None:
        result = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)

        assert result.presigned is not None
        assert result.presigned.headers["If-None-Match"] == "*"
        assert state.storage_presigns[-1]["create_only"] is True

    def test_a_single_put_expires_within_an_hour_by_default_fifteen_minutes(self) -> None:
        with pytest.raises(ValidationError):
            _initiate_args(expires_in=timedelta(hours=1, seconds=1))

        args = InitiateStoredFileUploadRequestDTO(filename="a", size=1, content_type="text/plain")
        assert args.expires_in == timedelta(minutes=15)

    async def test_multipart_completion_does_not_replace_an_existing_object(
        self, ctx: ExecutionContext
    ) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(multipart=True), ctx)
        key = started.file.storage_key
        uploads = ctx.storage.uploads(_KIT.resolved_storage)
        session = UploadSession(key=key, upload_id=started.upload_id)
        part = uploads.deposit_part(session, 1, b"other")  # type: ignore[attr-defined]
        await _put(ctx, key, b"hello", "text/plain")  # something already holds the key

        with pytest.raises(CoreException) as ei:
            await run_operation(
                _REG,
                _COMPLETE,
                CompleteStoredFileUploadRequestDTO(
                    id=started.file.id,
                    upload_id=started.upload_id,
                    parts=[UploadPartDTO(part_number=1, etag=part.etag)],
                ),
                ctx,
            )

        assert ei.value.kind is ExceptionKind.CONFLICT
        downloaded = await ctx.storage.query(_KIT.resolved_storage).download(key)
        assert downloaded.data == b"hello"


# ....................... #


class TestCompleteRace:
    async def _ready_with_stale_snapshot(
        self, ctx: ExecutionContext
    ) -> tuple[StoredFileRead, StoredFileRead]:
        started = await run_operation(_REG, _INITIATE, _initiate_args(), ctx)
        await _put(ctx, started.file.storage_key, b"hello", "text/plain")
        stale = await _row(ctx, started.file.id)
        ready = await run_operation(
            _REG, _COMPLETE, CompleteStoredFileUploadRequestDTO(id=started.file.id), ctx
        )
        return stale, ready

    async def test_the_losing_complete_conflicts_and_the_winner_stands(
        self, ctx: ExecutionContext
    ) -> None:
        stale, ready = await self._ready_with_stale_snapshot(ctx)
        loser = _complete_handler(ctx, reads=_stale_reads(stale))

        with pytest.raises(CoreException) as ei:
            await loser(CompleteStoredFileUploadRequestDTO(id=ready.id))

        assert ei.value.code == "revision_mismatch"
        assert (await _row(ctx, ready.id)).status is StoredFileStatus.READY
        assert await _object_exists(ctx, ready.storage_key)

    async def test_a_refusal_that_lost_the_race_keeps_the_winners_bytes(
        self, ctx: ExecutionContext
    ) -> None:
        stale, ready = await self._ready_with_stale_snapshot(ctx)
        # The losing complete refuses the object (a smaller cap), after the winner marked it
        # ready: the failed mark must lose on the revision, and the bytes must stay.
        loser = _complete_handler(ctx, reads=_stale_reads(stale), max_bytes=1)

        with pytest.raises(CoreException) as ei:
            await loser(CompleteStoredFileUploadRequestDTO(id=ready.id))

        assert ei.value.code == "revision_mismatch"
        assert (await _row(ctx, ready.id)).status is StoredFileStatus.READY
        assert await _object_exists(ctx, ready.storage_key)


# ....................... #


class TestMultipartSizeBeforeAssembly:
    @pytest.mark.parametrize(
        ("parts", "code"),
        [
            ([b"x" * 600, b"y" * 600], "upload_too_large"),
            ([b"hel", b"lo!"], "upload_mismatch"),
        ],
        ids=["over-the-cap", "over-the-declared-size"],
    )
    async def test_oversized_parts_are_refused_before_assembly(
        self, ctx: ExecutionContext, state: MockState, parts: list[bytes], code: str
    ) -> None:
        started = await run_operation(_REG, _INITIATE, _initiate_args(multipart=True), ctx)
        real = ctx.storage.uploads(_KIT.resolved_storage)
        session = UploadSession(key=started.file.storage_key, upload_id=started.upload_id)
        landed = [
            real.deposit_part(session, n, data)  # type: ignore[attr-defined]
            for n, data in enumerate(parts, start=1)
        ]
        spy = MagicMock(wraps=real)
        spy.list_parts = AsyncMock(side_effect=real.list_parts)
        spy.abort_upload = AsyncMock(side_effect=real.abort_upload)
        spy.complete_upload = AsyncMock()

        with pytest.raises(CoreException) as ei:
            await _complete_handler(ctx, uploads=spy)(
                CompleteStoredFileUploadRequestDTO(
                    id=started.file.id,
                    upload_id=started.upload_id,
                    parts=[UploadPartDTO(part_number=p.part_number, etag=p.etag) for p in landed],
                )
            )

        assert ei.value.code == code
        spy.complete_upload.assert_not_awaited()
        spy.abort_upload.assert_awaited_once()
        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED


# ....................... #


class TestEncryptingRouteRefusedAtResolve:
    """A client-side-encrypting route can serve no presigned URL or upload session, so the
    operations that need one refuse to build: ``check_wiring`` reports them before any call."""

    def test_check_wiring_names_exactly_the_direct_transfer_operations(self) -> None:
        from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
        from forze.application.execution import CryptoDepsModule, check_wiring
        from forze_mock import MockKeyManagement
        from forze_s3.execution.deps import S3DepsModule, S3StorageConfig
        from forze_s3.kernel.client import S3ClientPort

        kit = StoredFileKitSpec(name="files", direct_uploads=True, presign_downloads=True)
        registry = freeze_stored_file_registry(kit, tx_route="mock")

        def _ctx() -> ExecutionContext:
            return context_from_modules(
                MockDepsModule(),
                CryptoDepsModule(
                    kms=MockKeyManagement(), directory=StaticKeyDirectory(KeyRef(key_id="cmk"))
                ),
                S3DepsModule(
                    client=MagicMock(spec=S3ClientPort),
                    storages={"files": S3StorageConfig(bucket="b", encrypt=True)},
                ),
            )

        report = check_wiring(registry, _ctx)

        assert {str(f.op) for f in report.failures} == {
            *_DIRECT,
            _KEY(StoredFileKernelOp.PRESIGN_DOWNLOAD),
        }
        assert {f.code for f in report.failures} == {"storage_direct_transfer_encrypted"}
        assert all(f.is_wiring for f in report.failures)
