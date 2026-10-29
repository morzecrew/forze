"""The stored-file pending sweep: fail abandoned uploads and free their bytes (on the mock).

# covers: forze_kits.aggregates.stored_file.stored_file_pending_sweep_lifecycle_step
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.storage import UploadSession
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import FrozenTimeSource, bind_time_source, utcnow
from forze.testing import context_from_modules
from forze_kits.aggregates.stored_file import (
    CompleteStoredFileUpload,
    CompleteStoredFileUploadRequestDTO,
    InitiateStoredFileUploadRequestDTO,
    StoredFileKernelOp,
    StoredFileUploadSessionDTO,
    freeze_stored_file_registry,
    stored_file_pending_sweep_lifecycle_step,
)
from forze_kits.aggregates.stored_file.lifecycle import (
    _PendingSweepStartup,  # pyright: ignore[reportPrivateUsage]
)
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
)
from forze_mock import MockDepsModule, MockRouteConfig, MockState

# ----------------------- #

_KIT = StoredFileKitSpec(name="files", direct_uploads=True)
_REG = freeze_stored_file_registry(_KIT, tx_route="mock")
_INITIATE = _KIT.document.default_namespace.key(StoredFileKernelOp.INITIATE_UPLOAD)

_STALE = timedelta(hours=25)
_FAST = timedelta(milliseconds=10)
_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _startup(**kw: Any) -> _PendingSweepStartup:
    step = stored_file_pending_sweep_lifecycle_step(_KIT, interval=_FAST, jitter=0.0, **kw)
    return cast(_PendingSweepStartup, step.startup)


async def _initiate(
    ctx: ExecutionContext, *, age: timedelta = _STALE, multipart: bool = False
) -> StoredFileUploadSessionDTO:
    args = InitiateStoredFileUploadRequestDTO(
        filename="notes.txt",
        size=5,
        content_type="text/plain",
        multipart=multipart,
    )

    with bind_time_source(FrozenTimeSource(instant=utcnow() - age)):
        return await run_operation(_REG, _INITIATE, args, ctx)


async def _put(ctx: ExecutionContext, key: str) -> None:
    """What the client's PUT to the presigned URL does to the store."""

    async def _chunks():  # type: ignore[no-untyped-def]
        yield b"hello"

    storage = ctx.storage.command(_KIT.resolved_storage)
    await storage.overwrite_stream(key, _chunks(), content_type="text/plain")


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


async def _wait_until(check, timeout: float = 5.0) -> bool:  # type: ignore[no-untyped-def]
    waited = 0.0

    while waited < timeout:
        if await check():
            return True

        await asyncio.sleep(0.01)
        waited += 0.01

    return False


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=MockState()))


# ....................... #


class TestSweep:
    async def test_a_stale_upload_is_failed_and_its_object_deleted(
        self, ctx: ExecutionContext
    ) -> None:
        started = await _initiate(ctx)
        await _put(ctx, started.file.storage_key)

        await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]

        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED
        assert not await _object_exists(ctx, started.file.storage_key)

    async def test_a_fresh_upload_is_kept(self, ctx: ExecutionContext) -> None:
        started = await _initiate(ctx, age=timedelta(hours=23))
        await _put(ctx, started.file.storage_key)

        await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]

        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.PENDING
        assert await _object_exists(ctx, started.file.storage_key)

    async def test_a_stale_multipart_session_is_aborted(self, ctx: ExecutionContext) -> None:
        started = await _initiate(ctx, multipart=True)
        key = started.file.storage_key
        uploads = ctx.storage.uploads(_KIT.resolved_storage)
        uploads.deposit_part(  # type: ignore[attr-defined]
            UploadSession(key=key, upload_id=started.upload_id), 1, b"hel"
        )

        assert [s.upload_id for s in await uploads.list_uploads(key)] == [started.upload_id]

        await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]

        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED
        assert await uploads.list_uploads(key) == []

    async def test_a_buffered_uploads_stale_row_is_only_failed(self, ctx: ExecutionContext) -> None:
        # A buffered upload whose process died before its after-commit stage: no key yet.
        with bind_time_source(FrozenTimeSource(instant=utcnow() - _STALE)):
            row = await ctx.doc.command(_KIT.document).create(
                StoredFileCreateCmd(filename="a.txt", size=5)
            )

        await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]

        assert (await _row(ctx, row.id)).status is StoredFileStatus.FAILED

    async def test_a_stale_ready_file_is_left_alone(self, ctx: ExecutionContext) -> None:
        started = await _initiate(ctx)
        await _put(ctx, started.file.storage_key)
        await run_operation(
            _REG,
            _KIT.document.default_namespace.key(StoredFileKernelOp.COMPLETE_UPLOAD),
            CompleteStoredFileUploadRequestDTO(id=started.file.id),
            ctx,
        )

        await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]

        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.READY
        assert await _object_exists(ctx, started.file.storage_key)

    @pytest.mark.parametrize(("batches", "failed"), [(10, 5), (1, 2)])
    async def test_it_reads_in_batches_up_to_the_tick_budget(
        self, ctx: ExecutionContext, batches: int, failed: int
    ) -> None:
        rows = [await _initiate(ctx) for _ in range(5)]

        await _startup(limit=2, max_batches_per_tick=batches)._drain(  # pyright: ignore[reportPrivateUsage]
            ctx
        )

        statuses = [(await _row(ctx, r.file.id)).status for r in rows]
        assert statuses.count(StoredFileStatus.FAILED) == failed

    async def test_one_rows_failure_does_not_stop_the_batch(
        self, ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first, second = await _initiate(ctx), await _initiate(ctx)
        real = _PendingSweepStartup._sweep_row  # pyright: ignore[reportPrivateUsage]
        calls = {"n": 0}

        async def _flaky(self: _PendingSweepStartup, *args: Any) -> bool:
            calls["n"] += 1

            if calls["n"] == 1:
                raise exc.infrastructure("store blip")

            return await real(self, *args)

        monkeypatch.setattr(_PendingSweepStartup, "_sweep_row", _flaky)
        await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]

        statuses = {(await _row(ctx, r.file.id)).status for r in (first, second)}
        assert statuses == {StoredFileStatus.PENDING, StoredFileStatus.FAILED}


# ....................... #


class TestRaceWithComplete:
    async def test_a_complete_that_won_keeps_its_file(self, ctx: ExecutionContext) -> None:
        started = await _initiate(ctx)
        await _put(ctx, started.file.storage_key)
        seen_by_sweep = await _row(ctx, started.file.id)
        await run_operation(
            _REG,
            _KIT.document.default_namespace.key(StoredFileKernelOp.COMPLETE_UPLOAD),
            CompleteStoredFileUploadRequestDTO(id=started.file.id),
            ctx,
        )
        storage = _KIT.resolved_storage

        swept = await _startup()._sweep_row(  # pyright: ignore[reportPrivateUsage]
            seen_by_sweep,
            ctx.doc.command(_KIT.document),
            ctx.storage.command(storage),
            ctx.storage.uploads(storage),
        )

        assert swept is False
        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.READY
        assert await _object_exists(ctx, started.file.storage_key)

    async def test_a_complete_that_lost_conflicts_and_marks_nothing(
        self, ctx: ExecutionContext
    ) -> None:
        started = await _initiate(ctx)
        await _put(ctx, started.file.storage_key)
        storage = _KIT.resolved_storage
        real = ctx.storage.query(storage)

        async def _head_then_sweep(key: str) -> Any:
            # The complete has checked the object; the sweep fails the row before it updates.
            head = await real.head(key)
            await _startup()._drain(ctx)  # pyright: ignore[reportPrivateUsage]
            return head

        racing = MagicMock(wraps=real)
        racing.head = AsyncMock(side_effect=_head_then_sweep)
        complete = CompleteStoredFileUpload(
            doc=ctx.doc.command(_KIT.document),
            reads=ctx.doc.query(_KIT.document),
            storage=racing,
            blobs=ctx.storage.command(storage),
            uploads=ctx.storage.uploads(storage),
        )

        with pytest.raises(CoreException) as ei:
            await complete(CompleteStoredFileUploadRequestDTO(id=started.file.id))

        assert ei.value.code == "revision_mismatch"
        assert (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED
        assert not await _object_exists(ctx, started.file.storage_key)


# ....................... #


def _runtime(state: MockState, *, tenant_aware: bool = False) -> ExecutionRuntime:
    routes = {"files": MockRouteConfig(tenant_aware=True)} if tenant_aware else {}
    module = MockDepsModule(state=state, routes=routes)
    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


def _bind(ctx: ExecutionContext, tenant: UUID):  # type: ignore[no-untyped-def]
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


class TestStep:
    async def test_it_sweeps_on_an_interval_and_stops_cleanly(self) -> None:
        step = stored_file_pending_sweep_lifecycle_step(_KIT, interval=_FAST, jitter=0.0)
        runtime = _runtime(MockState())

        async with runtime.scope():
            ctx = runtime.get_context()
            started = await _initiate(ctx)

            await step.startup(ctx)
            await step.startup(ctx)  # a duplicate startup is ignored while running

            sweep = cast(_PendingSweepStartup, step.startup)
            assert sweep in ctx.drainables.loops  # type: ignore[comparison-overlap]

            async def _failed() -> bool:
                return (await _row(ctx, started.file.id)).status is StoredFileStatus.FAILED

            assert await _wait_until(_failed)

            await step.shutdown(ctx)
            assert sweep.task is not None and sweep.task.done() and not sweep.task.cancelled()

    async def test_it_sweeps_each_tenant_and_isolates_a_failing_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        broken, healthy = _T1, _T2
        original = _PendingSweepStartup._drain  # pyright: ignore[reportPrivateUsage]

        async def _per_tenant(self: _PendingSweepStartup, ctx: ExecutionContext) -> None:
            tenant = ctx.inv_ctx.get_tenant()

            if tenant is not None and tenant.tenant_id == broken:
                raise exc.infrastructure("tenant store down")

            await original(self, ctx)

        monkeypatch.setattr(_PendingSweepStartup, "_drain", _per_tenant)
        step = stored_file_pending_sweep_lifecycle_step(
            _KIT, interval=_FAST, jitter=0.0, tenants=lambda: (broken, healthy)
        )
        runtime = _runtime(MockState(), tenant_aware=True)

        async with runtime.scope():
            ctx = runtime.get_context()
            started: dict[UUID, StoredFileUploadSessionDTO] = {}

            for tenant in (broken, healthy):
                with _bind(ctx, tenant):
                    started[tenant] = await _initiate(ctx)

            await step.startup(ctx)

            async def _healthy_failed() -> bool:
                with _bind(ctx, healthy):
                    row = await _row(ctx, started[healthy].file.id)
                    return row.status is StoredFileStatus.FAILED

            assert await _wait_until(_healthy_failed)

            with _bind(ctx, broken):
                assert (await _row(ctx, started[broken].file.id)).status is StoredFileStatus.PENDING

            task = cast(_PendingSweepStartup, step.startup).task
            assert task is not None and not task.done()  # isolated, not terminal

            await step.shutdown(ctx)

    @pytest.mark.parametrize("tenants", [None, (_T1,)], ids=["tenant-global", "per-tenant"])
    async def test_a_configuration_error_stops_the_loop(
        self, monkeypatch: pytest.MonkeyPatch, tenants: tuple[UUID, ...] | None
    ) -> None:
        async def _config_error(self: _PendingSweepStartup, ctx: ExecutionContext) -> None:
            raise exc.configuration("route is not wired")

        monkeypatch.setattr(_PendingSweepStartup, "_drain", _config_error)
        step = stored_file_pending_sweep_lifecycle_step(
            _KIT,
            interval=_FAST,
            jitter=0.0,
            tenants=(lambda: tenants) if tenants is not None else None,
        )
        runtime = _runtime(MockState(), tenant_aware=tenants is not None)

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            task = cast(_PendingSweepStartup, step.startup).task
            assert task is not None

            async def _stopped() -> bool:
                return task.done()

            assert await _wait_until(_stopped)
            assert not task.cancelled()

            await step.shutdown(ctx)

    async def test_a_tenant_scoped_route_without_tenants_fails_startup(self) -> None:
        step = stored_file_pending_sweep_lifecycle_step(_KIT)
        runtime = _runtime(MockState(), tenant_aware=True)

        async with runtime.scope():
            with pytest.raises(CoreException) as ei:
                await step.startup(runtime.get_context())

        assert ei.value.code == "tenant_global_required"

    def test_the_step_id_names_the_kit(self) -> None:
        assert (
            stored_file_pending_sweep_lifecycle_step(_KIT).id == "stored_file_pending_sweep_files"
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"older_than": timedelta(hours=1)},
            {"interval": timedelta(0)},
            {"jitter": 1.0},
            {"limit": 0},
            {"max_batches_per_tick": 0},
        ],
        ids=["older-than-within-url-lifetime", "interval", "jitter", "limit", "batches"],
    )
    def test_bad_settings_are_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as ei:
            stored_file_pending_sweep_lifecycle_step(_KIT, **kwargs)

        assert ei.value.kind is ExceptionKind.CONFIGURATION
