"""Background sweep for the stored-file kit: fail the uploads nobody finished.

A direct upload's row stays ``pending`` from ``initiate_upload`` until ``complete_upload``. A
client that never completes leaves it there for good, with whatever object it sent and any
multipart session it opened. So does a buffered ``upload`` whose process died between the
commit and the after-commit stage that stores its bytes. Nothing in the request path looks at
such a row again, so this step does, on an interval.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Final, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentCommandPort
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.storage import StorageCommandPort, StorageUploadSessionPort
from forze.application.contracts.tenancy import TenantIdentity, require_tenant_global
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import StrKey, current_entropy_source, utcnow
from forze_kits.aggregates._logger import logger
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileDocument,
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)

from .dto import _UPLOAD_URL_MAX_EXPIRY  # pyright: ignore[reportPrivateUsage]

# ----------------------- #

DEFAULT_PENDING_MAX_AGE: Final[timedelta] = timedelta(hours=24)
"""How old a ``pending`` row must be before the sweep fails it: well past the longest a
direct-upload URL lives (1 hour), so no URL handed out for it still works."""

_StoredFileCommands = DocumentCommandPort[
    StoredFileRead,
    StoredFileDocument,
    StoredFileCreateCmd,
    StoredFileUpdateCmd,
]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _PendingSweepStartup(LifecycleHook):
    """Fail stale ``pending`` stored files on an interval until the runtime drains."""

    kit: StoredFileKitSpec
    older_than: timedelta
    interval: timedelta
    jitter: float
    limit: int
    max_batches_per_tick: int
    tenants: Callable[[], Sequence[UUID]] | None = None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="stored_file_pending_sweep")),
        init=False,
    )

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop between ticks. Idempotent. A sweep still running at the deadline is cancelled,
        which can leave behind the bytes of the row it was cleaning."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def _sweep_row(
        self,
        file: StoredFileRead,
        doc: _StoredFileCommands,
        blobs: StorageCommandPort,
        uploads: StorageUploadSessionPort,
    ) -> bool:
        """Fail *file*, then free its bytes; ``False`` when another writer moved it first."""

        # Marked first, at the revision read, as complete_upload's refusal does: a racing
        # complete or a sibling sweeper either wins (this raises, nothing is touched) or loses.
        try:
            await doc.update(
                pk=file.id,
                rev=file.rev,
                dto=StoredFileUpdateCmd(status=StoredFileStatus.FAILED),
            )

        except CoreException as error:
            if error.code == "revision_mismatch" or error.kind is ExceptionKind.NOT_FOUND:
                return False

            raise

        if file.storage_key:
            # Sessions before the object: a complete assembles only through an open session,
            # so once they are aborted nothing can land after the delete.
            for session in await uploads.list_uploads(file.storage_key):
                await uploads.abort_upload(session)

            await blobs.delete(file.storage_key)

        return True

    # ....................... #

    async def _drain(self, ctx: ExecutionContext) -> None:
        """Fail stale rows under whatever tenant is bound, until a batch comes short."""

        reads = ctx.doc.query(self.kit.document)
        doc = ctx.doc.command(self.kit.document)
        blobs = ctx.storage.command(self.kit.resolved_storage)
        uploads = ctx.storage.uploads(self.kit.resolved_storage)
        cutoff = utcnow() - self.older_than
        swept = 0

        for _ in range(self.max_batches_per_tick):
            page = await reads.find_many(
                filters={
                    "$values": {
                        "status": StoredFileStatus.PENDING.value,
                        "created_at": {"$lt": cutoff},
                    }
                },
                pagination={"limit": self.limit},
            )

            for file in page.hits:
                try:
                    swept += await self._sweep_row(file, doc, blobs, uploads)

                except asyncio.CancelledError:
                    raise

                except CoreException as error:
                    if error.kind is ExceptionKind.CONFIGURATION:
                        raise

                    logger.exception("Stored-file pending sweep failed a row", file_id=str(file.id))

                except Exception:
                    logger.exception("Stored-file pending sweep failed a row", file_id=str(file.id))

            if len(page.hits) < self.limit:
                break

        if swept:
            logger.info(
                "Stored-file pending sweep failed stale uploads",
                kit=str(self.kit.name),
                swept=swept,
            )

    # ....................... #

    async def _sweep_tick(self, ctx: ExecutionContext, tenants: Sequence[UUID] | None) -> None:
        if tenants is None:
            await self._drain(ctx)
            return

        for tenant in tenants:
            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await self._drain(ctx)

            except asyncio.CancelledError:
                raise

            except CoreException as error:
                # Wiring does not fix itself on the next tenant: stop loudly. Operational
                # failures stay with their tenant.
                if error.kind is ExceptionKind.CONFIGURATION:
                    raise

                logger.exception("Stored-file pending sweep failed for tenant", tenant=str(tenant))

            except Exception:
                logger.exception("Stored-file pending sweep failed for tenant", tenant=str(tenant))

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            logger.warning("Stored-file pending sweep already running; ignoring duplicate startup")
            return

        # Frozen at startup like the other tenant-sharded loops; restart to repartition.
        tenants = list(self.tenants()) if self.tenants is not None else None
        # Resolved for a shard too, so a missing route fails startup rather than every tick.
        reads = ctx.doc.query(self.kit.document)
        storage = ctx.storage.query(self.kit.resolved_storage)

        if tenants is None:
            require_tenant_global(reads, what="Stored-file pending sweep")
            require_tenant_global(storage, what="Stored-file pending sweep")

        self.control.arm()

        async def _loop() -> None:
            while True:
                try:
                    await self._sweep_tick(ctx, tenants)

                except asyncio.CancelledError:
                    raise

                except CoreException as error:
                    if error.kind is ExceptionKind.CONFIGURATION:
                        logger.exception(
                            "Stored-file pending sweep hit a configuration error; loop "
                            "stopped — fix the wiring and restart"
                        )
                        return

                    logger.exception("Stored-file pending sweep failed")

                except Exception:
                    logger.exception("Stored-file pending sweep failed")

                # Jitter only spreads replicas out; concurrent sweeps are safe (see the step).
                if await self.control.sleep_or_stop(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
                    )
                ):
                    return

        try:
            self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
            ctx.drainables.register(self)

        except BaseException:
            task = self.control.task

            if task is not None:
                task.cancel()

            self.control.task = None
            self.control.event = None
            raise


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _PendingSweepShutdown(LifecycleHook):
    """Stop the sweep loop (fallback for a hand-driven lifecycle; idempotent)."""

    startup: _PendingSweepStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def stored_file_pending_sweep_lifecycle_step(
    kit: StoredFileKitSpec,
    *,
    older_than: timedelta = DEFAULT_PENDING_MAX_AGE,
    interval: timedelta = timedelta(minutes=10),
    jitter: float = 0.2,
    limit: int = 100,
    max_batches_per_tick: int = 10,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Fail the stored files left ``pending`` longer than *older_than*, and free their bytes.

    Each sweep reads ``pending`` rows created before the cutoff in batches of *limit*, until
    one comes short or *max_batches_per_tick* have run, then sleeps *interval* with
    multiplicative *jitter*. For each row it:

    1. marks the row ``failed`` at the revision it read, as ``complete_upload`` does when it
       refuses an upload, so a racing ``complete_upload`` either wins (the sweep leaves the
       row alone) or loses with ``revision_mismatch``;
    2. aborts every multipart session open at the row's key;
    3. deletes the object at the key, if one landed.

    A buffered ``upload``'s pending row has no key yet, so it is only marked failed.

    *older_than* must exceed the longest a direct-upload URL lives (1 hour), or a client
    could still upload into a key the sweep has cleaned. Raise it above your longest
    multipart upload, which can keep asking for part URLs while its row is pending.

    Concurrent sweepers never fail a row twice: the revision check lets one win, and the
    cleanup is idempotent. So run the step unwrapped on every replica. *tenants* works as on
    :func:`~forze_kits.integrations.realtime.realtime_mailbox_retention_lifecycle_step`: pass
    it for tenant-aware document and storage routes; ``None`` sweeps once with no tenant
    bound, and fails startup over a tenant-scoped route (``tenant_global_required``). A
    ``configuration`` error stops the loop; any other error is logged, per row and per
    tenant, and retried on the next sweep.

    :param kit: The stored-file kit spec whose rows and objects to sweep.
    :param older_than: Age past which a ``pending`` row counts as abandoned (default 24 hours).
    :param interval: Time between sweeps.
    :param jitter: Multiplicative jitter on *interval*, in ``[0, 1)``.
    :param limit: Rows read per batch.
    :param max_batches_per_tick: Batches one sweep runs at most.
    :param tenants: The tenants to sweep, each with its tenant bound.
    :param step_id: Lifecycle step id; derived from the kit name by default.
    :returns: The lifecycle step.
    :raises CoreException: ``configuration`` for an *older_than* within the upload URL
        lifetime, a non-positive *interval*, a *jitter* outside ``[0, 1)``, or a *limit* or
        *max_batches_per_tick* below 1.
    """

    if older_than <= _UPLOAD_URL_MAX_EXPIRY:
        raise exc.configuration(
            f"Stored-file pending sweep older_than must exceed {_UPLOAD_URL_MAX_EXPIRY}, the "
            f"longest a direct-upload URL lives, not {older_than}.",
        )

    if interval.total_seconds() <= 0:
        raise exc.configuration("Stored-file pending sweep interval must be positive")

    if not 0.0 <= jitter < 1.0:
        raise exc.configuration("Jitter must be in [0, 1)")

    if limit < 1 or max_batches_per_tick < 1:
        raise exc.configuration("Stored-file pending sweep limit and batches must be >= 1")

    startup = _PendingSweepStartup(
        kit=kit,
        older_than=older_than,
        interval=interval,
        jitter=jitter,
        limit=limit,
        max_batches_per_tick=max_batches_per_tick,
        tenants=tenants,
    )

    return LifecycleStep(
        id=step_id if step_id is not None else f"stored_file_pending_sweep_{kit.name}",
        startup=startup,
        shutdown=_PendingSweepShutdown(startup=startup),
        requires_long_running=True,
    )
