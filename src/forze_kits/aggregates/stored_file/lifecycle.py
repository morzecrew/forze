"""Background sweep for the stored-file kit: fail the uploads nobody finished.

A direct upload's row stays ``pending`` from ``initiate_upload`` until ``complete_upload``. A
client that never completes leaves it there for good, with whatever object it sent and any
multipart session it opened. So does a buffered ``upload`` whose process died between the
commit and the after-commit stage that stores its bytes. Nothing in the request path looks at
such a row again, so this step does, on an interval: it fails the row, then frees its bytes,
and retries the freeing on a later sweep if it fails.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Final, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.storage import StorageCommandPort, StorageUploadSessionPort
from forze.application.contracts.tenancy import TenantIdentity, require_tenant_global
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict, StrKey, current_entropy_source, utcnow
from forze_kits.aggregates._logger import logger
from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileDocument,
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)
from forze_kits.integrations._wiring import is_wiring_error

from .dto import _UPLOAD_URL_MAX_EXPIRY  # pyright: ignore[reportPrivateUsage]

# ----------------------- #

DEFAULT_PENDING_MAX_AGE: Final[timedelta] = timedelta(hours=24)
"""How long a ``pending`` row must go without an update before the sweep fails it: well past
the longest a direct-upload URL lives (1 hour). Every URL handed out was signed before the
row's last update, so none of them still works."""

_StoredFileCommands = DocumentCommandPort[
    StoredFileRead,
    StoredFileDocument,
    StoredFileCreateCmd,
    StoredFileUpdateCmd,
]


# ....................... #


async def _update_at_rev(
    doc: _StoredFileCommands,
    file: StoredFileRead,
    dto: StoredFileUpdateCmd,
) -> StoredFileRead | None:
    """*file* updated at the revision read, or ``None`` when another writer moved it first:
    what is left is then theirs to finish."""

    try:
        return await doc.update(pk=file.id, rev=file.rev, dto=dto)

    except CoreException as error:
        if error.code == "revision_mismatch" or error.kind is ExceptionKind.NOT_FOUND:
            return None

        raise


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
        """Stop between ticks. Idempotent. A sweep still running at the deadline is cancelled;
        the row it was cleaning keeps its key, so a later sweep finishes it."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def _sweep_row(
        self,
        file: StoredFileRead,
        doc: _StoredFileCommands,
        blobs: StorageCommandPort,
        uploads: StorageUploadSessionPort,
    ) -> bool:
        """Fail *file* if it is pending, then free its bytes; ``False`` when another writer
        moved it first."""

        if file.status is StoredFileStatus.PENDING:
            # Marked first, at the revision read, as complete_upload's refusal does: a racing
            # complete or a sibling sweeper either wins, and the row is left to it, or loses.
            marked = await _update_at_rev(
                doc, file, StoredFileUpdateCmd(status=StoredFileStatus.FAILED)
            )

            if marked is None:
                return False

            file = marked

        if not file.storage_key:
            return True

        try:
            # Sessions before the object: a complete assembles only through an open session, so
            # once they are aborted nothing can land after the delete.
            for session in await uploads.list_uploads(file.storage_key):
                await uploads.abort_upload(session)

        finally:
            # Deleted even when the sessions could not be listed; the key stays, so a later
            # sweep retries both.
            await blobs.delete(file.storage_key)

        # Cleared once both are gone, so the failed-row pass stops reading the row.
        await _update_at_rev(doc, file, StoredFileUpdateCmd(storage_key=None))

        return True

    # ....................... #

    async def _scan(
        self,
        values: JsonDict,
        reads: DocumentQueryPort[StoredFileRead],
        doc: _StoredFileCommands,
        blobs: StorageCommandPort,
        uploads: StorageUploadSessionPort,
    ) -> int:
        """Sweep the rows matching *values* in ``id`` order, a batch at a time; how many moved.

        Keyset on ``id``: a row whose sweep keeps failing still matches, and an offsetless
        query would read it again at the head of every batch, ahead of the rows behind it.
        """

        after: UUID | None = None
        swept = 0

        for _ in range(self.max_batches_per_tick):
            page = await reads.find_many(
                filters={"$values": values if after is None else {**values, "id": {"$gt": after}}},
                pagination={"limit": self.limit},
                sorts={"id": "asc"},
            )

            for file in page.hits:
                try:
                    swept += await self._sweep_row(file, doc, blobs, uploads)

                except asyncio.CancelledError:
                    raise

                except Exception as error:
                    if is_wiring_error(error):
                        raise

                    # One row's failure, a configuration one included (a route that started
                    # encrypting refuses its sessions), stays with that row.
                    logger.exception(
                        "Stored-file pending sweep skipped a row", file_id=str(file.id)
                    )

            if len(page.hits) < self.limit:
                break

            after = page.hits[-1].id

        return swept

    # ....................... #

    async def _drain(self, ctx: ExecutionContext) -> None:
        """Fail stale pending rows, then retry the cleanup of stale failed rows that still hold
        a key, under whatever tenant is bound."""

        ports = (
            ctx.doc.query(self.kit.document),
            ctx.doc.command(self.kit.document),
            ctx.storage.command(self.kit.resolved_storage),
            ctx.storage.uploads(self.kit.resolved_storage),
        )
        stale: JsonDict = {"last_update_at": {"$lt": utcnow() - self.older_than}}
        failed = await self._scan({**stale, "status": StoredFileStatus.PENDING.value}, *ports)
        # A failed row keeps its key until its bytes are gone: a cleanup that failed after the
        # mark, or a complete_upload refusal whose PUT URL could still land an object then.
        cleaned = await self._scan(
            {**stale, "status": StoredFileStatus.FAILED.value, "storage_key": {"$null": False}},
            *ports,
        )

        if failed or cleaned:
            logger.info(
                "Stored-file pending sweep failed stale uploads and freed their bytes",
                kit=str(self.kit.name),
                failed=failed,
                cleaned=cleaned,
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

            except Exception as error:
                # Wiring does not fix itself on the next tenant: stop loudly. Anything else,
                # a tenant's own configuration error included, stays with that tenant.
                if is_wiring_error(error):
                    raise

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
        ctx.doc.command(self.kit.document)
        ctx.storage.command(self.kit.resolved_storage)
        ctx.storage.uploads(self.kit.resolved_storage)

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

                except Exception as error:
                    if is_wiring_error(error):
                        logger.exception(
                            "Stored-file pending sweep hit a wiring error; loop stopped — fix "
                            "the wiring and restart"
                        )
                        return

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

    A row's age counts from its last update: ``initiate_upload`` creates it and each
    ``presign_upload_part`` touches it. Each sweep reads the ``pending`` rows not updated since
    the cutoff in ``id`` order, in batches of *limit*, until one comes short or
    *max_batches_per_tick* have run. For each row it:

    1. marks the row ``failed`` at the revision it read, as ``complete_upload`` does when it
       refuses an upload, so a racing ``complete_upload`` either wins (the sweep leaves the
       row alone) or loses with ``revision_mismatch``;
    2. aborts every multipart session open at the row's key;
    3. deletes the object at the key, if one landed;
    4. clears the row's ``storage_key``.

    It then retries steps 2 to 4 for the ``failed`` rows not updated since the cutoff that
    still hold a key: a cleanup that failed after the mark, or a ``complete_upload`` refusal
    whose single ``PUT`` URL could still land an object afterwards. A buffered ``upload``'s
    pending row has no key yet, so it is only marked failed. Then the sweep sleeps *interval*
    with multiplicative *jitter*.

    *older_than* must exceed the longest a direct-upload URL lives (1 hour). Every URL is
    signed before the update that records it: ``initiate_upload`` signs before it creates the
    row, and ``presign_upload_part`` signs, then touches the row, and hands the URL out only
    if the touch succeeds. So every URL handed out expires within 1 hour of the row's last
    update, and none still works once the row is past the cutoff.

    Concurrent sweepers never fail a row twice: the revision check lets one win, and the
    cleanup is idempotent. So run the step unwrapped on every replica. *tenants* works as on
    :func:`~forze_kits.integrations.realtime.realtime_mailbox_retention_lifecycle_step`: pass
    it for tenant-aware document and storage routes; ``None`` sweeps once with no tenant
    bound, and fails startup over a tenant-scoped route (``tenant_global_required``). A
    missing route fails startup too. Only a wiring error (``tenant_global_required`` and the
    like) stops the loop; any other error, a configuration error included, is logged for its
    row or tenant and retried on a later sweep.

    :param kit: The stored-file kit spec whose rows and objects to sweep.
    :param older_than: How long a ``pending`` row may go without an update before it counts
        as abandoned (default 24 hours).
    :param interval: Time between sweeps.
    :param jitter: Multiplicative jitter on *interval*, in ``[0, 1)``.
    :param limit: Rows read per batch.
    :param max_batches_per_tick: Batches each pass of one sweep runs at most.
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
