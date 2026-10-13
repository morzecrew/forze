"""Compose outbox command ports at the execution boundary."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel

from forze.application._logger import logger
from forze.application.contracts.crypto import BytesCipherPort, KeyringDepKey
from forze.application.contracts.hlc import HlcCheckpointDepKey, HlcCheckpointPort
from forze.application.contracts.outbox import (
    OutboxRowPersistPort,
    OutboxSpec,
    OutboxWakeSignal,
)
from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubCommandPort
from forze.application.execution.context import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach
from forze.application.integrations.outbox import OutboxStaging, StagingOutboxCommand
from forze.application.integrations.outbox.staging import FlushRowsFn
from forze.base.exceptions import exc

from .enrichment import InvocationOutboxEnricher

# ----------------------- #


def _resolve_payload_cipher(ctx: ExecutionContext, spec: OutboxSpec[Any]) -> BytesCipherPort | None:
    """The keyring for whole-payload encryption, or ``None`` when the route is plaintext.

    Fails closed when a route declares ``encrypt=True`` but no keyring is wired — the
    same posture as document field encryption. Enforces the deployment ``required_reach``
    floor first, before the plaintext early-out, so a ``none`` route is rejected under a floor.
    """

    enforce_required_reach(ctx.deps, route=str(spec.name), declared=spec.encryption, kind="outbox")

    if not spec.encrypts:
        return None

    if not ctx.deps.exists(KeyringDepKey):
        raise exc.configuration(
            f"Outbox route {spec.name!r} declares encryption={spec.encryption!r} but no "
            "keyring is wired. Add a CryptoDepsModule (registers the keyring) or set "
            "encryption='none'.",
            code="core.outbox.encryption_wiring",
        )

    return ctx.deps.provide(KeyringDepKey)


# ....................... #


def _resolve_hlc_checkpoint(ctx: ExecutionContext) -> HlcCheckpointPort | None:
    """The node's HLC high-water-mark store, or ``None`` when unwired (optional).

    A node-global :class:`SimpleDepPort` (one clock per runtime), so it is resolved once
    per scope. Absent by default — recovery is then a no-op and the clock resumes from
    ``(0, 0)`` as before, not an error."""

    if not ctx.deps.exists(HlcCheckpointDepKey):
        return None

    return ctx.deps.resolve_simple(ctx, HlcCheckpointDepKey)


# ....................... #


def _wake_after_commit(
    ctx: ExecutionContext, spec: OutboxSpec[Any]
) -> Callable[[], Awaitable[None]] | None:
    """The wake a flush calls once it wrote rows, or ``None`` when the route has none.

    The pub/sub port is resolved here, when the command is built, so a wake route that is
    not wired fails the operation as a configuration error instead of every wake being
    dropped quietly. Publishing is deferred until the root transaction commits — a wake
    must never arrive before its rows are visible, and a rollback publishes nothing — and
    it is best-effort: a failed publish is logged, never raised, as the relay's poll still
    delivers the rows.
    """

    wake = spec.wake

    if wake is None:
        return None

    port: PubSubCommandPort[OutboxWakeSignal] = ctx.deps.resolve_configurable(
        ctx, PubSubCommandDepKey, wake.spec, route=wake.spec.name
    )
    signal = OutboxWakeSignal(route=str(spec.name))

    async def _publish() -> None:
        try:
            await port.publish(wake.topic, signal)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.warning(
                "Outbox wake publish failed; the relay delivers the rows on its next poll",
                outbox_route=str(spec.name),
                exc_info=True,
            )

    async def _wake() -> None:
        await ctx.tx_ctx.run_or_defer(_publish)

    return _wake


# ....................... #


def build_staging_outbox_command[M: BaseModel](
    ctx: ExecutionContext,
    spec: OutboxSpec[M],
    *,
    flush_rows: FlushRowsFn,
) -> StagingOutboxCommand[M]:
    """Wire :class:`StagingOutboxCommand` for *spec* using *ctx* invocation state."""

    staging = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx, clock=ctx.outbox_clock),
        flush_rows=flush_rows,
        payload_cipher=_resolve_payload_cipher(ctx, spec),
        tx_depth=ctx.tx_ctx.depth,
        # The node-global checkpoint only advances atomically with the rows when the flush
        # runs inside the business transaction, so wire it only for routes that require one;
        # a non-transactional route keeps the prior resume-from-(0,0) behavior.
        checkpoint=(_resolve_hlc_checkpoint(ctx) if spec.require_transaction else None),
    )
    return StagingOutboxCommand(spec=spec, staging=staging, wake=_wake_after_commit(ctx, spec))


# ....................... #


def build_staging_outbox_command_for_store[M: BaseModel](
    ctx: ExecutionContext,
    spec: OutboxSpec[M],
    store: OutboxRowPersistPort,
) -> StagingOutboxCommand[M]:
    """Wire command port that flushes via *store.persist_rows*."""

    return build_staging_outbox_command(ctx, spec, flush_rows=store.persist_rows)
