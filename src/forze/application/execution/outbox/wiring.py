"""Compose outbox command ports at the execution boundary."""

from collections.abc import Awaitable, Sequence
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from forze.application.contracts.crypto import BytesCipherPort, KeyringDepKey
from forze.application.contracts.outbox import OutboxSpec, StagedOutboxEntry
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.outbox import OutboxStaging, StagingOutboxCommand
from forze.application.integrations.outbox.staging import FlushRowsFn
from forze.base.exceptions import exc

from .enrichment import InvocationOutboxEnricher

# ----------------------- #


@runtime_checkable
class OutboxRowPersistPort(Protocol):
    """Narrow store surface used when wiring flush into staging."""

    def persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> Awaitable[int]:
        """Insert staged rows; return count of new rows."""
        ...


# ....................... #


def _resolve_payload_cipher(
    ctx: ExecutionContext, spec: OutboxSpec[Any]
) -> BytesCipherPort | None:
    """The keyring for whole-payload encryption, or ``None`` when the route is plaintext.

    Fails closed when a route declares ``encrypt=True`` but no keyring is wired — the
    same posture as document field encryption.
    """

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
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=flush_rows,
        payload_cipher=_resolve_payload_cipher(ctx, spec),
    )
    return StagingOutboxCommand(spec=spec, staging=staging)


# ....................... #


def build_staging_outbox_command_for_store[M: BaseModel](
    ctx: ExecutionContext,
    spec: OutboxSpec[M],
    store: OutboxRowPersistPort,
) -> StagingOutboxCommand[M]:
    """Wire command port that flushes via *store.persist_rows*."""

    return build_staging_outbox_command(ctx, spec, flush_rows=store.persist_rows)
