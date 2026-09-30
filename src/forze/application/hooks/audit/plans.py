"""Wire audit into :class:`~forze.application.execution.operations.registry.OperationRegistry` plans.

Two hooks, split by outcome, because no single hook both sees a denial and shares the
operation's transaction. Guards run in the outer scope, before the transaction opens, so only
an outer ``finally`` observes a denial — and it runs after the transaction has closed. So:

- an **admitted** operation's row is written by an ``on_success`` step inside the transaction,
  and commits or rolls back with the operation's own write;
- a **failed** or **denied** operation's row is written by the outer ``finally``, after the
  transaction rolled back, in a write of its own;
- a **read** (a ``QUERY`` operation) runs in a read-only transaction, so its row is written by
  the outer ``finally`` once the read completes.
"""

from collections.abc import Callable, Mapping
from typing import Any, Final, final
from uuid import UUID

import attrs

from forze.application._logger import logger
from forze.application.contracts.audit import (
    AUDIT_METADATA_REFUSED,
    AuditDepKey,
    AuditEntry,
    AuditObjectRef,
    AuditOutcome,
    AuditPort,
    AuditSpec,
)
from forze.application.contracts.execution import (
    Failure,
    Finally,
    FinallyStep,
    OnSuccess,
    OnSuccessStep,
    Outcome,
)
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations.registry.binder import OperationRegistryBinder
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import StrKey, utcnow

# ----------------------- #

_DENIALS: Final = frozenset({ExceptionKind.AUTHENTICATION, ExceptionKind.AUTHORIZATION})
"""Failure kinds recorded as ``denied`` rather than ``failed``."""

type AuditMetadata = Callable[[Any, Any], Mapping[str, object]]
"""``(args, result) -> metadata``; ``result`` is ``None`` for a failed or denied operation."""

type AuditObject = Callable[[Any, Any], AuditObjectRef | None]
"""``(args, result) -> object_ref``; ``result`` is ``None`` for a failed or denied operation."""

type AuditOwner = Callable[[Any, Any], UUID | None]
"""``(args, result) -> owner``: whose data a read returned."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Audited:
    """Audit an operation under :attr:`spec` — bound with :meth:`bind`.

    The actor and subject come from the invocation's authenticated identity, never from the
    operation's arguments: a trail whose actor the caller can set records nothing.
    """

    spec: AuditSpec
    """The audited action and its metadata allowlist."""

    metadata: AuditMetadata | None = None
    """Builds the row's metadata; every key must be in :attr:`AuditSpec.allowed_metadata`."""

    object_ref: AuditObject | None = None
    """Names what the operation acted on."""

    owner: AuditOwner | None = None
    """For a read: whose data it returned. A principal reading their own data, undelegated,
    is not recorded. Without it every admitted read is recorded — the skip is never a guess."""

    # ....................... #

    def bind(
        self,
        binder: OperationRegistryBinder,
        *,
        transactional: bool = True,
        step_id: StrKey | None = None,
    ) -> OperationRegistryBinder:
        """Bind the audit hooks to the operations *binder* selects.

        ``transactional`` (the default) writes an admitted operation's row inside its
        transaction, so the binder's operations need a transaction route; pass ``False`` for
        operations that run without one, whose row is then written after they return.
        """

        sid = step_id if step_id is not None else f"audit.{self.spec.action}"
        outer = (
            binder.bind_outer()
            .finally_(
                FinallyStep(id=sid, factory=_AuditFinally(audited=self, admitted=not transactional))
            )
            .finish()
        )

        if not transactional:
            return outer

        return (
            outer.bind_tx()
            .on_success(OnSuccessStep(id=sid, factory=_AuditAdmitted(audited=self)))
            .finish()
        )

    # ....................... #

    def entry(
        self,
        ctx: ExecutionContext,
        args: Any,
        result: Any,
        outcome: AuditOutcome,
        *,
        bare: bool = False,
    ) -> AuditEntry:
        """The row for one operation. Raises when the metadata breaks the allowlist.

        ``bare`` leaves out what the callables would add — metadata and the object reference.
        """

        identity = ctx.inv_ctx.get_authn()
        metadata = self.metadata(args, result) if self.metadata and not bare else {}

        return AuditEntry(
            action=self.spec.action,
            outcome=outcome,
            actor_id=identity.performer_id if identity is not None else None,
            subject_id=identity.principal_id if identity is not None else None,
            actor_ids=identity.actor_ids if identity is not None else (),
            at=utcnow(),
            object_ref=self.object_ref(args, result) if self.object_ref and not bare else None,
            metadata=self.spec.check_metadata(metadata),
        )

    # ....................... #

    def reads_own_data(self, ctx: ExecutionContext, args: Any, result: Any) -> bool:
        identity = ctx.inv_ctx.get_authn()

        if self.owner is None or identity is None or identity.is_delegated:
            return False

        return self.owner(args, result) == identity.principal_id

    # ....................... #

    async def record_admitted(
        self,
        ctx: ExecutionContext,
        port: AuditPort,
        args: Any,
        result: Any,
        *,
        read: bool = False,
    ) -> None:
        """Write an admitted operation's row, applying :attr:`AuditSpec.on_failure`.

        A callable that raises and a store that refuses are both audit failures, under the
        policy. A metadata refusal always raises: it is the over-collecting call site, not an
        outage, and ``"ignore"`` would let it survive to be copied.
        """

        try:
            if read and self.reads_own_data(ctx, args, result):
                return

            await port.record(self.entry(ctx, args, result, AuditOutcome.ALLOWED))

        except Exception as error:
            if self.spec.on_failure == "fail" or _refused(error):
                raise

            logger.warning(
                "audit.write_failed",
                action=self.spec.action,
                outcome=str(AuditOutcome.ALLOWED),
                error=type(error).__name__,
            )


# ....................... #


def _refused(error: Exception) -> bool:
    return isinstance(error, CoreException) and error.code == AUDIT_METADATA_REFUSED


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _AuditAdmitted:
    """Transaction-scope ``on_success``: the admitted row, in the operation's transaction."""

    audited: Audited

    def __call__(self, ctx: ExecutionContext) -> OnSuccess[Any, Any]:
        # Resolved when the operation is, so a missing audit dependency fails at wiring.
        port = ctx.deps.resolve_simple(ctx, AuditDepKey)

        async def _on_success(args: Any, result: Any) -> None:
            # A read runs in a read-only transaction; its row is the outer hook's.
            if ctx.inv_ctx.is_read_only():
                return

            await self.audited.record_admitted(ctx, port, args, result)

        return _on_success


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _AuditFinally:
    """Outer-scope ``finally``: denials, failures, reads, and admitted rows not written in a
    transaction."""

    audited: Audited

    admitted: bool
    """Whether this hook writes an admitted write-operation's row (no transaction step)."""

    def __call__(self, ctx: ExecutionContext) -> Finally[Any, Any]:
        port = ctx.deps.resolve_simple(ctx, AuditDepKey)
        audited = self.audited

        async def _finally(args: Any, outcome: Outcome[Any]) -> None:
            reading = ctx.inv_ctx.is_read_only()

            if isinstance(outcome, Failure):
                # A refused or failed read is not recorded: a log of refused reads is a record
                # of who wanted to see whose data.
                if not reading:
                    await _record_failure(ctx, port, audited, args, outcome.exc)

                return

            if reading:
                if audited.spec.audit_reads == "never":
                    return

            elif not self.admitted:
                return

            await audited.record_admitted(ctx, port, args, outcome.value, read=reading)

        return _finally


async def _record_failure(
    ctx: ExecutionContext,
    port: AuditPort,
    audited: Audited,
    args: Any,
    error: Exception,
) -> None:
    # The operation is already failing with *error*, and this runs in its ``finally``: an
    # exception raised here would replace it. So nothing here raises — a row that cannot be
    # built or written is logged, and the operation's own failure is what the caller sees.
    denied = isinstance(error, CoreException) and error.kind in _DENIALS
    outcome = AuditOutcome.DENIED if denied else AuditOutcome.FAILED

    # One guard around everything, the bare fallback included: it still reads the clock.
    try:
        try:
            entry = audited.entry(ctx, args, None, outcome)

        except Exception as build_error:
            # A metadata callable written for the success path (``result.root_id``) cannot
            # run without a result. The event is what the trail needs from a denial, so it is
            # kept without the metadata rather than lost with it.
            logger.error(
                "audit.metadata_failed",
                action=audited.spec.action,
                outcome=str(outcome),
                error=type(build_error).__name__,
            )
            entry = audited.entry(ctx, args, None, outcome, bare=True)

        await port.record(entry)

    except Exception as audit_error:
        logger.error(
            "audit.write_failed",
            action=audited.spec.action,
            outcome=str(outcome),
            error=type(audit_error).__name__,
        )
