import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableFunctionSpec
from forze.application.contracts.execution import Handler, OnSuccess, TwoPhaseHandler
from forze.application.contracts.transaction import AfterCommitPort
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey

from ...context.active_operation import active_operation_var
from ...context.commit_state import commit_started, reset_commit_started
from ...context.deadline import remaining_time, reset_deadline, set_deadline
from ...context.drain import OperationDrainGate
from ...tracing.emit import record
from ..planning.plans import OperationKind, ResolvedOperationPlan
from .plan import TransactionRunner, run_resolved_operation_plan

if TYPE_CHECKING:
    from ...context import ExecutionContext
    from ...context.invocation import InvocationContext
    from ..registry import FrozenOperationRegistry

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedOperation[Args, R](Handler[Args, R]):
    """Resolved operation."""

    op: StrKey
    """Operation key."""

    handler: Handler[Args, R] | TwoPhaseHandler[Args, Any, R]
    """Resolved handler — a plain ``Handler`` or, for a two-phase operation, a
    ``TwoPhaseHandler``. ``run_resolved_operation_plan`` branches on ``plan.two_phase``."""

    plan: ResolvedOperationPlan
    """Resolved operation plan."""

    tx_runner: TransactionRunner
    """Opens a transaction scope on a route (optionally read-only)."""

    defer_after_commit: AfterCommitPort
    """Defer work until after a successful root transaction commit."""

    inv_ctx: "InvocationContext"
    """Invocation context — used to bind the read-only flag for a QUERY operation."""

    drain_gate: OperationDrainGate
    """Admits top-level invocations; rejects new ones while the scope drains."""

    # ....................... #

    async def _run(self, args: Args) -> R:
        # Deadline enforcement: the plan's declared budget (if any) is bound
        # first — tighten-only, so a caller-bound deadline can shorten it but
        # never extend past the plan's cap — then the effective deadline (see
        # ``context.deadline``) bounds the whole plan: hooks, transaction,
        # dispatch. No deadline anywhere is the hot path: one attribute and
        # one ContextVar read, no timeout machinery. The post-commit drain is
        # cancellation-protected in ``TransactionContext.scope``, so a
        # deadline firing mid-drain still lets the drain finish before the
        # timeout surfaces here.
        plan_budget = self.plan.deadline_s
        token = None if plan_budget is None else set_deadline(plan_budget)

        try:
            remaining = remaining_time()

            if remaining is None:
                return await run_resolved_operation_plan(
                    self.plan,
                    self.handler,
                    args,
                    tx_runner=self.tx_runner,
                    defer_after_commit=self.defer_after_commit,
                    inv_ctx=self.inv_ctx,
                )

            if remaining <= 0.0:
                raise exc.timeout(
                    f"Invocation deadline exceeded before operation {str(self.op)!r} started",
                    code="deadline_exceeded",
                )

            try:
                async with asyncio.timeout(remaining):
                    return await run_resolved_operation_plan(
                        self.plan,
                        self.handler,
                        args,
                        tx_runner=self.tx_runner,
                        defer_after_commit=self.defer_after_commit,
                        inv_ctx=self.inv_ctx,
                    )

            except TimeoutError as error:
                if commit_started():
                    # The deadline fired at or after the transaction commit point:
                    # the commit may have (or has) landed, so the outcome is
                    # ambiguous. Surface a non-retryable error instead of a
                    # retryable deadline, so an at-least-once caller does not retry
                    # into a duplicate execution.
                    raise exc.internal(
                        f"Operation {str(self.op)!r} deadline fired at or after its "
                        "transaction commit; the commit outcome is ambiguous (it may "
                        "have committed). Surfaced as non-retryable so a retry cannot "
                        "double-execute — reconcile before re-running.",
                        code="commit_ambiguous",
                    ) from error

                raise exc.timeout(
                    f"Operation {str(self.op)!r} exceeded the invocation deadline",
                    code="deadline_exceeded",
                ) from error

        finally:
            if token is not None:
                reset_deadline(token)

    # ....................... #

    async def __call__(self, args: Args) -> R:
        """Call the operation.

        A QUERY operation runs under a read-only flag, so a command (write) port cannot be
        acquired for its duration (enforced in ``ConvenientDeps._resolve_command``).
        Execution is marked via the module-level active-operation flag so that
        constructing an :class:`ExecutionContext` mid-operation (per-request
        creation, an unsupported mode) can be detected and warned about.

        A **top-level** invocation (no operation already active on this task)
        is admitted through the scope's drain gate: rejected with ``THROTTLED``
        (``code="draining"``) once the runtime is draining, counted in flight
        otherwise. An admitted operation's own dispatch chains ride its slot —
        genuine in-await nested dispatch, and engine-internal task hops that
        adopt the operation explicitly (see below) — so draining never starves
        an admitted operation of them.

        Nesting is decided by **task identity**, not by the marker's mere
        presence: the marker is a ContextVar, so a task a handler spawns
        (``asyncio.create_task(facade.run(...))``) inherits it despite being a
        distinct, detached task. Only a marker owned by the *current* task counts
        as nested; an inherited marker on a different task is treated as the
        fresh top-level driver it is, so the spawned operation is admitted,
        counted in flight, and its task tracked by the gate — otherwise it would
        escape drain and run on against the clients teardown is closing.

        The engine's own machinery hops tasks *within* one admitted operation —
        the two-phase ``prepare`` task, hedged attempts, the post-commit
        callback runner, concurrent graph-wave steps. Each such spawn site wraps
        its payload in
        :func:`~forze.application.execution.context.active_operation.continue_operation_on_task`,
        re-stamping the marker onto the new task, so a dispatch made there is
        recognized as nested here. The adoption is explicit at the spawn site
        (never ambient) precisely so user-spawned tasks stay classified as fresh
        top-level drivers.

        Hot path: both flags are token set/reset directly (the equivalent of the
        :func:`~forze.application.execution.context.active_operation.operation_running`
        and ``InvocationContext.bind_read_only`` context managers) — a
        ``@contextmanager`` enter/exit costs ~5x a raw ContextVar set/reset pair.
        """

        # Nested only when the enclosing operation runs on THIS task. A marker
        # inherited via a copied context on a spawned task belongs to a different
        # task, so it is a fresh top-level driver and must be gated (counted /
        # tracked / rejectable), never mistaken for in-await nesting.
        current_task = asyncio.current_task()
        owner_task = active_operation_var.get()
        nested = current_task is not None and owner_task is current_task
        gate = None if nested else self.drain_gate

        if gate is not None:
            gate.admit(self.op)
            # Top-level entry: clear any stale commit-reached flag left by a root
            # transaction that committed *outside* an operation boundary (e.g. the inbox
            # consumer's per-message tx, cross-aggregate invariant enforcement), so it
            # cannot misclassify this operation's body-timeout as ``commit_ambiguous``.
            # Nested invocations (gate is None) skip this so a nested commit still
            # propagates to the top-level boundary.
            reset_commit_started()

        marker_token = active_operation_var.set(current_task if current_task is not None else True)

        try:
            if self.plan.kind is OperationKind.QUERY:
                ro_token = self.inv_ctx.set_read_only()

                try:
                    return await self._run(args)

                finally:
                    self.inv_ctx.reset_read_only(ro_token)

            return await self._run(args)

        finally:
            active_operation_var.reset(marker_token)

            if gate is not None:
                gate.release()
                # Top-level invocation only: clear the commit-reached flag so a
                # committed operation cannot leak a false ``commit_ambiguous`` onto a
                # later top-level operation sharing this task's context. Nested
                # invocations leave it set so a nested commit still reaches the
                # top-level boundary.
                reset_commit_started()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DispatchedOperation[Args, R](OnSuccess[Args, R]):
    """Resolved operation dispatcher."""

    resolved: ResolvedOperation[Any, Any]
    """Resolved operation."""

    mapper: Callable[[Args, R], Any]
    """Mapper function to transform the result of the target operation."""

    # ....................... #

    async def __call__(self, args: Args, result: R) -> None:
        """Call the operation dispatcher."""

        op_args = self.mapper(args, result)

        return await self.resolved(op_args)


# ....................... #


async def run_operation(
    registry: "FrozenOperationRegistry",
    op: StrKey,
    args: Any,
    ctx: "ExecutionContext",
) -> Any:
    """Run an operation from a frozen registry (resolve + full plan)."""

    resolved = registry.resolve(op, ctx)

    # A cascade (this op invoked from inside another operation — a saga or event handler) has no
    # top-level driver; the marker is already set by the enclosing invocation. Capturing it before
    # ``resolved`` runs (which sets the marker for *this* op) lets a trace consumer attribute the
    # invoke correctly. The invoke's ``seq`` becomes the correlation id its terminal carries back,
    # so concurrent calls of the same op are paired exactly rather than per-op FIFO.
    nested = bool(active_operation_var.get())
    invoke_seq = record(
        domain="operation", op=str(op), phase="invoke", nested=nested, deps=ctx.deps
    )
    try:
        result = await resolved(args)
    except Exception as error:
        # Classify the failure for the trace: a declared domain failure (a CoreException —
        # an expected, handled outcome) is recorded as ``failed``, while any other exception
        # is an unhandled bug, recorded as ``error``. This makes the runtime trace the single
        # source of truth for the domain-failure-vs-bug distinction (consumed e.g. by DST's
        # ``no_unexpected_error``), with no separate classification needed downstream.
        record(
            domain="operation",
            op=str(op),
            phase="error",
            outcome="failed" if isinstance(error, CoreException) else "error",
            error=type(error).__name__,
            corr=invoke_seq,
            deps=ctx.deps,
        )
        raise

    record(
        domain="operation",
        op=str(op),
        phase="complete",
        outcome="ok",
        corr=invoke_seq,
        deps=ctx.deps,
    )
    return result


# ....................... #


def handler_for_registry_operation(
    registry: "FrozenOperationRegistry",
    operation: StrKey,
) -> Callable[["ExecutionContext"], Handler[Any, Any]]:
    """Return a factory that yields a resolved operation (full plan) for *operation*."""

    def factory(ctx: "ExecutionContext") -> Handler[Any, Any]:
        return registry.resolve(operation, ctx)

    return factory


# ....................... #


async def run_durable_function(
    spec: DurableFunctionSpec[Any, Any],
    registry: "FrozenOperationRegistry",
    ctx: "ExecutionContext",
    args: Any,
) -> Any:
    """Run a durable function backed by :attr:`DurableFunctionSpec.operation`."""

    if spec.operation is None:
        raise exc.configuration(
            "DurableFunctionSpec.operation is required for registry-backed runs",
        )

    return await run_operation(registry, spec.operation, args, ctx)


# ....................... #


async def run_durable_function_typed[SpecIn: BaseModel, SpecOut: BaseModel](
    spec: DurableFunctionSpec[SpecIn, SpecOut],
    registry: "FrozenOperationRegistry",
    ctx: "ExecutionContext",
    args: SpecIn,
) -> SpecOut:
    """Typed wrapper around :func:`run_durable_function`."""

    return cast(SpecOut, await run_durable_function(spec, registry, ctx, args))
