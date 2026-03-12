"""Usecase composition plans for middleware ordering and transaction wrapping.

Provides :class:`UsecasePlan` (per-operation middleware composition),
:class:`OperationPlan` (buckets for before/wrap/after, in-tx, after-commit),
and :class:`MiddlewareSpec` (priority + factory). Plans are merged and resolved
into composed usecases via :class:`UsecaseRegistry`.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import Any, Callable, Final, Iterable, Literal, Self, TypeVar, final

import attrs
from pydantic import BaseModel, Field

from forze.base.errors import CoreError
from forze.base.introspection import get_callable_module, get_callable_name
from forze.base.logging import log_section

from .context import ExecutionContext
from .middleware import (
    Effect,
    EffectMiddleware,
    Guard,
    GuardMiddleware,
    Middleware,
    TxMiddleware,
)
from .usecase import Usecase

# ----------------------- #

logger = logging.getLogger(__name__)

# ....................... #

U = TypeVar("U", bound=Usecase[Any, Any])

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that builds a guard from execution context."""

EffectFactory = Callable[[ExecutionContext], Effect[Any, Any]]
"""Factory that builds an effect from execution context."""

MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""

OpKey = str | StrEnum
"""Operation identifier (string or enum)."""

WILDCARD: Final[str] = "*"
"""Wildcard operation key for default/fallback plans."""

PlanBucket = Literal[
    "outer_before",
    "outer_wrap",
    "outer_after",
    "in_tx_before",
    "in_tx_wrap",
    "in_tx_after",
    "after_commit",
]
"""Bucket names for middleware placement in the chain."""

# ....................... #


class _ExplainItem(BaseModel):
    """Single entry in a plan explanation, representing one middleware slot."""

    bucket: PlanBucket
    priority: int
    factory: str
    factory_id: int


class _Explain(BaseModel):
    """Human-readable explanation of a resolved plan for a single operation."""

    op: str
    tx: bool
    chain: list[_ExplainItem] = Field(default_factory=list)
    after_commit: list[_ExplainItem] = Field(default_factory=list)

    # ....................... #

    def pretty_format(self) -> str:
        lines: list[str] = []
        lines.append(f"UsecasePlan explain for operation `{self.op}` (tx={self.tx})")
        lines.append("Chain (outer -> inner):")

        for i, item in enumerate(self.chain, 1):
            lines.append(
                f"  {i:02d}. {item.bucket:12s} prio={item.priority:6d} "
                f"factory={item.factory} id={item.factory_id}"
            )

        if self.after_commit:
            lines.append("After-commit effects (run after successful commit):")

            for i, item in enumerate(self.after_commit, 1):
                lines.append(
                    f"  {i:02d}. {item.bucket:12s} prio={item.priority:6d} "
                    f"factory={item.factory} id={item.factory_id}"
                )

        else:
            lines.append("After-commit effects: <none>")

        return "\n".join(lines)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareSpec:
    """Specification for a middleware attached to an operation plan.

    Middlewares are ordered by ``priority`` (descending) and created lazily from a
    :class:`ExecutionContext` when a plan is resolved.
    """

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    factory: MiddlewareFactory


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Per-operation middleware composition with transaction support.

    Buckets: ``outer_*`` run outside tx; ``in_tx_*`` inside
    :class:`TxMiddleware`; ``after_commit`` runs after successful commit.
    When ``tx`` is ``True``, in-tx and after-commit buckets are used.
    """

    tx: bool = False
    """Whether the operation runs inside a transaction."""

    # outer
    outer_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects before the transaction (if any)."""

    outer_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Wrapping middlewares outside the transaction."""

    outer_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects after the transaction."""

    # in tx
    in_tx_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects inside the transaction, before the usecase."""

    in_tx_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Wrapping middlewares inside the transaction."""

    in_tx_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects inside the transaction, after the usecase."""

    after_commit: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Effects to run after successful commit."""

    # ....................... #

    def add(
        self,
        bucket: PlanBucket,
        spec: MiddlewareSpec,
    ) -> Self:
        """Add a middleware spec to a bucket.

        :param bucket: Bucket name.
        :param spec: Middleware spec.
        :returns: New plan instance.
        :raises CoreError: If bucket is invalid.
        """

        logger.debug(
            "Adding middleware spec to bucket %s (priority=%s, factory_id=%s)",
            bucket,
            spec.priority,
            id(spec.factory),
        )

        if not hasattr(self, bucket):
            raise CoreError(f"Invalid bucket: {bucket}")

        cur = getattr(self, bucket)

        with log_section():
            logger.debug("Current bucket size: %d", len(cur))

        return attrs.evolve(self, **{bucket: (*cur, spec)})  # type: ignore[arg-type, misc]

    # ....................... #

    def validate(self) -> None:
        """Validate that in-tx buckets are only used when tx is enabled.

        :raises CoreError: If in-tx or after-commit buckets are used without tx.
        """

        logger.debug("Validating operation plan (tx=%s)", self.tx)

        with log_section():
            logger.debug(
                "Bucket sizes: outer_before=%d outer_wrap=%d outer_after=%d "
                "in_tx_before=%d in_tx_wrap=%d in_tx_after=%d after_commit=%d",
                len(self.outer_before),
                len(self.outer_wrap),
                len(self.outer_after),
                len(self.in_tx_before),
                len(self.in_tx_wrap),
                len(self.in_tx_after),
                len(self.after_commit),
            )

            if (
                self.in_tx_before
                or self.in_tx_after
                or self.in_tx_wrap
                or self.after_commit
            ) and not self.tx:
                raise CoreError(
                    "Operation plan uses IN_TX_* middlewares but tx() is not enabled"
                )

    # ....................... #

    def __ensure_no_collisions(
        self,
        specs: Iterable[MiddlewareSpec],
        *,
        bucket: PlanBucket,
    ) -> None:
        used: set[int] = set()

        for s in specs:
            k = s.priority
            if k in used:
                raise CoreError(
                    f"Priority collision in bucket '{bucket}': {s.priority}"
                )

            used.add(k)

    # ....................... #

    def __dedupe(self, bucket: PlanBucket) -> tuple[MiddlewareSpec, ...]:
        if not hasattr(self, bucket):
            raise CoreError(f"Invalid bucket: {bucket}")

        cur = getattr(self, bucket)
        seen: set[tuple[int, int]] = set()
        out: list[MiddlewareSpec] = []

        for s in cur:
            k = (id(s.factory), s.priority)

            if k in seen:
                continue

            seen.add(k)
            out.append(s)

        self.__ensure_no_collisions(out, bucket=bucket)

        return tuple(out)

    # ....................... #

    def __sort(
        self,
        specs: Iterable[MiddlewareSpec],
        *,
        reverse: bool,
    ) -> tuple[MiddlewareSpec, ...]:
        return tuple(sorted(specs, key=lambda s: s.priority, reverse=reverse))

    # ....................... #

    def build(self, bucket: PlanBucket) -> tuple[MiddlewareSpec, ...]:
        """Build the ordered middleware specs for a bucket.

        Deduplicates by priority and factory id, then sorts by priority
        descending (higher first).

        :param bucket: Bucket name.
        :returns: Ordered specs.
        """

        deduped_specs = self.__dedupe(bucket)
        built = self.__sort(deduped_specs, reverse=True)

        logger.debug(
            "Built bucket %s with %d spec(s) priorities=%s",
            bucket,
            len(built),
            tuple(s.priority for s in built),
        )

        return built

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self) -> OperationPlan:
        """Merge multiple plans into a single aggregate plan.

        :param plans: Plans to merge.
        :returns: A new :class:`OperationPlan` with combined operations.
        """

        logger.debug("Merging %d operation plan(s)", len(plans))

        with log_section():
            acc: OperationPlan = OperationPlan()

            for i, plan in enumerate(plans, 1):
                logger.debug(
                    "Merging plan #%d (tx=%s, outer_before=%d, outer_wrap=%d, outer_after=%d, "
                    "in_tx_before=%d, in_tx_wrap=%d, in_tx_after=%d, after_commit=%d)",
                    i,
                    plan.tx,
                    len(plan.outer_before),
                    len(plan.outer_wrap),
                    len(plan.outer_after),
                    len(plan.in_tx_before),
                    len(plan.in_tx_wrap),
                    len(plan.in_tx_after),
                    len(plan.after_commit),
                )

                acc = OperationPlan(
                    tx=acc.tx or plan.tx,
                    outer_before=(*acc.outer_before, *plan.outer_before),
                    outer_wrap=(*acc.outer_wrap, *plan.outer_wrap),
                    outer_after=(*acc.outer_after, *plan.outer_after),
                    in_tx_before=(*acc.in_tx_before, *plan.in_tx_before),
                    in_tx_wrap=(*acc.in_tx_wrap, *plan.in_tx_wrap),
                    in_tx_after=(*acc.in_tx_after, *plan.in_tx_after),
                    after_commit=(*acc.after_commit, *plan.after_commit),
                )

            logger.debug("Merged operation plan tx=%s", acc.tx)

        return acc


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    """Declarative plan for composing usecases per operation.

    Maps operation keys to :class:`OperationPlan`. Use ``*`` (wildcard) for
    defaults applied to all operations. :meth:`resolve` merges base and
    op-specific plans, then builds the middleware chain.
    """

    ops: dict[str, OperationPlan] = attrs.field(factory=dict)
    """Operation key to plan mapping."""

    # ....................... #
    # Helpers

    def _base(self) -> OperationPlan:
        return self.ops.get(WILDCARD, OperationPlan())

    def _op(self, op: OpKey) -> OperationPlan:
        return self.ops.get(str(op), OperationPlan())

    def _put(self, op: OpKey, plan: OperationPlan) -> Self:
        new_ops = dict(self.ops)
        new_ops[str(op)] = plan

        return attrs.evolve(self, ops=new_ops)

    def _add(self, op: OpKey, bucket: PlanBucket, spec: MiddlewareSpec) -> Self:
        logger.debug(
            "Adding middleware to usecase plan (op=%s, bucket=%s, priority=%s, factory_id=%s)",
            op,
            bucket,
            spec.priority,
            id(spec.factory),
        )

        with log_section():
            cur = self._op(op)
            logger.debug("Current operation tx=%s", cur.tx)

        return self._put(op, cur.add(bucket, spec))

    # ....................... #

    def tx(self, op: OpKey) -> Self:
        """Enable transaction wrapping for the operation.

        :param op: Operation key.
        :returns: New plan instance.
        """

        logger.debug("Enabling transaction for operation %s", op)
        cur = self._op(op)

        return self._put(op, attrs.evolve(cur, tx=True))

    # ....................... #

    def before(self, op: OpKey, guard: GuardFactory, *, priority: int = 0) -> Self:
        def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        return self._add(
            op,
            "outer_before",
            MiddlewareSpec(factory=factory, priority=priority),
        )

    # ....................... #

    def after(self, op: OpKey, effect: EffectFactory, *, priority: int = 0) -> Self:
        def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
            return EffectMiddleware[Any, Any](effect=effect(ctx))

        return self._add(
            op,
            "outer_after",
            MiddlewareSpec(factory=factory, priority=priority),
        )

    # ....................... #

    def wrap(
        self,
        op: OpKey,
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        return self._add(
            op,
            "outer_wrap",
            MiddlewareSpec(factory=middleware, priority=priority),
        )

    # ....................... #

    def in_tx_before(
        self,
        op: OpKey,
        guard: GuardFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        return self._add(
            op,
            "in_tx_before",
            MiddlewareSpec(factory=factory, priority=priority),
        )

    # ....................... #

    def in_tx_after(
        self,
        op: OpKey,
        effect: EffectFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
            return EffectMiddleware[Any, Any](effect=effect(ctx))

        return self._add(
            op,
            "in_tx_after",
            MiddlewareSpec(factory=factory, priority=priority),
        )

    # ....................... #

    def in_tx_wrap(
        self,
        op: OpKey,
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        return self._add(
            op,
            "in_tx_wrap",
            MiddlewareSpec(factory=middleware, priority=priority),
        )

    # ....................... #

    def after_commit(
        self,
        op: OpKey,
        effect: EffectFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
            return EffectMiddleware[Any, Any](effect=effect(ctx))

        return self._add(
            op,
            "after_commit",
            MiddlewareSpec(factory=factory, priority=priority),
        )

    # ....................... #

    def resolve(
        self,
        op: OpKey,
        ctx: ExecutionContext,
        factory: Callable[[ExecutionContext], U],
    ) -> U:
        """Build a composed usecase instance for an operation.

        Merges base (wildcard) and op-specific plans, validates, builds the
        middleware chain, and wraps the factory result.

        :param op: Operation key (wildcard not allowed).
        :param ctx: Execution context for factory resolution.
        :param factory: Usecase factory.
        :returns: Composed usecase with middlewares.
        :raises CoreError: If op is wildcard or plan is invalid.
        """
        op = str(op)

        logger.debug(
            "Resolving usecase plan for operation %s with context %s",
            op,
            type(ctx).__qualname__,
        )

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError(f"Resolve on wildcard operation `{op}` is not allowed")

        with log_section():
            plan = OperationPlan.merge(self._base(), self._op(op))
            plan.validate()

            outer_before = plan.build("outer_before")
            outer_wrap = plan.build("outer_wrap")
            outer_after = plan.build("outer_after")

            in_tx_before = plan.build("in_tx_before")
            in_tx_wrap = plan.build("in_tx_wrap")
            in_tx_after = plan.build("in_tx_after")

            after_commit = plan.build("after_commit")

            logger.debug(
                "Built plan for %s: tx=%s outer_before=%d outer_wrap=%d outer_after=%d "
                "in_tx_before=%d in_tx_wrap=%d in_tx_after=%d after_commit=%d",
                op,
                plan.tx,
                len(outer_before),
                len(outer_wrap),
                len(outer_after),
                len(in_tx_before),
                len(in_tx_wrap),
                len(in_tx_after),
                len(after_commit),
            )

            after_commit_effects: list[Effect[Any, Any]] = []

            for s in after_commit:
                mw = s.factory(ctx)

                logger.debug(
                    "Built after_commit middleware %s from factory_id=%s",
                    type(mw).__qualname__,
                    id(s.factory),
                )

                if not isinstance(mw, EffectMiddleware):
                    raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

                after_commit_effects.append(mw.effect)

            chain: list[Middleware[Any, Any]] = []

            chain.extend(s.factory(ctx) for s in outer_before)
            chain.extend(s.factory(ctx) for s in outer_wrap)

            if plan.tx:
                chain.append(
                    TxMiddleware[Any, Any](ctx=ctx).with_after_commit(
                        *after_commit_effects
                    )
                )
                chain.extend(s.factory(ctx) for s in in_tx_before)
                chain.extend(s.factory(ctx) for s in in_tx_wrap)
                chain.extend(s.factory(ctx) for s in in_tx_after)

            chain.extend(s.factory(ctx) for s in outer_after)
            logger.debug(
                "Constructed middleware chain with %d middleware(s)", len(chain)
            )

            uc = factory(ctx)
            logger.debug("Built usecase instance %s", type(uc).__qualname__)

            resolved = uc.with_middlewares(*chain)
            logger.debug("Applied middleware chain to %s", type(uc).__qualname__)

        return resolved

    # ....................... #
    #! Redundant method if we have logging with indentation

    def explain(self, op: OpKey) -> _Explain:
        """Return a human-readable explanation of the middleware chain for an op.

        :param op: Operation key.
        :returns: Explain object with pretty_format.
        :raises CoreError: If op is wildcard.
        """
        op = str(op)

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError(f"Explain on wildcard operation `{op}` is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op))
        plan.validate()

        def pack(
            bucket: PlanBucket,
            specs: Iterable[MiddlewareSpec],
        ) -> list[_ExplainItem]:
            out: list[_ExplainItem] = []

            for s in specs:
                mod = get_callable_module(s.factory)
                name = get_callable_name(s.factory)

                out.append(
                    _ExplainItem(
                        bucket=bucket,
                        priority=s.priority,
                        factory=f"{mod}:{name}",
                        factory_id=id(s.factory),
                    )
                )

            return out

        outer_before = plan.build("outer_before")
        outer_wrap = plan.build("outer_wrap")
        outer_after = plan.build("outer_after")

        in_tx_before = plan.build("in_tx_before")
        in_tx_wrap = plan.build("in_tx_wrap")
        in_tx_after = plan.build("in_tx_after")

        after_commit = plan.build("after_commit")

        chain: list[_ExplainItem] = []
        chain.extend(pack("outer_before", outer_before))
        chain.extend(pack("outer_wrap", outer_wrap))

        if plan.tx:
            chain.extend(pack("in_tx_before", in_tx_before))
            chain.extend(pack("in_tx_wrap", in_tx_wrap))
            chain.extend(pack("in_tx_after", in_tx_after))

        chain.extend(pack("outer_after", outer_after))

        return _Explain(
            op=op,
            tx=plan.tx,
            chain=chain,
            after_commit=pack("after_commit", after_commit),
        )

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self) -> Self:
        """Merge multiple plans into a single aggregate plan.

        For each operation key, merges the corresponding :class:`OperationPlan`
        instances. Base (wildcard) and op-specific plans are combined per op.

        :param plans: Plans to merge.
        :returns: Merged plan.
        """

        logger.debug("Merging %d usecase plan(s)", len(plans))

        with log_section():
            acc: dict[str, OperationPlan] = {}

            for i, p in enumerate(plans, 1):
                logger.debug("Merging usecase plan #%d with %d op(s)", i, len(p.ops))

                for op, pl in p.ops.items():
                    logger.debug("Merging operation %s", op)

                    cur = acc.get(op, OperationPlan())
                    acc[op] = OperationPlan.merge(cur, pl)

                logger.debug("Merged usecase plan contains %d op(s)", len(acc))

        return cls(ops=acc)
