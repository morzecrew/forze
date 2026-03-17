"""Usecase composition plans for middleware ordering and transaction wrapping."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Callable, Final, Iterable, Literal, Self, TypeVar, final

import attrs

from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError
from forze.base.logging import getLogger

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

logger = getLogger(__name__).bind(scope="plan")

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

        logger.trace(
            "Adding middleware spec to bucket {bucket} (priority={priority}, factory_id={factory_id})",
            sub={"bucket": bucket, "priority": spec.priority, "factory_id": id(spec.factory)},
        )

        if not hasattr(self, bucket):
            raise CoreError(f"Invalid bucket: {bucket}")

        cur = getattr(self, bucket)

        with logger.section():
            logger.trace("Current bucket size: {size}", sub={"size": len(cur)})

        return attrs.evolve(self, **{bucket: (*cur, spec)})  # type: ignore[arg-type, misc]

    # ....................... #

    def validate(self) -> None:
        """Validate that in-tx buckets are only used when tx is enabled.

        :raises CoreError: If in-tx or after-commit buckets are used without tx.
        """

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

        If method called on an instance, the instance is merged with the other plans.
        Otherwise only provided plans are merged.

        Deduplicates by priority and factory id, then sorts by priority
        descending (higher first).

        :param bucket: Bucket name.
        :returns: Ordered specs.
        """

        deduped_specs = self.__dedupe(bucket)
        built = self.__sort(deduped_specs, reverse=True)

        return built

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc]
        cls: type[Self],  # pyright: ignore[reportGeneralTypeIssues]
        *plans: Self,
    ) -> OperationPlan:
        """Merge multiple plans into a single aggregate plan.

        :param plans: Plans to merge.
        :returns: A new :class:`OperationPlan` with combined operations.
        """

        acc: OperationPlan = OperationPlan()

        for plan in plans:
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

        return acc

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # pyright: ignore[reportUnusedFunction]
        self: Self,
        *plans: Self,
    ) -> OperationPlan:
        """Merge multiple plans into a single aggregate plan."""

        return type(self).merge(self, *plans)


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
        logger.trace(
            "Adding middleware to usecase plan (op={op}, bucket={bucket}, priority={priority}, factory_id={factory_id})",
            sub={"op": op, "bucket": bucket, "priority": spec.priority, "factory_id": id(spec.factory)},
        )

        with logger.section():
            cur = self._op(op)
            logger.trace("Current operation tx={tx}", sub={"tx": cur.tx})

        return self._put(op, cur.add(bucket, spec))

    # ....................... #

    def tx(self, op: OpKey) -> Self:
        """Enable transaction wrapping for the operation.

        :param op: Operation key.
        :returns: New plan instance.
        """

        logger.trace("Enabling transaction for operation '{op}'", sub={"op": op})
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

        logger.debug("Resolving usecase plan for operation '{op}'", sub={"op": op})

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError(f"Resolve on wildcard operation '{op}' is not allowed")

        with logger.section():
            plan = OperationPlan.merge(self._base(), self._op(op))
            plan.validate()

            outer_before = plan.build("outer_before")
            outer_wrap = plan.build("outer_wrap")
            outer_after = plan.build("outer_after")

            in_tx_before = plan.build("in_tx_before")
            in_tx_wrap = plan.build("in_tx_wrap")
            in_tx_after = plan.build("in_tx_after")

            after_commit = plan.build("after_commit")

            logger.trace(
                "Built plan for '{op}' (tx={tx}, outer_before={outer_before}, outer_wrap={outer_wrap}, outer_after={outer_after}, "
                "in_tx_before={in_tx_before}, in_tx_wrap={in_tx_wrap}, in_tx_after={in_tx_after}, after_commit={after_commit})",
                sub={
                    "op": op,
                    "tx": plan.tx,
                    "outer_before": len(outer_before),
                    "outer_wrap": len(outer_wrap),
                    "outer_after": len(outer_after),
                    "in_tx_before": len(in_tx_before),
                    "in_tx_wrap": len(in_tx_wrap),
                    "in_tx_after": len(in_tx_after),
                    "after_commit": len(after_commit),
                },
            )

            after_commit_effects: list[Effect[Any, Any]] = []

            for s in after_commit:
                mw = s.factory(ctx)

                logger.trace(
                    "Built after_commit middleware {qualname} from factory_id={factory_id}",
                    sub={"qualname": type(mw).__qualname__, "factory_id": id(s.factory)},
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
            logger.trace(
                "Constructed middleware chain with {count} middleware(s)",
                sub={"count": len(chain)},
            )

            uc = factory(ctx)
            resolved = uc.with_middlewares(*chain)

        return resolved

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc]
        cls: type[Self],  # pyright: ignore[reportGeneralTypeIssues]
        *plans: Self,
    ) -> Self:
        """Merge multiple plans into a single aggregate plan.

        If method called on an instance, the instance is merged with the other plans.
        Otherwise only provided plans are merged.

        For each operation key, merges the corresponding :class:`OperationPlan`
        instances. Base (wildcard) and op-specific plans are combined per op.

        :param plans: Plans to merge.
        :returns: Merged plan.
        """

        acc: dict[str, OperationPlan] = {}

        for p in plans:
            for op, pl in p.ops.items():
                cur = acc.get(op, OperationPlan())
                acc[op] = cur.merge(pl)

        return cls(ops=acc)

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # pyright: ignore[reportUnusedFunction]
        self: Self,
        *plans: Self,
    ) -> Self:
        """Merge multiple plans into a single aggregate plan.

        :param plans: Plans to merge.
        :returns: Merged plan.
        """

        return type(self).merge(self, *plans)
