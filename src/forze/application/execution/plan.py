"""Usecase composition plans for middleware ordering and transaction wrapping."""

from __future__ import annotations

from enum import StrEnum
from typing import (
    Any,
    Callable,
    Final,
    Iterable,
    Literal,
    Self,
    Sequence,
    TypeVar,
    final,
)

import attrs

from forze.application._logger import logger
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError

from .context import ExecutionContext
from .middleware import (
    Effect,
    EffectMiddleware,
    Finally,
    FinallyMiddleware,
    Guard,
    GuardMiddleware,
    Middleware,
    OnFailure,
    OnFailureMiddleware,
    TxMiddleware,
)
from .usecase import Usecase

# ----------------------- #
#! TODO: Consider replacement of CoreError to RuntimeError

U = TypeVar("U", bound=Usecase[Any, Any])

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that builds a guard from execution context."""

EffectFactory = Callable[[ExecutionContext], Effect[Any, Any]]
"""Factory that builds an effect from execution context."""

FinallyFactory = Callable[[ExecutionContext], Finally[Any, Any]]
"""Factory that builds a finally hook from execution context."""

OnFailureFactory = Callable[[ExecutionContext], OnFailure[Any]]
"""Factory that builds an on-failure hook from execution context."""

MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""

OpKey = str | StrEnum
"""Operation identifier (string or enum)."""

WILDCARD: Final[str] = "*"
"""Wildcard operation key for default/fallback plans."""

PlanBucket = Literal[
    "outer_before",
    "outer_wrap",
    "outer_finally",
    "outer_on_failure",
    "outer_after",
    "in_tx_before",
    "in_tx_finally",
    "in_tx_on_failure",
    "in_tx_wrap",
    "in_tx_after",
    "after_commit",
]
"""Bucket names for middleware placement in the chain."""

_EFFECT_OR_WRAP_BUCKETS_REVERSED_IN_USECASE_TUPLE: Final[frozenset[PlanBucket]] = (
    frozenset(
        {
            "outer_wrap",
            "outer_finally",
            "outer_on_failure",
            "in_tx_finally",
            "in_tx_on_failure",
            "in_tx_wrap",
            "outer_after",
            "in_tx_after",
        }
    )
)
"""Buckets whose :meth:`OperationPlan.build` order is reversed when appending to
:class:`Usecase` so that higher priority always means "runs first" in the logical
sense: first guard on the way in, first effect after ``main``, first wrap entry
innermost. ``after_commit`` is not included; it runs in ``build`` order (see
:class:`TxMiddleware`).
"""

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
class TransactionSpec:
    """Specification for a transaction attached to an operation plan."""

    route: str | StrEnum
    """Routing key for the transaction."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Per-operation middleware composition with transaction support.

    Buckets: ``outer_*`` run outside :class:`TxMiddleware`; ``outer_finally`` and
    ``outer_on_failure`` are placed after ``outer_wrap`` and wrap the
    transactional segment (or core usecase when tx is disabled). ``in_tx_*``
    run inside the transaction scope. ``after_commit`` runs only after a
    successful commit.
    """

    tx: TransactionSpec | None = attrs.field(default=None)
    """Transaction spec for the operation. None means non-transactional."""

    # outer
    outer_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects before the transaction (if any)."""

    outer_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Wrapping middlewares outside the transaction."""

    outer_finally: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Finally hooks outside the transaction (wrap failed or successful tx scope)."""

    outer_on_failure: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """On-failure hooks outside the transaction (after rollback when tx is used)."""

    outer_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects after the transaction."""

    # in tx
    in_tx_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Guards/effects inside the transaction, before the usecase."""

    in_tx_finally: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """Finally hooks inside the transaction (before commit/rollback completes)."""

    in_tx_on_failure: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    """On-failure hooks inside the transaction."""

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
            "Adding middleware spec to bucket '%s' (priority=%s, factory_id=%s)",
            bucket,
            spec.priority,
            id(spec.factory),
        )

        if not hasattr(self, bucket):
            raise CoreError(f"Invalid bucket: {bucket}")

        cur = getattr(self, bucket)

        logger.trace("Current bucket size: %s", len(cur))

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
            or self.in_tx_finally
            or self.in_tx_on_failure
            or self.after_commit
        ) and self.tx is None:
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
        descending (higher first). :meth:`UsecasePlan.resolve` may reverse that
        order for effect and wrap buckets when constructing the flat
        :attr:`Usecase.middlewares` tuple; see
        ``_EFFECT_OR_WRAP_BUCKETS_REVERSED_IN_USECASE_TUPLE``.

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
                outer_finally=(*acc.outer_finally, *plan.outer_finally),
                outer_on_failure=(*acc.outer_on_failure, *plan.outer_on_failure),
                outer_after=(*acc.outer_after, *plan.outer_after),
                in_tx_before=(*acc.in_tx_before, *plan.in_tx_before),
                in_tx_finally=(*acc.in_tx_finally, *plan.in_tx_finally),
                in_tx_on_failure=(*acc.in_tx_on_failure, *plan.in_tx_on_failure),
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


def _middleware_specs_for_usecase_tuple(
    plan: OperationPlan, bucket: PlanBucket
) -> tuple[MiddlewareSpec, ...]:
    """Map :meth:`OperationPlan.build` output to `Usecase.middlewares` segment order."""

    built = plan.build(bucket)
    if bucket in _EFFECT_OR_WRAP_BUCKETS_REVERSED_IN_USECASE_TUPLE:
        return tuple(reversed(built))
    return built


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
            "Adding middleware to usecase plan (op=%s, bucket=%s, priority=%s, factory_id=%s)",
            op,
            bucket,
            spec.priority,
            id(spec.factory),
        )

        cur = self._op(op)
        logger.trace("Current operation tx=%s", cur.tx)

        return self._put(op, cur.add(bucket, spec))

    # ....................... #

    def tx(self, op: OpKey | list[OpKey], *, route: str | StrEnum) -> Self:
        """Enable transaction wrapping for the operation.

        :param op: Operation key.
        :param route: Routing key for the transaction.
        :returns: New plan instance.
        """

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            logger.trace("Enabling transaction for operation '%s' (route=%s)", o, route)
            cur = out._op(o)

            out = out._put(o, attrs.evolve(cur, tx=TransactionSpec(route=route)))

        return out

    # ....................... #

    def no_tx(self, op: OpKey | list[OpKey]) -> Self:
        """Disable transaction wrapping for the operation.

        :param op: Operation key.
        :returns: New plan instance.
        """

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            logger.trace("Disabling transaction for operation '%s'", o)
            cur = out._op(o)

            out = out._put(o, attrs.evolve(cur, tx=None))

        return out

    # ....................... #

    def before(
        self,
        op: OpKey | list[OpKey],
        guard: GuardFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o, "outer_before", MiddlewareSpec(factory=factory, priority=priority)
            )

        return out

    # ....................... #

    def before_pipeline(
        self,
        op: OpKey | list[OpKey],
        guards: Sequence[GuardFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, guard in enumerate(guards):
            priority = first_priority - i * 10
            out = out.before(op, guard, priority=priority)

        return out

    # ....................... #

    def after(
        self, op: OpKey | list[OpKey], effect: EffectFactory, *, priority: int = 0
    ) -> Self:
        def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
            return EffectMiddleware[Any, Any](effect=effect(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o, "outer_after", MiddlewareSpec(factory=factory, priority=priority)
            )

        return out

    # ....................... #

    def after_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[EffectFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, effect in enumerate(effects):
            priority = first_priority - i * 10
            out = out.after(op, effect, priority=priority)

        return out

    # ....................... #

    def wrap(
        self,
        op: OpKey | list[OpKey],
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "outer_wrap",
                MiddlewareSpec(factory=middleware, priority=priority),
            )

        return out

    # ....................... #

    def wrap_pipeline(
        self,
        op: OpKey | list[OpKey],
        middlewares: Sequence[MiddlewareFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, middleware in enumerate(middlewares):
            priority = first_priority - i * 10
            out = out.wrap(op, middleware, priority=priority)

        return out

    # ....................... #

    def outer_finally(
        self,
        op: OpKey | list[OpKey],
        hook: FinallyFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> FinallyMiddleware[Any, Any]:
            return FinallyMiddleware[Any, Any](hook=hook(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "outer_finally",
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def outer_finally_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[FinallyFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.outer_finally(op, hook, priority=priority)

        return out

    # ....................... #

    def outer_on_failure(
        self,
        op: OpKey | list[OpKey],
        hook: OnFailureFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> OnFailureMiddleware[Any, Any]:
            return OnFailureMiddleware[Any, Any](hook=hook(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "outer_on_failure",
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def outer_on_failure_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[OnFailureFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.outer_on_failure(op, hook, priority=priority)

        return out

    # ....................... #

    def in_tx_before(
        self,
        op: OpKey | list[OpKey],
        guard: GuardFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "in_tx_before",
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def in_tx_before_pipeline(
        self,
        op: OpKey | list[OpKey],
        guards: Sequence[GuardFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, guard in enumerate(guards):
            priority = first_priority - i * 10
            out = out.in_tx_before(op, guard, priority=priority)

        return out

    # ....................... #

    def in_tx_finally(
        self,
        op: OpKey | list[OpKey],
        hook: FinallyFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> FinallyMiddleware[Any, Any]:
            return FinallyMiddleware[Any, Any](hook=hook(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "in_tx_finally",
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def in_tx_finally_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[FinallyFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.in_tx_finally(op, hook, priority=priority)

        return out

    # ....................... #

    def in_tx_on_failure(
        self,
        op: OpKey | list[OpKey],
        hook: OnFailureFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> OnFailureMiddleware[Any, Any]:
            return OnFailureMiddleware[Any, Any](hook=hook(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "in_tx_on_failure",
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def in_tx_on_failure_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[OnFailureFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.in_tx_on_failure(op, hook, priority=priority)

        return out

    # ....................... #

    def in_tx_after(
        self,
        op: OpKey | list[OpKey],
        effect: EffectFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
            return EffectMiddleware[Any, Any](effect=effect(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o, "in_tx_after", MiddlewareSpec(factory=factory, priority=priority)
            )

        return out

    # ....................... #

    def in_tx_after_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[EffectFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, effect in enumerate(effects):
            priority = first_priority - i * 10
            out = out.in_tx_after(op, effect, priority=priority)

        return out

    # ....................... #

    def in_tx_wrap(
        self,
        op: OpKey | list[OpKey],
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o, "in_tx_wrap", MiddlewareSpec(factory=middleware, priority=priority)
            )

        return out

    # ....................... #

    def in_tx_wrap_pipeline(
        self,
        op: OpKey | list[OpKey],
        middlewares: Sequence[MiddlewareFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, middleware in enumerate(middlewares):
            priority = first_priority - i * 10
            out = out.in_tx_wrap(op, middleware, priority=priority)

        return out

    # ....................... #

    def after_commit(
        self,
        op: OpKey | list[OpKey],
        effect: EffectFactory,
        *,
        priority: int = 0,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
            return EffectMiddleware[Any, Any](effect=effect(ctx))

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o, "after_commit", MiddlewareSpec(factory=factory, priority=priority)
            )

        return out

    # ....................... #

    def after_commit_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[EffectFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, effect in enumerate(effects):
            priority = first_priority - i * 10
            out = out.after_commit(op, effect, priority=priority)

        return out

    # ....................... #

    def in_tx_pipeline(
        self,
        op: OpKey | list[OpKey],
        before: Sequence[GuardFactory] | None = None,
        after: Sequence[EffectFactory] | None = None,
        wrap: Sequence[MiddlewareFactory] | None = None,
        on_failure: Sequence[OnFailureFactory] | None = None,
        finally_hooks: Sequence[FinallyFactory] | None = None,
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        if before is not None:
            out = out.in_tx_before_pipeline(op, before, first_priority=first_priority)

        if finally_hooks is not None:
            out = out.in_tx_finally_pipeline(
                op, finally_hooks, first_priority=first_priority
            )

        if on_failure is not None:
            out = out.in_tx_on_failure_pipeline(
                op, on_failure, first_priority=first_priority
            )

        if wrap is not None:
            out = out.in_tx_wrap_pipeline(op, wrap, first_priority=first_priority)

        if after is not None:
            out = out.in_tx_after_pipeline(op, after, first_priority=first_priority)

        return out

    # ....................... #

    def outer_pipeline(
        self,
        op: OpKey | list[OpKey],
        before: Sequence[GuardFactory] | None = None,
        after: Sequence[EffectFactory] | None = None,
        wrap: Sequence[MiddlewareFactory] | None = None,
        on_failure: Sequence[OnFailureFactory] | None = None,
        finally_hooks: Sequence[FinallyFactory] | None = None,
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        if before is not None:
            out = out.before_pipeline(op, before, first_priority=first_priority)

        if wrap is not None:
            out = out.wrap_pipeline(op, wrap, first_priority=first_priority)

        if finally_hooks is not None:
            out = out.outer_finally_pipeline(
                op, finally_hooks, first_priority=first_priority
            )

        if on_failure is not None:
            out = out.outer_on_failure_pipeline(
                op, on_failure, first_priority=first_priority
            )

        if after is not None:
            out = out.after_pipeline(op, after, first_priority=first_priority)

        return out

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

        logger.debug("Resolving usecase plan")

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError("Resolve on wildcard operation is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op))
        plan.validate()

        outer_before = _middleware_specs_for_usecase_tuple(plan, "outer_before")
        outer_wrap = _middleware_specs_for_usecase_tuple(plan, "outer_wrap")
        outer_finally = _middleware_specs_for_usecase_tuple(plan, "outer_finally")
        outer_on_failure = _middleware_specs_for_usecase_tuple(plan, "outer_on_failure")
        outer_after = _middleware_specs_for_usecase_tuple(plan, "outer_after")

        in_tx_before = _middleware_specs_for_usecase_tuple(plan, "in_tx_before")
        in_tx_finally = _middleware_specs_for_usecase_tuple(plan, "in_tx_finally")
        in_tx_on_failure = _middleware_specs_for_usecase_tuple(plan, "in_tx_on_failure")
        in_tx_wrap = _middleware_specs_for_usecase_tuple(plan, "in_tx_wrap")
        in_tx_after = _middleware_specs_for_usecase_tuple(plan, "in_tx_after")

        after_commit = plan.build("after_commit")

        logger.trace("Built plan for '%s' (tx=%s)", op, plan.tx)

        after_commit_effects: list[Effect[Any, Any]] = []

        for s in after_commit:
            mw = s.factory(ctx)

            logger.trace(
                "Built after_commit middleware %s (factory_id=%s)",
                type(mw).__qualname__,
                id(s.factory),
            )

            if not isinstance(mw, EffectMiddleware):
                raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

            after_commit_effects.append(mw.effect)

        chain: list[Middleware[Any, Any]] = []

        chain.extend(s.factory(ctx) for s in outer_before)
        chain.extend(s.factory(ctx) for s in outer_wrap)
        chain.extend(s.factory(ctx) for s in outer_finally)
        chain.extend(s.factory(ctx) for s in outer_on_failure)

        if plan.tx is not None:
            chain.append(
                TxMiddleware[Any, Any](
                    ctx=ctx,
                    route=plan.tx.route,
                ).with_after_commit(*after_commit_effects)
            )
            chain.extend(s.factory(ctx) for s in in_tx_before)
            chain.extend(s.factory(ctx) for s in in_tx_finally)
            chain.extend(s.factory(ctx) for s in in_tx_on_failure)
            chain.extend(s.factory(ctx) for s in in_tx_wrap)
            chain.extend(s.factory(ctx) for s in in_tx_after)

        chain.extend(s.factory(ctx) for s in outer_after)
        logger.trace("Constructed middleware chain with %s middleware(s)", len(chain))

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
