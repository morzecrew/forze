from enum import StrEnum
from typing import Any, Callable, Final, Iterable, Literal, Self, TypeVar, final

import attrs
from pydantic import BaseModel, Field

from forze.base.errors import CoreError
from forze.utils.debug import get_callable_module, get_callable_name

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

U = TypeVar("U", bound=Usecase[Any, Any])

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
EffectFactory = Callable[[ExecutionContext], Effect[Any, Any]]
MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]

OpKey = str | StrEnum

WILDCARD: Final[str] = "*"

PlanBucket = Literal[
    "outer_before",
    "outer_wrap",
    "outer_after",
    "in_tx_before",
    "in_tx_wrap",
    "in_tx_after",
    "after_commit",
]

# ....................... #


class _ExplainItem(BaseModel):
    bucket: PlanBucket
    priority: int
    factory: str
    factory_id: int


class _Explain(BaseModel):
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
    """Per-operation composition"""

    tx: bool = False

    # outer
    outer_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    outer_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    outer_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)

    # in tx
    in_tx_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    in_tx_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    in_tx_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)

    after_commit: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)

    # ....................... #

    def add(
        self,
        bucket: PlanBucket,
        spec: MiddlewareSpec,
    ) -> Self:  # questionable dodgy code ...
        if not hasattr(self, bucket):
            raise CoreError(f"Invalid bucket: {bucket}")

        cur = getattr(self, bucket)
        return attrs.evolve(self, **{bucket: (*cur, spec)})

    # ....................... #

    def validate(self) -> None:
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
        self, specs: Iterable[MiddlewareSpec], *, bucket: PlanBucket
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
        self, specs: Iterable[MiddlewareSpec], *, reverse: bool
    ) -> tuple[MiddlewareSpec, ...]:
        return tuple(sorted(specs, key=lambda s: s.priority, reverse=reverse))

    # ....................... #

    def build(self, bucket: PlanBucket) -> tuple[MiddlewareSpec, ...]:
        deduped_specs = self.__dedupe(bucket)

        return self.__sort(deduped_specs, reverse=True)

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self):
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


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    """Mutable description of how usecases for operations are composed."""

    ops: dict[str, OperationPlan] = attrs.field(factory=dict)

    # ....................... #
    # Helpers

    def _base(self):
        return self.ops.get(WILDCARD, OperationPlan())

    def _op(self, op: OpKey) -> OperationPlan:
        return self.ops.get(str(op), OperationPlan())

    def _put(self, op: OpKey, plan: OperationPlan) -> Self:
        new_ops = dict(self.ops)
        new_ops[str(op)] = plan

        return attrs.evolve(self, ops=new_ops)

    def _add(self, op: OpKey, bucket: PlanBucket, spec: MiddlewareSpec) -> Self:
        cur = self._op(op)
        return self._put(op, cur.add(bucket, spec))

    # ....................... #

    def tx(self, op: OpKey) -> Self:
        cur = self._op(op)
        return self._put(op, attrs.evolve(cur, tx=True))

    # ....................... #

    def before(self, op: OpKey, guard: GuardFactory, *, priority: int = 0) -> Self:
        def factory(ctx: ExecutionContext):
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        return self._add(
            op,
            "outer_before",
            MiddlewareSpec(factory=factory, priority=priority),
        )

    # ....................... #

    def after(self, op: OpKey, effect: EffectFactory, *, priority: int = 0) -> Self:
        def factory(ctx: ExecutionContext):
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
        def factory(ctx: ExecutionContext):
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
        def factory(ctx: ExecutionContext):
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
        def factory(ctx: ExecutionContext):
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
        """Build a composed usecase instance for an operation."""

        op = str(op)

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError(f"Resolve on wildcard operation `{op}` is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op))
        plan.validate()

        outer_before = plan.build("outer_before")
        outer_wrap = plan.build("outer_wrap")
        outer_after = plan.build("outer_after")

        in_tx_before = plan.build("in_tx_before")
        in_tx_wrap = plan.build("in_tx_wrap")
        in_tx_after = plan.build("in_tx_after")

        after_commit = plan.build("after_commit")
        after_commit_effects: list[Effect[Any, Any]] = []

        for s in after_commit:
            mw = s.factory(ctx)

            if not isinstance(mw, EffectMiddleware):
                raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

            after_commit_effects.append(mw.effect)

        chain: list[Middleware[Any, Any]] = []

        chain += [s.factory(ctx) for s in outer_before]
        chain += [s.factory(ctx) for s in outer_wrap]

        if plan.tx:
            chain.append(
                TxMiddleware[Any, Any](ctx=ctx).with_after_commit(*after_commit_effects)
            )
            chain += [s.factory(ctx) for s in in_tx_before]
            chain += [s.factory(ctx) for s in in_tx_wrap]
            chain += [s.factory(ctx) for s in in_tx_after]

        chain += [s.factory(ctx) for s in outer_after]

        uc = factory(ctx)

        return uc.with_middlewares(*chain)

    # ....................... #

    def explain(self, op: OpKey):
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
        chain += pack("outer_before", outer_before)
        chain += pack("outer_wrap", outer_wrap)

        if plan.tx:
            chain += pack("in_tx_before", in_tx_before)
            chain += pack("in_tx_wrap", in_tx_wrap)
            chain += pack("in_tx_after", in_tx_after)

        chain += pack("outer_after", outer_after)

        return _Explain(
            op=op,
            tx=plan.tx,
            chain=chain,
            after_commit=pack("after_commit", after_commit),
        )

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self) -> Self:
        """Merge multiple plans into a single aggregate plan."""

        acc: dict[str, OperationPlan] = {}

        for p in plans:
            for op, pl in p.ops.items():
                cur = acc.get(op, OperationPlan())
                acc[op] = OperationPlan.merge(cur, pl)

        return cls(ops=acc)
