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
    TypeAlias,
    TypeVar,
    cast,
    final,
)

import attrs

from forze.application._logger import logger
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError

from .capabilities import (
    CapabilityExecutionEvent,
    SchedulableCapabilitySpec,
    build_capability_middleware_chain,
)
from .capability_keys import CapabilityKey
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


def frozenset_capability_keys(
    values: frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
) -> frozenset[str]:
    """Normalize ``requires`` / ``provides`` inputs to a ``frozenset[str]``.

    Accepts :class:`~forze.application.execution.capability_keys.CapabilityKey`
    values and other iterables of string-like keys used on plan builders and
    :class:`MiddlewareSpec`.
    """

    if values is None:
        return frozenset()

    if isinstance(values, frozenset):
        return frozenset(str(x) for x in values)

    if isinstance(values, set):
        return frozenset(str(x) for x in values)

    return frozenset(str(x) for x in values)


def _coerce_step_capability_caps(value: Any) -> frozenset[str]:
    return frozenset_capability_keys(
        cast(
            "frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None",
            value,
        )
    )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GuardStep:
    """Guard slot for pipelines with explicit ``requires`` / ``provides`` / ``step_label``."""

    factory: GuardFactory
    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    step_label: str | None = None


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectStep:
    """Effect slot for pipelines with explicit ``requires`` / ``provides`` / ``step_label``."""

    factory: EffectFactory
    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=_coerce_step_capability_caps,
    )
    step_label: str | None = None


PipelineGuardItem: TypeAlias = GuardFactory | GuardStep
PipelineEffectItem: TypeAlias = EffectFactory | EffectStep


def _normalize_pipeline_guard(
    item: PipelineGuardItem,
) -> tuple[GuardFactory, frozenset[str], frozenset[str], str | None]:
    if isinstance(item, GuardStep):
        return item.factory, item.requires, item.provides, item.step_label

    return item, frozenset(), frozenset(), None


def _normalize_pipeline_effect(
    item: PipelineEffectItem,
) -> tuple[EffectFactory, frozenset[str], frozenset[str], str | None]:
    if isinstance(item, EffectStep):
        return item.factory, item.requires, item.provides, item.step_label

    return item, frozenset(), frozenset(), None


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

_OPERATION_PLAN_BUCKETS: Final[tuple[PlanBucket, ...]] = (
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
)
"""All :class:`OperationPlan` middleware buckets (for dispatch edge collection)."""

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

CAPABILITY_SCHEDULER_BUCKETS: Final[frozenset[PlanBucket]] = frozenset(
    {
        "outer_before",
        "in_tx_before",
        "outer_after",
        "in_tx_after",
        "after_commit",
    }
)
"""Buckets whose specs participate in :func:`~forze.application.execution.capabilities.schedule_capability_specs`."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DispatchDeclaringEffectFactory:
    """Wraps an :class:`EffectFactory` and declares child op keys for dispatch graphs.

    Returned by :meth:`UsecaseDelegate.effect_factory` so :class:`UsecasePlan`
    builders can attach ``(source_op, target_op)`` edges to middleware specs.
    """

    inner: EffectFactory
    """Factory returning :class:`Effect`."""

    dispatch_targets: frozenset[str]
    """Logical child operation keys (same strings as registry registration)."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Effect[Any, Any]:
        return self.inner(ctx)


# ....................... #


def dispatch_edges_for_delegate_effect(
    source_ops: Sequence[str],
    effect: EffectFactory,
) -> frozenset[tuple[str, str]]:
    """Build dispatch edge tuples when ``effect`` declares delegate targets."""

    if isinstance(effect, DispatchDeclaringEffectFactory):
        return frozenset(
            (src, t) for src in source_ops for t in effect.dispatch_targets
        )

    return frozenset()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareSpec:
    """Specification for a middleware attached to an operation plan.

    Middlewares are ordered by ``priority`` (descending) and created lazily from a
    :class:`ExecutionContext` when a plan is resolved.

    When :attr:`UsecasePlan.use_capability_engine` is enabled, guard and effect
    buckets additionally order steps by ``requires`` / ``provides`` capability
    keys (see the capability execution reference page).
    """

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    factory: MiddlewareFactory
    """Callable returning middleware; effect buckets may use :class:`DispatchDeclaringEffectFactory`."""

    dispatch_edges: frozenset[tuple[str, str]] = attrs.field(
        factory=frozenset,
        repr=False,
    )
    """Edges ``(source_op, target_op)`` derived for registry dispatch validation."""

    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    """Capability keys that must be ready before this step runs (per-bucket graph)."""

    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    """Capability keys this step marks ready on success, or missing when skipped."""

    step_label: str | None = None
    """Optional stable label for logs and :meth:`UsecasePlan.explain`."""


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

    # ....................... #
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

    # ....................... #
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


def middleware_specs_for_usecase_tuple(
    plan: OperationPlan,
    bucket: PlanBucket,
) -> tuple[MiddlewareSpec, ...]:
    """Return specs for ``bucket`` in the order used when building ``Usecase.middlewares``.

    Applies the same reversal rules as :meth:`UsecasePlan.resolve` (see
    ``_EFFECT_OR_WRAP_BUCKETS_REVERSED_IN_USECASE_TUPLE``). This is the tuple
    passed into :func:`~forze.application.execution.capabilities.schedule_capability_specs`
    for buckets in :data:`CAPABILITY_SCHEDULER_BUCKETS`.
    """

    built = plan.build(bucket)

    if bucket in _EFFECT_OR_WRAP_BUCKETS_REVERSED_IN_USECASE_TUPLE:
        return tuple(reversed(built))

    return built


# ....................... #

StepExplainKind = Literal["guard", "effect", "wrap", "finally", "on_failure", "tx"]
ScheduleMode = Literal["legacy_priority", "capability_topo"]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StepExplainRow:
    """One scheduled step in :class:`ExecutionPlanReport`."""

    bucket: str
    label: str
    priority: int
    requires: frozenset[str]
    provides: frozenset[str]
    schedule_index: int
    kind: StepExplainKind
    schedule_mode: ScheduleMode
    dispatch_edge_count: int = 0


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionPlanReport:
    """Static introspection for :meth:`UsecasePlan.explain`."""

    op: str
    use_capability_engine: bool
    has_transaction: bool
    steps: tuple[StepExplainRow, ...]


# ....................... #


def _factory_label(spec: MiddlewareSpec) -> str:
    f = spec.factory
    qn = getattr(f, "__qualname__", None)

    if isinstance(qn, str) and qn:
        return qn

    name = getattr(f, "__name__", None)

    if isinstance(name, str) and name:
        return name

    return repr(f)


def _bucket_schedule_mode(
    bucket: PlanBucket,
    specs: tuple[MiddlewareSpec, ...],
    *,
    use_capability_engine: bool,
) -> ScheduleMode:
    if not use_capability_engine:
        return "legacy_priority"

    if bucket not in CAPABILITY_SCHEDULER_BUCKETS:
        return "legacy_priority"

    if any(s.requires or s.provides for s in specs):
        return "capability_topo"

    return "legacy_priority"


def _ordered_specs_for_explain(
    bucket: PlanBucket,
    specs: tuple[MiddlewareSpec, ...],
    *,
    use_capability_engine: bool,
) -> tuple[MiddlewareSpec, ...]:
    from .capabilities import schedule_capability_specs

    if use_capability_engine and bucket in CAPABILITY_SCHEDULER_BUCKETS:
        return cast(
            tuple[MiddlewareSpec, ...],
            schedule_capability_specs(
                cast(tuple[SchedulableCapabilitySpec, ...], specs),
                bucket=bucket,
            ),
        )

    return specs


def _append_explain_rows(
    rows: list[StepExplainRow],
    bucket: PlanBucket | str,
    specs: tuple[MiddlewareSpec, ...],
    *,
    kind: StepExplainKind,
    schedule_mode: ScheduleMode,
    start_idx: int,
) -> int:
    idx = start_idx

    for s in specs:
        rows.append(
            StepExplainRow(
                bucket=str(bucket),
                label=s.step_label or _factory_label(s),
                priority=s.priority,
                requires=s.requires,
                provides=s.provides,
                schedule_index=idx,
                kind=kind,
                schedule_mode=schedule_mode,
                dispatch_edge_count=len(s.dispatch_edges),
            )
        )

        idx += 1

    return idx


def _build_execution_plan_report(
    *,
    plan: OperationPlan,
    op: str,
    use_capability_engine: bool,
) -> ExecutionPlanReport:
    rows: list[StepExplainRow] = []
    idx = 0

    outer_segments: tuple[tuple[PlanBucket, StepExplainKind], ...] = (
        ("outer_before", "guard"),
        ("outer_wrap", "wrap"),
        ("outer_finally", "finally"),
        ("outer_on_failure", "on_failure"),
    )

    for bucket, kind in outer_segments:
        specs = middleware_specs_for_usecase_tuple(plan, bucket)
        mode = _bucket_schedule_mode(
            bucket, specs, use_capability_engine=use_capability_engine
        )
        ordered = _ordered_specs_for_explain(
            bucket, specs, use_capability_engine=use_capability_engine
        )
        idx = _append_explain_rows(
            rows, bucket, ordered, kind=kind, schedule_mode=mode, start_idx=idx
        )

    if plan.tx is not None:
        rows.append(
            StepExplainRow(
                bucket="tx",
                label=f"TxMiddleware(route={str(plan.tx.route)})",
                priority=0,
                requires=frozenset(),
                provides=frozenset(),
                schedule_index=idx,
                kind="tx",
                schedule_mode="legacy_priority",
                dispatch_edge_count=0,
            )
        )
        idx += 1

        in_tx_segments: tuple[tuple[PlanBucket, StepExplainKind], ...] = (
            ("in_tx_before", "guard"),
            ("in_tx_finally", "finally"),
            ("in_tx_on_failure", "on_failure"),
            ("in_tx_wrap", "wrap"),
        )

        for bucket, kind in in_tx_segments:
            specs = middleware_specs_for_usecase_tuple(plan, bucket)
            mode = _bucket_schedule_mode(
                bucket, specs, use_capability_engine=use_capability_engine
            )
            ordered = _ordered_specs_for_explain(
                bucket, specs, use_capability_engine=use_capability_engine
            )
            idx = _append_explain_rows(
                rows, bucket, ordered, kind=kind, schedule_mode=mode, start_idx=idx
            )

        ita_specs = middleware_specs_for_usecase_tuple(plan, "in_tx_after")
        ita_mode = _bucket_schedule_mode(
            "in_tx_after", ita_specs, use_capability_engine=use_capability_engine
        )
        ita_ordered = _ordered_specs_for_explain(
            "in_tx_after", ita_specs, use_capability_engine=use_capability_engine
        )
        idx = _append_explain_rows(
            rows,
            "in_tx_after",
            ita_ordered,
            kind="effect",
            schedule_mode=ita_mode,
            start_idx=idx,
        )

        ac_specs = middleware_specs_for_usecase_tuple(plan, "after_commit")
        ac_mode = _bucket_schedule_mode(
            "after_commit", ac_specs, use_capability_engine=use_capability_engine
        )
        ac_ordered = _ordered_specs_for_explain(
            "after_commit", ac_specs, use_capability_engine=use_capability_engine
        )
        idx = _append_explain_rows(
            rows,
            "after_commit",
            ac_ordered,
            kind="effect",
            schedule_mode=ac_mode,
            start_idx=idx,
        )

    oa_specs = middleware_specs_for_usecase_tuple(plan, "outer_after")
    oa_mode = _bucket_schedule_mode(
        "outer_after", oa_specs, use_capability_engine=use_capability_engine
    )
    oa_ordered = _ordered_specs_for_explain(
        "outer_after", oa_specs, use_capability_engine=use_capability_engine
    )
    _append_explain_rows(
        rows,
        "outer_after",
        oa_ordered,
        kind="effect",
        schedule_mode=oa_mode,
        start_idx=idx,
    )

    return ExecutionPlanReport(
        op=op,
        use_capability_engine=use_capability_engine,
        has_transaction=plan.tx is not None,
        steps=tuple(rows),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    """Declarative plan for composing usecases per operation.

    Maps operation keys to :class:`OperationPlan`. Use ``*`` (wildcard) for
    defaults applied to all operations. :meth:`resolve` merges base and
    op-specific plans, then builds the middleware chain.

    Set :attr:`use_capability_engine` (or call :meth:`with_capability_engine`) to
    derive guard/effect order from declared capability keys inside each bucket.
    """

    ops: dict[str, OperationPlan] = attrs.field(factory=dict)
    """Operation key to plan mapping."""

    use_capability_engine: bool = False
    """When ``True``, :meth:`resolve` composes guard/effect buckets via the capability scheduler."""

    # ....................... #
    # Helpers

    def _base(self) -> OperationPlan:
        return self.ops.get(WILDCARD, OperationPlan())

    def _op(self, op: OpKey) -> OperationPlan:
        return self.ops.get(str(op), OperationPlan())

    def merged_operation_plan(self, op: OpKey) -> OperationPlan:
        """Merge the wildcard plan with the plan registered for ``op``."""

        return OperationPlan.merge(self._base(), self._op(op))

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

    def derived_dispatch_edges(self) -> frozenset[tuple[str, str]]:
        """Collect ``dispatch_edges`` from every middleware spec in this plan."""

        acc: set[tuple[str, str]] = set()

        for oplan in self.ops.values():
            for bucket in _OPERATION_PLAN_BUCKETS:
                for spec in getattr(oplan, bucket):
                    acc.update(spec.dispatch_edges)

        return frozenset(acc)

    # ....................... #

    def with_capability_engine(self, enabled: bool = True) -> Self:
        """Return a plan that enables or disables capability-driven guard/effect ordering."""

        return attrs.evolve(self, use_capability_engine=bool(enabled))

    # ....................... #

    def explain(self, op: OpKey) -> ExecutionPlanReport:
        """Return a static report of merged middleware and capability schedules for ``op``.

        Does not instantiate ports or call factories beyond reading attributes.

        :param op: Concrete operation key (wildcard not allowed).
        :returns: :class:`~forze.application.execution.capabilities.ExecutionPlanReport`
        """

        op_s = str(op)

        if op_s == WILDCARD or op_s.endswith(WILDCARD):
            raise CoreError("Explain on wildcard operation is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op_s))
        plan.validate()

        return _build_execution_plan_report(
            plan=plan,
            op=op_s,
            use_capability_engine=self.use_capability_engine,
        )

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
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "outer_before",
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def before_pipeline(
        self,
        op: OpKey | list[OpKey],
        guards: Sequence[PipelineGuardItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(guards):
            priority = first_priority - i * 10
            gf, req, prov, label = _normalize_pipeline_guard(item)
            out = out.before(
                op,
                gf,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def after(
        self,
        op: OpKey | list[OpKey],
        effect: EffectFactory,
        *,
        priority: int = 0,
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            o_s = str(o)
            d_edges = dispatch_edges_for_delegate_effect((o_s,), effect)

            def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
                return EffectMiddleware[Any, Any](effect=effect(ctx))

            out = out._add(
                o,
                "outer_after",
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    dispatch_edges=d_edges,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def after_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[PipelineEffectItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(effects):
            priority = first_priority - i * 10
            ef, req, prov, label = _normalize_pipeline_effect(item)
            out = out.after(
                op,
                ef,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

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
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        def factory(ctx: ExecutionContext) -> GuardMiddleware[Any, Any]:
            return GuardMiddleware[Any, Any](guard=guard(ctx))

        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            out = out._add(
                o,
                "in_tx_before",
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def in_tx_before_pipeline(
        self,
        op: OpKey | list[OpKey],
        guards: Sequence[PipelineGuardItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(guards):
            priority = first_priority - i * 10
            gf, req, prov, label = _normalize_pipeline_guard(item)
            out = out.in_tx_before(
                op,
                gf,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

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
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            o_s = str(o)
            d_edges = dispatch_edges_for_delegate_effect((o_s,), effect)

            def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
                return EffectMiddleware[Any, Any](effect=effect(ctx))

            out = out._add(
                o,
                "in_tx_after",
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    dispatch_edges=d_edges,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def in_tx_after_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[PipelineEffectItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(effects):
            priority = first_priority - i * 10
            ef, req, prov, label = _normalize_pipeline_effect(item)
            out = out.in_tx_after(
                op,
                ef,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

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
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)

        out: Self = self

        if not isinstance(op, list):
            op = [op]

        for o in op:
            o_s = str(o)
            d_edges = dispatch_edges_for_delegate_effect((o_s,), effect)

            def factory(ctx: ExecutionContext) -> EffectMiddleware[Any, Any]:
                return EffectMiddleware[Any, Any](effect=effect(ctx))

            out = out._add(
                o,
                "after_commit",
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    dispatch_edges=d_edges,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def after_commit_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[PipelineEffectItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(effects):
            priority = first_priority - i * 10
            ef, req, prov, label = _normalize_pipeline_effect(item)
            out = out.after_commit(
                op,
                ef,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def in_tx_pipeline(
        self,
        op: OpKey | list[OpKey],
        before: Sequence[PipelineGuardItem] | None = None,
        after: Sequence[PipelineEffectItem] | None = None,
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
        before: Sequence[PipelineGuardItem] | None = None,
        after: Sequence[PipelineEffectItem] | None = None,
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
        *,
        capability_execution_trace: list[CapabilityExecutionEvent] | None = None,
    ) -> U:
        """Build a composed usecase instance for an operation.

        Merges base (wildcard) and op-specific plans, validates, builds the
        middleware chain, and wraps the factory result.

        :param op: Operation key (wildcard not allowed).
        :param ctx: Execution context for factory resolution.
        :param factory: Usecase factory.
        :param capability_execution_trace: When provided, append-only list filled
            with capability segment events when the capability engine is enabled.
        :returns: Composed usecase with middlewares.
        :raises CoreError: If op is wildcard or plan is invalid.
        """

        op = str(op)

        logger.debug("Resolving usecase plan")

        if op == WILDCARD or op.endswith(WILDCARD):
            raise CoreError("Resolve on wildcard operation is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op))
        plan.validate()

        outer_before = middleware_specs_for_usecase_tuple(plan, "outer_before")
        outer_wrap = middleware_specs_for_usecase_tuple(plan, "outer_wrap")
        outer_finally = middleware_specs_for_usecase_tuple(plan, "outer_finally")
        outer_on_failure = middleware_specs_for_usecase_tuple(plan, "outer_on_failure")
        outer_after = middleware_specs_for_usecase_tuple(plan, "outer_after")

        in_tx_before = middleware_specs_for_usecase_tuple(plan, "in_tx_before")
        in_tx_finally = middleware_specs_for_usecase_tuple(plan, "in_tx_finally")
        in_tx_on_failure = middleware_specs_for_usecase_tuple(plan, "in_tx_on_failure")
        in_tx_wrap = middleware_specs_for_usecase_tuple(plan, "in_tx_wrap")
        in_tx_after = middleware_specs_for_usecase_tuple(plan, "in_tx_after")

        after_commit = plan.build("after_commit")

        logger.trace("Built plan for '%s' (tx=%s)", op, plan.tx)

        chain: list[Middleware[Any, Any]]

        if self.use_capability_engine:
            chain = list(
                build_capability_middleware_chain(
                    ctx=ctx,
                    plan=plan,
                    outer_before=outer_before,
                    outer_wrap=outer_wrap,
                    outer_finally=outer_finally,
                    outer_on_failure=outer_on_failure,
                    outer_after=outer_after,
                    in_tx_before=in_tx_before,
                    in_tx_finally=in_tx_finally,
                    in_tx_on_failure=in_tx_on_failure,
                    in_tx_wrap=in_tx_wrap,
                    in_tx_after=in_tx_after,
                    after_commit_specs=after_commit,
                    capability_execution_trace=capability_execution_trace,
                )
            )

        else:
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

            chain = []

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
        use_engine = False

        for p in plans:
            use_engine = use_engine or p.use_capability_engine

            for op, pl in p.ops.items():
                cur = acc.get(op, OperationPlan())
                acc[op] = cur.merge(pl)

        return cls(ops=acc, use_capability_engine=use_engine)

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
