"""Registry-owned authoring and resolution for application execution."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal, Self, final

import attrs
from structlog.contextvars import bind_contextvars

from forze.application._logger import logger
from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from .context import ExecutionContext
from .dispatch import (
    assert_dispatch_edges_reference_registered_ops,
    assert_dispatch_graph_acyclic,
    expand_wildcard_dispatch_sources,
)
from .engine.compiler import ExecutionChainCompiler
from .engine.model import OperationStages
from .engine.stages import Stage
from .middleware import SuccessHook  # type: ignore[import-not-found]
from .plan.builders import (  # type: ignore[import-not-found]
    finally_middleware_factory,
    guard_middleware_factory,
    on_failure_middleware_factory,
    success_hook_middleware_factory,
)
from .plan.dag import PlanDag
from .plan.report import ExecutionPlanReport, build_execution_plan_report
from .plan.spec import MiddlewareSpec, frozenset_capability_keys
from .plan.types import (  # type: ignore[import-not-found]
    WILDCARD,
    FinallyFactory,
    GuardFactory,
    MiddlewareFactory,
    OnFailureFactory,
    SuccessHookFactory,
)
from .registry.graph import DispatchGraph
from .registry.ops import OperationNamespace, OperationRef
from .usecase import Usecase, UsecaseFactory

# ----------------------- #

type CapabilityKeysInput = (frozenset[str] | set[str] | Iterable[str | StrEnum] | None)

_SCOPE_TOKEN_RE = re.compile(r"[^\w.-]+")

# ....................... #


def _coerce_namespace(
    namespace: str | OperationNamespace | None,
) -> OperationNamespace | None:
    if namespace is None:
        return None

    if isinstance(namespace, OperationNamespace):
        return namespace

    return OperationNamespace(prefix=namespace)


# ....................... #


def _op_list(op: StrKey | list[StrKey]) -> list[StrKey]:
    return op if isinstance(op, list) else [op]


def _scope_token(op: str) -> str:
    if op == WILDCARD:
        return "wildcard"

    cleaned = _SCOPE_TOKEN_RE.sub("_", op).strip("_.")
    return cleaned or "op"


# ....................... #


@final
@attrs.define(slots=True)
class UsecaseRegistry:
    """Single authoring surface for factories, stages, and nested dispatch."""

    _init_factories: Mapping[StrKey, UsecaseFactory] | None = attrs.field(
        default=None,
        alias="factories",
        repr=False,
        eq=False,
    )

    _namespace: OperationNamespace | None = attrs.field(
        default=None,
        alias="namespace",
        kw_only=True,
        converter=_coerce_namespace,
        repr=False,
    )

    _dispatch_graph: DispatchGraph = attrs.field(
        init=False,
        factory=DispatchGraph,
        repr=False,
    )

    _factories: dict[str, UsecaseFactory] = attrs.field(
        init=False,
        factory=dict,
        repr=False,
    )

    _finalized: bool = attrs.field(
        init=False,
        default=False,
        repr=False,
    )

    _operation_id_prefix: str | None = attrs.field(
        init=False,
        default=None,
        repr=False,
    )

    _stages: dict[str, OperationStages] = attrs.field(
        init=False,
        factory=dict,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self._init_factories:
            to_register = dict(self._init_factories)
            object.__setattr__(self, "_init_factories", None)
            self.register_many(to_register)

    @property
    def namespace(self) -> OperationNamespace | None:
        return self._namespace

    @property
    def factories(self) -> Mapping[str, UsecaseFactory]:
        return MappingProxyType(self._factories)

    def _raise_if_finalized(self) -> None:
        if self._finalized:
            raise CoreError("Registry is finalized")

    def _normalize_op_key(
        self,
        op: StrKey | OperationRef[Any, Any],
    ) -> str:
        if isinstance(op, OperationRef):
            return op.op

        op_s = str(op)

        if not op_s:
            raise CoreError("Operation key must be non-empty")

        if op_s == WILDCARD or "." in op_s or self._namespace is None:
            return op_s

        return self._namespace.key(op_s)

    def _normalize_ops(self, op: StrKey | list[StrKey]) -> tuple[str, ...]:
        return tuple(self._normalize_op_key(op_key) for op_key in _op_list(op))

    def _base(self) -> OperationStages:
        return self._stages.get(WILDCARD, OperationStages())

    def _set_operation_stages(
        self,
        op: str,
        stages: OperationStages,
    ) -> None:
        self._stages[op] = stages

    def _add_stage_spec(
        self,
        op: StrKey | list[StrKey],
        stage: Stage,
        spec: MiddlewareSpec,
    ) -> None:
        for op_key in self._normalize_ops(op):
            logger.trace(
                "Adding middleware to registry (op=%s, stage=%s, priority=%s, factory_id=%s)",
                op_key,
                stage.value,
                spec.priority,
                id(spec.factory),
            )
            current = self._stages.get(op_key, OperationStages())
            self._set_operation_stages(op_key, current.add(stage, spec))

    @staticmethod
    def _guard_spec(
        guard: GuardFactory,
        *,
        priority: int,
        requires: CapabilityKeysInput,
        provides: CapabilityKeysInput,
        step_label: str | None,
    ) -> MiddlewareSpec:
        return MiddlewareSpec(
            factory=guard_middleware_factory(guard),
            priority=priority,
            requires=frozenset_capability_keys(requires),
            provides=frozenset_capability_keys(provides),
            step_label=step_label,
        )

    @staticmethod
    def _success_hook_spec(
        hook: SuccessHookFactory,
        *,
        priority: int,
        requires: CapabilityKeysInput,
        provides: CapabilityKeysInput,
        step_label: str | None,
    ) -> MiddlewareSpec:
        return MiddlewareSpec(
            factory=success_hook_middleware_factory(hook),
            priority=priority,
            requires=frozenset_capability_keys(requires),
            provides=frozenset_capability_keys(provides),
            step_label=step_label,
        )

    def _dag_scope(self, op: str, stage: Stage) -> str:
        existing = len(self._stages.get(op, OperationStages()).specs(stage))
        return f"{stage.value}.{_scope_token(op)}.{existing}"

    def _add_guard(
        self,
        op: StrKey | list[StrKey],
        stage: Stage,
        guard: GuardFactory,
        *,
        priority: int,
        requires: CapabilityKeysInput,
        provides: CapabilityKeysInput,
        step_label: str | None,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            stage,
            self._guard_spec(
                guard,
                priority=priority,
                requires=requires,
                provides=provides,
                step_label=step_label,
            ),
        )
        return self

    def _add_success_hook(
        self,
        op: StrKey | list[StrKey],
        stage: Stage,
        hook: SuccessHookFactory,
        *,
        priority: int,
        requires: CapabilityKeysInput,
        provides: CapabilityKeysInput,
        step_label: str | None,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            stage,
            self._success_hook_spec(
                hook,
                priority=priority,
                requires=requires,
                provides=provides,
                step_label=step_label,
            ),
        )
        return self

    def _add_guard_dag(
        self,
        op: StrKey | list[StrKey],
        stage: Stage,
        dag: PlanDag[GuardFactory],
    ) -> Self:
        self._raise_if_finalized()

        for op_key in self._normalize_ops(op):
            specs = dag.compile_(
                scope=self._dag_scope(op_key, stage),
                spec_factory=lambda factory, priority, requires, provides, label: (
                    self._guard_spec(
                        factory,
                        priority=priority,
                        requires=requires,
                        provides=provides,
                        step_label=label,
                    )
                ),
            )

            for spec in specs:
                self._add_stage_spec(op_key, stage, spec)

        return self

    def _add_success_hook_dag(
        self,
        op: StrKey | list[StrKey],
        stage: Stage,
        dag: PlanDag[SuccessHookFactory],
    ) -> Self:
        self._raise_if_finalized()

        for op_key in self._normalize_ops(op):
            specs = dag.compile_(
                scope=self._dag_scope(op_key, stage),
                spec_factory=lambda factory, priority, requires, provides, label: (
                    self._success_hook_spec(
                        factory,
                        priority=priority,
                        requires=requires,
                        provides=provides,
                        step_label=label,
                    )
                ),
            )

            for spec in specs:
                self._add_stage_spec(op_key, stage, spec)

        return self

    def _merged_operation_stages(
        self,
        op: StrKey | OperationRef[Any, Any],
    ) -> OperationStages:
        normalized = self._normalize_op_key(op)
        return OperationStages.merge_base_and_specific(
            self._base(),
            self._stages.get(normalized, OperationStages()),
        )

    def _assert_dispatch_edge_if_nested(self, op_s: str, ctx: ExecutionContext) -> None:
        if not self._finalized or not self._dispatch_graph.is_frozen:
            return

        stack = ctx.usecase_dispatch_stack

        if not stack:
            return

        parent_entry = stack[-1]

        if self._operation_id_prefix is not None:
            prefix = f"{self._operation_id_prefix}."

            if not parent_entry.startswith(prefix):
                return

            parent_logical = parent_entry[len(prefix) :]
        else:
            parent_logical = parent_entry

        if not self._dispatch_graph.has_edge(parent_logical, op_s):
            raise CoreError(
                "Usecase dispatch is not declared on the registry graph: "
                f"from {parent_logical!r} to {op_s!r}. "
                "Add an edge via add_dispatch_edge before finalize.",
            )

    def _validate_dispatch_graph_for_finalize(self) -> frozenset[tuple[str, str]]:
        combined = self._dispatch_graph.edges()
        registered = set(self._factories.keys())
        expanded = expand_wildcard_dispatch_sources(
            combined,
            registered,
            wildcard=WILDCARD,
        )
        assert_dispatch_graph_acyclic(expanded)
        assert_dispatch_edges_reference_registered_ops(expanded, registered)
        return expanded

    def _validate_capabilities_for_finalize(self) -> None:
        from .engine.capabilities import schedule_capability_specs

        for op in sorted(self._factories.keys()):
            stages = self._merged_operation_stages(op)
            stages.validate()

            for stage in Stage.iter_schedulable():
                schedule_capability_specs(
                    stages.specs_for_chain(stage),
                    stage=stage.value,
                )

    # ....................... #

    def op(self, suffix: StrKey | OperationRef[Any, Any]) -> str:
        return self._normalize_op_key(suffix)

    def ref[Args, R](
        self,
        op: StrKey | OperationRef[Args, R],
        *,
        uc: type[Usecase[Args, R]] | None = None,
    ) -> OperationRef[Args, R]:
        if isinstance(op, OperationRef):
            key = self._normalize_op_key(op)
            return OperationRef(key, uc=op.uc or uc, name=op.name)

        key = self._normalize_op_key(op)

        if self._namespace is None and "." not in str(op):
            raise CoreError(
                "Registry.ref requires a full operation key when registry.namespace is None",
            )

        return OperationRef(key, uc=uc)

    def register(self, op: StrKey, factory: UsecaseFactory) -> Self:
        self._raise_if_finalized()

        op_s = self._normalize_op_key(op)
        if op_s in self._factories:
            raise CoreError(
                f"Usecase factory is already registered for operation: {op_s}",
            )

        self._factories[op_s] = factory
        return self

    def override(self, op: StrKey, factory: UsecaseFactory) -> Self:
        self._raise_if_finalized()

        op_s = self._normalize_op_key(op)
        if op_s not in self._factories:
            raise CoreError(f"Usecase factory is not registered for operation: {op_s}")

        self._factories[op_s] = factory
        return self

    def register_many(self, ops: Mapping[StrKey, UsecaseFactory]) -> Self:
        self._raise_if_finalized()

        normalized = {
            self._normalize_op_key(op): factory for op, factory in ops.items()
        }
        already_registered = set(self._factories).intersection(normalized)

        if already_registered:
            raise CoreError(
                f"Usecase factories are already registered for operations: {already_registered}",
            )

        self._factories.update(normalized)
        return self

    def override_many(self, ops: Mapping[StrKey, UsecaseFactory]) -> Self:
        self._raise_if_finalized()

        normalized = {
            self._normalize_op_key(op): factory for op, factory in ops.items()
        }
        missing = set(normalized).difference(self._factories)

        if missing:
            raise CoreError(
                f"Usecase factories are not registered for operations: {missing}",
            )

        self._factories.update(normalized)
        return self

    def exists(self, op: StrKey | OperationRef[Any, Any]) -> bool:
        return self._normalize_op_key(op) in self._factories

    def add_dispatch_edge(
        self,
        from_op: StrKey | OperationRef[Any, Any],
        to_op: StrKey | OperationRef[Any, Any],
    ) -> Self:
        self._raise_if_finalized()
        self._dispatch_graph.add_edge(
            self._normalize_op_key(from_op),
            self._normalize_op_key(to_op),
        )
        return self

    def before(
        self,
        op: StrKey | list[StrKey],
        guard: GuardFactory,
        *,
        priority: int = 0,
        requires: CapabilityKeysInput = None,
        provides: CapabilityKeysInput = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_guard(
            op,
            Stage.before,
            guard,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    def before_dag(
        self,
        op: StrKey | list[StrKey],
        dag: PlanDag[GuardFactory],
    ) -> Self:
        return self._add_guard_dag(op, Stage.before, dag)

    def after_success(
        self,
        op: StrKey | list[StrKey],
        hook: SuccessHookFactory,
        *,
        priority: int = 0,
        requires: CapabilityKeysInput = None,
        provides: CapabilityKeysInput = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_success_hook(
            op,
            Stage.after_success,
            hook,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    def after_success_dag(
        self,
        op: StrKey | list[StrKey],
        dag: PlanDag[SuccessHookFactory],
    ) -> Self:
        return self._add_success_hook_dag(op, Stage.after_success, dag)

    def wrap(
        self,
        op: StrKey | list[StrKey],
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            Stage.wrap,
            MiddlewareSpec(factory=middleware, priority=priority),
        )
        return self

    def finally_(
        self,
        op: StrKey | list[StrKey],
        hook: FinallyFactory,
        *,
        priority: int = 0,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            Stage.finally_,
            MiddlewareSpec(
                factory=finally_middleware_factory(hook),
                priority=priority,
            ),
        )
        return self

    def on_failure(
        self,
        op: StrKey | list[StrKey],
        hook: OnFailureFactory,
        *,
        priority: int = 0,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            Stage.on_failure,
            MiddlewareSpec(
                factory=on_failure_middleware_factory(hook),
                priority=priority,
            ),
        )
        return self

    def tx(self, op: StrKey | list[StrKey], *, route: str | StrEnum) -> Self:
        self._raise_if_finalized()

        for op_key in self._normalize_ops(op):
            current = self._stages.get(op_key, OperationStages())
            self._set_operation_stages(op_key, current.with_tx(route))

        return self

    def tx_before(
        self,
        op: StrKey | list[StrKey],
        guard: GuardFactory,
        *,
        priority: int = 0,
        requires: CapabilityKeysInput = None,
        provides: CapabilityKeysInput = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_guard(
            op,
            Stage.tx_before,
            guard,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    def tx_before_dag(
        self,
        op: StrKey | list[StrKey],
        dag: PlanDag[GuardFactory],
    ) -> Self:
        return self._add_guard_dag(op, Stage.tx_before, dag)

    def tx_wrap(
        self,
        op: StrKey | list[StrKey],
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            Stage.tx_wrap,
            MiddlewareSpec(factory=middleware, priority=priority),
        )
        return self

    def tx_finally(
        self,
        op: StrKey | list[StrKey],
        hook: FinallyFactory,
        *,
        priority: int = 0,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            Stage.tx_finally,
            MiddlewareSpec(
                factory=finally_middleware_factory(hook),
                priority=priority,
            ),
        )
        return self

    def tx_on_failure(
        self,
        op: StrKey | list[StrKey],
        hook: OnFailureFactory,
        *,
        priority: int = 0,
    ) -> Self:
        self._raise_if_finalized()
        self._add_stage_spec(
            op,
            Stage.tx_on_failure,
            MiddlewareSpec(
                factory=on_failure_middleware_factory(hook),
                priority=priority,
            ),
        )
        return self

    def tx_after_success(
        self,
        op: StrKey | list[StrKey],
        hook: SuccessHookFactory,
        *,
        priority: int = 0,
        requires: CapabilityKeysInput = None,
        provides: CapabilityKeysInput = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_success_hook(
            op,
            Stage.tx_after_success,
            hook,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    def tx_after_success_dag(
        self,
        op: StrKey | list[StrKey],
        dag: PlanDag[SuccessHookFactory],
    ) -> Self:
        return self._add_success_hook_dag(op, Stage.tx_after_success, dag)

    def after_commit(
        self,
        op: StrKey | list[StrKey],
        hook: SuccessHookFactory,
        *,
        priority: int = 0,
        requires: CapabilityKeysInput = None,
        provides: CapabilityKeysInput = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_success_hook(
            op,
            Stage.after_commit,
            hook,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    def after_commit_dag(
        self,
        op: StrKey | list[StrKey],
        dag: PlanDag[SuccessHookFactory],
    ) -> Self:
        return self._add_success_hook_dag(op, Stage.after_commit, dag)

    def explain(
        self,
        op: StrKey | OperationRef[Any, Any],
    ) -> ExecutionPlanReport:
        op_s = self._normalize_op_key(op)

        if op_s == WILDCARD or op_s.endswith(WILDCARD):
            raise CoreError("Explain on wildcard operation is not allowed")

        stages = self._merged_operation_stages(op_s)
        stages.validate()
        return build_execution_plan_report(
            plan=stages,
            op=op_s,
            registry_graph=self._dispatch_graph,
        )

    def finalize(self, operation_id_prefix: str | None = None) -> Self:
        self._raise_if_finalized()

        if operation_id_prefix is None:
            prefix = None
        else:
            prefix = str(operation_id_prefix).strip()
            if not prefix:
                raise CoreError("operation_id_prefix must be non-empty when provided")

        expanded = self._validate_dispatch_graph_for_finalize()
        self._validate_capabilities_for_finalize()

        self._finalized = True
        self._operation_id_prefix = prefix
        self._dispatch_graph.freeze(edges=expanded)

        return self

    def operation_id_for(self, op: StrKey | OperationRef[Any, Any]) -> str:
        if not self._finalized:
            raise CoreError("Registry is not finalized")

        logical = self._normalize_op_key(op)

        if self._operation_id_prefix is None:
            return logical

        return f"{self._operation_id_prefix}.{logical}"

    def resolve(
        self,
        op: StrKey | OperationRef[Any, Any],
        ctx: ExecutionContext,
        *,
        capability_execution_trace: list[Any] | None = None,
    ) -> Usecase[Any, Any]:
        op_s = self._normalize_op_key(op)
        self._assert_dispatch_edge_if_nested(op_s, ctx)

        operation_id = self.operation_id_for(op_s)
        bind_contextvars(operation_id=operation_id)

        factory = self._factories.get(op_s)
        if factory is None:
            raise CoreError(f"Usecase factory is not registered for operation: {op_s}")

        stages = self._merged_operation_stages(op_s)
        stages.validate()
        chain = ExecutionChainCompiler(
            ctx=ctx,
            capability_execution_trace=capability_execution_trace,
        ).build(stages)

        return factory(ctx).with_middlewares(*chain).with_operation_id(operation_id)

    async def dispatch(
        self,
        ctx: ExecutionContext,
        op: StrKey | OperationRef[Any, Any],
        args: Any,
    ) -> Any:
        return await self.resolve(op, ctx)(args)

    def dispatch_success_hook(
        self,
        target: StrKey | OperationRef[Any, Any],
        map_in: Callable[[Any, Any], Any],
    ) -> SuccessHookFactory:
        target_key = self._normalize_op_key(target)

        def factory(ctx: ExecutionContext) -> SuccessHook[Any, Any]:
            async def hook(args: Any, result: Any) -> None:
                await self.dispatch(ctx, target_key, map_in(args, result))
                return None

            return hook

        return factory

    @classmethod
    def merge(
        cls,
        *registries: Self,
        on_conflict: Literal["error", "overwrite"] = "error",
    ) -> Self:
        for registry in registries:
            registry._raise_if_finalized()

        prefixes = {
            registry.namespace.prefix
            for registry in registries
            if registry.namespace is not None
        }
        namespace = (
            OperationNamespace(prefix=prefixes.pop()) if len(prefixes) == 1 else None
        )
        merged = cls(namespace=namespace)

        for registry in registries:
            for op, factory in registry._factories.items():
                existing = merged._factories.get(op)

                if existing is not None and on_conflict == "error":
                    raise CoreError(
                        f"Usecase factory is already registered for operation: {op}",
                    )

                merged._factories[op] = factory

            for op, stages in registry._stages.items():
                current = merged._stages.get(op)
                if current is None:
                    merged._stages[op] = stages
                else:
                    merged._stages[op] = OperationStages.merge(current, stages)

            merged._dispatch_graph = merged._dispatch_graph.merge(
                registry._dispatch_graph
            )

        return merged
