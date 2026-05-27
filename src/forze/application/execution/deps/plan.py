import os
from typing import Any, Self, final

import attrs

from forze.application._logger import logger

from .container import Deps
from .module import DepsModule
from .resolution_tracer import (
    ResolutionTracer,
    resolution_tracer_from_flag,
)
from .runtime_tracer import RuntimeTracer, runtime_tracer_from_flag

# ----------------------- #

_TRUTHY_ENV = frozenset({"1", "true", "yes"})


def _trace_from_env() -> bool:
    value = os.environ.get("FORZE_DEPS_TRACE", "").strip().lower()

    return value in _TRUTHY_ENV


def _runtime_trace_from_env() -> bool:
    value = os.environ.get("FORZE_RUNTIME_TRACE", "").strip().lower()

    return value in _TRUTHY_ENV


def _resolve_resolution_tracer(
    plan_value: ResolutionTracer | None,
    build_kw: bool | None,
) -> ResolutionTracer:
    if plan_value is not None:
        return plan_value

    if build_kw is not None:
        return resolution_tracer_from_flag(build_kw)

    return resolution_tracer_from_flag(_trace_from_env())


def _resolve_runtime_tracer(
    plan_value: RuntimeTracer | None,
    build_kw: bool | None,
) -> RuntimeTracer:
    if plan_value is not None:
        return plan_value

    if build_kw is not None:
        return runtime_tracer_from_flag(build_kw)

    return runtime_tracer_from_flag(_runtime_trace_from_env())


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DepsPlan:
    """Declarative plan for building dependency containers.

    Collects :class:`DepsModule` callables and merges them into a single
    :class:`Deps` instance on :meth:`build`. Merging fails if any module
    registers a conflicting dependency key.

    Tracing policy (resolution and runtime recorders) is applied once on the
    final merged container via :meth:`with_tracing` and :meth:`build`.
    """

    modules: tuple[DepsModule[Any], ...] = attrs.field(factory=tuple)
    """Modules to invoke and merge when building."""

    deps: tuple[Deps[Any], ...] = attrs.field(factory=tuple)
    """Deps to include in the plan."""

    resolution_tracer: ResolutionTracer | None = attrs.field(default=None)
    """When set, used for the built container (overrides env and build kwargs)."""

    runtime_tracer: RuntimeTracer | None = attrs.field(default=None)
    """When set, used for the built container (overrides env and build kwargs)."""

    # ....................... #

    @classmethod
    def from_modules(cls, *modules: DepsModule[Any]) -> Self:
        """Create a plan from modules.

        :param modules: Modules to include.
        :returns: New plan instance.
        """

        return cls(modules=modules)

    # ....................... #

    @classmethod
    def from_deps(cls, *deps: Deps[Any]) -> Self:
        """Create a plan from deps.

        :param deps: Deps to include.
        :returns: New plan instance.
        """

        return cls(deps=deps)

    # ....................... #

    def with_modules(self, *modules: DepsModule[Any]) -> Self:
        """Return a new plan with additional modules appended.

        :param modules: Modules to append.
        :returns: New plan instance.
        """

        logger.trace(
            "Appending %s module(s) to deps plan with %s existing module(s)",
            len(modules),
            len(self.modules),
        )

        return attrs.evolve(self, modules=(*self.modules, *modules))

    # ....................... #

    def with_deps(self, *deps: Deps[Any]) -> Self:
        """Return a new plan with additional deps appended.

        :param deps: Deps to append.
        :returns: New plan instance.
        """

        logger.trace(
            "Appending %s deps to deps plan with %s existing deps",
            len(deps),
            len(self.deps),
        )

        return attrs.evolve(self, deps=(*self.deps, *deps))

    # ....................... #

    def with_tracing(
        self,
        *,
        resolution: bool | ResolutionTracer | None = None,
        runtime: bool | RuntimeTracer | None = None,
    ) -> Self:
        """Return a plan that attaches tracers when :meth:`build` runs.

        :param resolution: ``True``/``False`` for recording/noop, or a tracer instance.
        :param runtime: ``True``/``False`` for recording/noop, or a tracer instance.
        """

        updates: dict[str, ResolutionTracer | RuntimeTracer] = {}

        if resolution is not None:
            updates["resolution_tracer"] = (
                resolution
                if isinstance(resolution, ResolutionTracer)
                else resolution_tracer_from_flag(resolution)
            )

        if runtime is not None:
            updates["runtime_tracer"] = (
                runtime
                if isinstance(runtime, RuntimeTracer)
                else runtime_tracer_from_flag(runtime)
            )

        return attrs.evolve(self, **updates)  # type: ignore[arg-type]

    # ....................... #

    def build(
        self,
        *,
        trace_resolution: bool | None = None,
        trace_runtime: bool | None = None,
    ) -> Deps[Any]:
        """Build a merged dependency container from all modules.

        :param trace_resolution: When ``True``, enable observed resolution tracing.
            When ``None`` (default), enable if ``FORZE_DEPS_TRACE`` is set to a
            truthy value (``1``, ``true``, ``yes``), unless :attr:`resolution_tracer`
            is set on the plan.
        :param trace_runtime: When ``True``, enable runtime tracing.
            When ``None`` (default), enable if ``FORZE_RUNTIME_TRACE`` is set to a
            truthy value (``1``, ``true``, ``yes``), unless :attr:`runtime_tracer`
            is set on the plan.
        :returns: Merged :class:`Deps` instance.
        """

        logger.trace(
            "Building dependency container from %s module(s)",
            len(self.modules),
        )

        resolution_tracer = _resolve_resolution_tracer(
            self.resolution_tracer,
            trace_resolution,
        )
        runtime_tracer = _resolve_runtime_tracer(
            self.runtime_tracer,
            trace_runtime,
        )

        if not self.modules and not self.deps:
            logger.trace("Deps plan is empty; returning empty container")

            return Deps[Any](
                resolution_tracer=resolution_tracer,
                runtime_tracer=runtime_tracer,
            )

        built: list[Deps[Any]] = []

        # 1. build modules
        for i, module in enumerate(self.modules, 1):
            deps = module()
            logger.trace(
                "Built deps module #%s with %s dependency(ies)",
                i,
                deps.count(),
            )
            built.append(deps)

        # 2. add pre-built deps
        for i, dep in enumerate(self.deps, 1):
            logger.trace(
                "Adding deps #%s with %s dependency(ies)",
                i,
                dep.count(),
            )
            built.append(dep)

        merged = Deps[Any].merge(
            *built,
            resolution_tracer=resolution_tracer,
            runtime_tracer=runtime_tracer,
        )

        return merged
