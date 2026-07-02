"""Authoring dependency registry (compose, freeze, resolve)."""

import os
from typing import TYPE_CHECKING, Any, Self, final

import attrs

from forze.application._logger import logger as _logger

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

    from forze.base.logging import Logger

from ..interception import PortInterceptor, PortInterceptorChain
from ..tracing import RuntimeTracer, runtime_tracer_from_flag
from forze.application.contracts.deps import Deps
from .frozen import FrozenDepsRegistry
from forze.application.contracts.deps import DepsModule
from .resolution import (
    ResolutionTracer,
    resolution_tracer_from_flag,
)
from forze.application.contracts.deps import ProviderStore

# ----------------------- #

_TRUTHY_ENV = frozenset({"1", "true", "yes"})

# ....................... #


def _trace_from_env() -> bool:
    value = os.environ.get("FORZE_DEPS_TRACE", "").strip().lower()

    return value in _TRUTHY_ENV


# ....................... #


def _runtime_trace_from_env() -> bool:
    value = os.environ.get("FORZE_RUNTIME_TRACE", "").strip().lower()

    return value in _TRUTHY_ENV


# ....................... #


def _resolve_resolution_tracer(
    registry_value: ResolutionTracer | None,
    freeze_kw: bool | None,
) -> ResolutionTracer:
    if registry_value is not None:
        return registry_value

    if freeze_kw is not None:
        return resolution_tracer_from_flag(freeze_kw)

    return resolution_tracer_from_flag(_trace_from_env())


# ....................... #


def _resolve_runtime_tracer(
    registry_value: RuntimeTracer | None,
    freeze_kw: bool | None,
) -> RuntimeTracer:
    if registry_value is not None:
        return registry_value

    if freeze_kw is not None:
        return runtime_tracer_from_flag(freeze_kw)

    return runtime_tracer_from_flag(_runtime_trace_from_env())


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DepsRegistry:
    """Authoring registry for dependency providers.

    Collects :class:`DepsModule` callables and registration :class:`Deps` blobs,
    then :meth:`freeze` merges them into a :class:`FrozenDepsRegistry`.
    """

    modules: tuple[DepsModule, ...] = attrs.field(factory=tuple)
    """Modules to invoke when freezing."""

    deps: tuple[Deps, ...] = attrs.field(factory=tuple)
    """Registration deps blobs to include when freezing."""

    resolution_tracer: ResolutionTracer | None = attrs.field(default=None)
    """When set, used when resolving (overrides env and freeze kwargs)."""

    runtime_tracer: RuntimeTracer | None = attrs.field(default=None)
    """When set, used when resolving (overrides env and freeze kwargs)."""

    interceptors: PortInterceptorChain = attrs.field(factory=tuple)
    """Deps-scoped port interceptors applied to every resolved configurable port."""

    otel_port_tracer: Any = attrs.field(default=None)
    """When set (via :meth:`with_otel_port_spans`), every resolved configurable port emits a per-call
    OpenTelemetry client span through this tracer (an OTel ``Tracer``)."""

    # ....................... #

    @classmethod
    def from_modules(cls, *modules: DepsModule) -> Self:
        """Create a registry from modules."""

        return cls(modules=modules)

    # ....................... #

    @classmethod
    def from_deps(cls, *deps: Deps) -> Self:
        """Create a registry from registration deps blobs."""

        return cls(deps=deps)

    # ....................... #

    def with_modules(self, *modules: DepsModule) -> Self:
        """Return a new registry with additional modules appended."""

        _logger.trace(
            "Appending %s module(s) to deps registry with %s existing module(s)",
            len(modules),
            len(self.modules),
        )

        return attrs.evolve(self, modules=(*self.modules, *modules))

    # ....................... #

    def with_deps(self, *deps: Deps) -> Self:
        """Return a new registry with additional registration deps appended."""

        _logger.trace(
            "Appending %s deps blob(s) to registry with %s existing blob(s)",
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
        capture_values: bool = False,
    ) -> Self:
        """Return a registry that attaches tracers when :meth:`freeze` runs.

        *capture_values* (DST-only) makes the runtime tracer capture redaction-applied call values
        onto the trace for value-level invariants; off by default so production stays id-only.
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
                else runtime_tracer_from_flag(runtime, capture_values=capture_values)
            )

        return attrs.evolve(self, **updates)  # type: ignore[arg-type]

    # ....................... #

    def with_otel_port_spans(self, *, tracer: "Tracer | None" = None) -> Self:
        """Return a registry that emits a per-call OpenTelemetry **client span** for every resolved
        configurable port (a child of the operation span — see ``instrument_operations``).

        Production observability: in a hexagonal app every external call is a port, so this turns the
        port seam into a complete outbound-I/O trace. Opt in once at assembly. *tracer* defaults to the
        global ``trace.get_tracer("forze")`` (configure the OTel SDK + exporter in your app); pass one
        explicitly to target a specific provider. Independent of ``with_tracing`` (the dev DST buffer).
        """

        from opentelemetry import trace

        return attrs.evolve(self, otel_port_tracer=tracer or trace.get_tracer("forze"))

    # ....................... #

    def with_interceptors(self, *interceptors: PortInterceptor) -> Self:
        """Return a registry that wraps every resolved configurable port in *interceptors*.

        Interceptors run as an ordered chain (first = outermost) inside the resilience
        port-policy wrap. Production registers none (the port is returned bare).
        """

        return attrs.evolve(self, interceptors=(*self.interceptors, *interceptors))

    # ....................... #

    def with_port_logging(self, *, logger: "Logger | None" = None) -> Self:
        """Return a registry that logs every resolved configurable port call.

        Registers a :class:`~forze.application.execution.interception.LoggingInterceptor`
        so all outbound I/O logs uniformly ``(surface, route, op, duration)`` under
        ``forze.integrations.<domain>`` (or *logger*). Volume-safe: a successful call
        logs at ``trace`` (a no-op in production unless trace is configured). Opt in once
        at assembly, alongside ``with_otel_port_spans``.
        """

        from ..interception import LoggingInterceptor

        return self.with_interceptors(LoggingInterceptor(logger=logger))

    # ....................... #

    def freeze(
        self,
        *,
        trace_resolution: bool | None = None,
        trace_runtime: bool | None = None,
    ) -> FrozenDepsRegistry:
        """Freeze merged providers and tracer policy into a frozen registry."""

        _logger.trace(
            "Freezing dependency registry from %s module(s)",
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

        built: list[Deps] = []

        for i, module in enumerate(self.modules, 1):
            module_deps = module()
            _logger.trace(
                "Built deps module #%s with %s dependency(ies)",
                i,
                module_deps.count(),
            )
            built.append(module_deps)

        for i, dep in enumerate(self.deps, 1):
            _logger.trace(
                "Adding registration deps #%s with %s dependency(ies)",
                i,
                dep.count(),
            )
            built.append(dep)

        if not built:
            store: ProviderStore = ProviderStore()
        else:
            store = ProviderStore.merge(*(d.store for d in built))

        return FrozenDepsRegistry(
            store=store,
            resolution_tracer=resolution_tracer,
            runtime_tracer=runtime_tracer,
            interceptors=self.interceptors,
            otel_port_tracer=self.otel_port_tracer,
        )
