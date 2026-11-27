"""Authoring dependency registry (compose, freeze, resolve)."""

import inspect
import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Self, cast, final

import attrs

from forze.application._logger import logger as _logger

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

    from forze.base.logging import Logger

from forze.application.contracts.deps import (
    DeclaresRequirements,
    Deps,
    DepsModule,
    ProviderStore,
)
from forze.base.exceptions import exc

from ..interception import PortInterceptor, PortInterceptorChain
from ..posture import PostureAware, WiringFinding
from ..tracing import RuntimeTracer, runtime_tracer_from_flag
from .frozen import FrozenDepsRegistry
from .resolution import (
    ResolutionTracer,
    resolution_tracer_from_flag,
)

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


def _declares(provider: object, protocol: type, method: str) -> bool:
    """Whether *provider* implements *protocol*: an instance whose attribute of that name is
    a method bound to it. The protocol check reads only the attribute's name, so a class
    registered as a factory, which carries the method unbound, declares nothing, and nor
    does a field of that name, even one holding a callable."""

    if isinstance(provider, type) or not isinstance(provider, protocol):
        return False

    bound = getattr(provider, method, None)

    return inspect.ismethod(bound) and bound.__self__ is provider


# ....................... #


def _refuse_unbound_requirements(store: ProviderStore) -> None:
    """Refuse a store where a registered provider declares a requirement nothing satisfies.

    A requirement is satisfied as resolution would satisfy it: its route is registered for the
    key, or a plain registration of the key is. The spec inventory is not consulted; it is
    attached after the freeze, and reconciliation reports a route it refuses.

    :raises CoreException: ``configuration`` (``dependency_requirement_unbound``), naming every
        unbound frame by key and route, then each remedy once.
    """

    unbound: set[str] = set()
    remedies: dict[str, None] = {}

    for key, route, provider in store.providers():
        if not _declares(provider, DeclaresRequirements, "requirements"):
            continue

        for requirement in cast(DeclaresRequirements, provider).requirements():
            if store.exists(requirement.key, route=requirement.route) or store.exists(
                requirement.key
            ):
                continue

            where = f"on route '{route}'" if route is not None else "as a plain registration"
            unbound.add(
                f"'{key.name}' {where} resolves '{requirement.key.name}' on route "
                f"'{requirement.route}', which is not registered and has no plain fallback."
            )
            remedies.setdefault(requirement.remedy)

    if unbound:
        raise exc.configuration(
            " ".join((*sorted(unbound), *remedies)),
            code="dependency_requirement_unbound",
        )


# ....................... #


def _provider_findings(store: ProviderStore) -> Iterator[WiringFinding]:
    """The posture findings registered providers report, for wiring passed as built ``Deps``,
    which no module carries to the freeze."""

    for _, _, provider in store.providers():
        if _declares(provider, PostureAware, "posture_findings"):
            yield from cast(PostureAware, provider).posture_findings()


# ....................... #


def _report_fallbacks(store: ProviderStore) -> None:
    """Log the fallback report for a hybrid wiring (fallback + real modules in one context).

    Only a genuinely hybrid store logs: a pure-mock context (everything fallback) and a
    production one (nothing fallback) are silent. This is the visibility that keeps the
    relaxed merge honest — a route the real module never registered is served by the
    fallback instead of failing, so the context says so at freeze rather than never.
    """

    report = store.fallback_report()

    if not report.hybrid:
        return

    # The catch-all set is named in full at info: those are the keys where a mistyped route
    # resolves to the fallback instead of failing. The bulk fallback-served list (every
    # plane nobody wired for real — ~80 keys under the mock) stays at debug.
    _logger.info(
        "Hybrid deps wiring: %s fallback-served entry(ies), %s shadowed by real modules%s%s",
        len(report.served_names()),
        len(report.shadowed),
        f"; shadowed: {', '.join(report.shadowed_names())}" if report.shadowed else "",
        (
            f"; catch-all behind real routes: {', '.join(report.catch_all_names())}"
            if report.catch_all
            else ""
        ),
    )
    _logger.debug("Hybrid deps wiring detail:\n%s", report.describe())


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
        # Read once, here, while the modules are still in hand: the frozen registry keeps
        # providers, not modules, and both posture gates read from what it keeps.
        findings = tuple(
            finding
            for module in self.modules
            if isinstance(module, PostureAware)
            for finding in module.posture_findings()
        )

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

        _refuse_unbound_requirements(store)
        # A module and the providers it registered may report one finding; keep the first.
        unique: dict[tuple[str, str], WiringFinding] = {}

        for finding in (*findings, *_provider_findings(store)):
            unique.setdefault((finding.target, finding.rule), finding)

        findings = tuple(unique.values())
        _report_fallbacks(store)

        return FrozenDepsRegistry(
            store=store,
            resolution_tracer=resolution_tracer,
            runtime_tracer=runtime_tracer,
            interceptors=self.interceptors,
            otel_port_tracer=self.otel_port_tracer,
            posture_findings=findings,
        )
