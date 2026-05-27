import os
from typing import Any, Self, final

import attrs

from forze.application._logger import logger

from .container import Deps
from .module import DepsModule

# ----------------------- #

_TRUTHY_ENV = frozenset({"1", "true", "yes"})


def _trace_from_env() -> bool:
    value = os.environ.get("FORZE_DEPS_TRACE", "").strip().lower()

    return value in _TRUTHY_ENV


def _runtime_trace_from_env() -> bool:
    value = os.environ.get("FORZE_RUNTIME_TRACE", "").strip().lower()

    return value in _TRUTHY_ENV


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DepsPlan:
    """Declarative plan for building dependency containers.

    Collects :class:`DepsModule` callables and merges them into a single
    :class:`Deps` instance on :meth:`build`. Merging fails if any module
    registers a conflicting dependency key.
    """

    modules: tuple[DepsModule[Any], ...] = attrs.field(factory=tuple)
    """Modules to invoke and merge when building."""

    deps: tuple[Deps[Any], ...] = attrs.field(factory=tuple)
    """Deps to include in the plan."""

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

    def build(
        self,
        *,
        trace_resolution: bool | None = None,
        trace_runtime: bool | None = None,
    ) -> Deps[Any]:
        """Build a merged dependency container from all modules.

        :param trace_resolution: When ``True``, enable observed resolution tracing.
            When ``None`` (default), enable if ``FORZE_DEPS_TRACE`` is set to a
            truthy value (``1``, ``true``, ``yes``).
        :param trace_runtime: When ``True``, enable runtime tracing.
            When ``None`` (default), enable if ``FORZE_RUNTIME_TRACE`` is set to a
            truthy value (``1``, ``true``, ``yes``).
        :returns: Merged :class:`Deps` instance.
        """

        logger.trace(
            "Building dependency container from %s module(s)",
            len(self.modules),
        )

        enable_trace = (
            _trace_from_env() if trace_resolution is None else trace_resolution
        )
        enable_runtime_trace = (
            _runtime_trace_from_env() if trace_runtime is None else trace_runtime
        )

        if not self.modules:
            logger.trace("Deps plan is empty; returning empty container")
            return Deps[Any](
                trace_resolution=enable_trace,
                trace_runtime=enable_runtime_trace,
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

        merged = Deps[Any].merge(*built)

        if enable_trace and not merged.trace_resolution:
            merged = attrs.evolve(merged, trace_resolution=True)

        if enable_runtime_trace and not merged.trace_runtime:
            merged = attrs.evolve(merged, trace_runtime=True)

        return merged
