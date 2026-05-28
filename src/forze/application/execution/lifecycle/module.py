from typing import Protocol

from forze.application.contracts.execution import LifecycleStep

# ----------------------- #


class LifecycleModule(Protocol):
    """Protocol for a module that returns lifecycle steps.

    Callables are invoked when building a :class:`LifecyclePlan`; multiple
    modules are merged and topologically ordered via :meth:`LifecyclePlan.build`.
    """

    def __call__(self) -> tuple[LifecycleStep, ...]:
        """Return lifecycle steps contributed by this module."""
        ...
