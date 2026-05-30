from typing import Protocol

from .container import Deps

# ----------------------- #


class DepsModule(Protocol):
    """Protocol for a module that returns a dependency container.

    Callables are invoked to produce a :class:`Deps` instance; multiple
    modules are merged via :meth:`Deps.merge` when building a plan.
    """

    def __call__(self) -> Deps:
        """Return a dependency container."""
        ...
