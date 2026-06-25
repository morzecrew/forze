"""Shared PEP 562 lazy-export machinery for curated package front doors.

A package root re-exports a small, curated set of names so callers can write
``from forze import DocumentSpec`` without importing the whole subtree eagerly.
:func:`lazy_exports` builds the ``__getattr__`` / ``__dir__`` pair from a
``name -> source module`` map, so each front door stays a thin declaration and the
two roots (``forze``, ``forze_kits``) can't drift in behaviour.

Lives in ``forze.base`` (the lowest layer) so both core and kits may import it, and
stays dependency-free so ``import forze`` pays only this module's trivial cost.
"""

import importlib
from typing import Any, Callable

# ----------------------- #


def lazy_exports(
    module_name: str, exports: dict[str, str]
) -> tuple[Callable[[str], Any], Callable[[], list[str]]]:
    """Build the ``__getattr__`` / ``__dir__`` pair for a lazy package front door.

    :param module_name: The importing module's ``__name__`` (for the error message).
    :param exports: Curated ``exported name -> source module path`` mapping.
    :returns: ``(__getattr__, __dir__)`` to assign at module level.
    """

    def __getattr__(name: str) -> Any:
        module = exports.get(name)

        if module is None:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}")

        return getattr(importlib.import_module(module), name)

    def __dir__() -> list[str]:
        return sorted(exports)

    return __getattr__, __dir__
