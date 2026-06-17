"""Resolve a user object from a ``module:attribute`` import string.

The CLI's "point at your code" contract: ``myapp.sim:simulation`` imports ``myapp.sim`` and
reads its ``simulation`` attribute (dotted attributes supported). The framework has no app to
run, so everything is driven from objects the user exposes — typically a
:class:`~forze_dst.Simulation` (or a zero-arg callable that builds one).
"""

from __future__ import annotations

import importlib
from typing import Any

from forze_dst import Simulation

# ----------------------- #


def load_object(ref: str) -> Any:
    """Import and return the object named by ``module:attr`` (``attr`` may be dotted)."""

    module_path, separator, attr_path = ref.partition(":")

    if not separator or not module_path or not attr_path:
        raise ValueError(
            f"expected an import string of the form 'module:attribute', got {ref!r}"
        )

    obj: Any = importlib.import_module(module_path)

    for name in attr_path.split("."):
        obj = getattr(obj, name)

    return obj


# ....................... #


def load_simulation(ref: str) -> Simulation:
    """Load a :class:`~forze_dst.Simulation` from *ref* (or a callable that returns one)."""

    obj = load_object(ref)

    if isinstance(obj, Simulation):
        return obj

    if callable(obj):
        built = obj()
        if isinstance(built, Simulation):
            return built

    raise TypeError(
        f"{ref!r} is not a forze_dst.Simulation (or a zero-arg callable returning one)"
    )
