"""Resolve a :class:`~forze_dst.Simulation` to drive, from what the user points at.

The CLI's "point at your code" contract is forgiving:

* ``module:attr`` imports ``module`` and reads ``attr`` — a ``Simulation``, a
  ``FrozenOperationRegistry``, or a zero-arg callable returning either;
* ``module`` alone *discovers* the single ``Simulation`` (preferred) or registry the module
  exposes.

A bare registry is wrapped into a ``Simulation`` on the fly with an auto-mocking
``MockDepsModule`` (every port stubbed) and no invariants — enough to inspect the topology /
derived scenario and smoke-run operations. For asserted bugs and custom wiring, expose a
``Simulation`` with your deps and invariants.

The current working directory is put on ``sys.path`` so import strings resolve against the
project you run ``forze`` in.
"""

from __future__ import annotations

import importlib
import os
import sys
from typing import Any

from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze_dst import Simulation

# ----------------------- #


def _ensure_cwd_importable() -> None:
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


# ....................... #


def _coerce(obj: Any) -> Simulation | None:
    """A Simulation as-is, or a bare registry wrapped with an auto-mocking deps module."""

    if isinstance(obj, Simulation):
        return obj

    if isinstance(obj, FrozenOperationRegistry):
        from forze_dst import no_unexpected_error
        from forze_mock import MockDepsModule

        # Auto-mock every port + a zero-instrumentation safety net: a bare registry still
        # gets checked for operations that crash under concurrency/faults. Domain invariants
        # need a Simulation.
        return Simulation(
            operations=obj,
            deps=MockDepsModule,
            invariants=[no_unexpected_error()],
        )

    return None


# ....................... #


def load_object(ref: str) -> Any:
    """Import and return the object named by ``module:attr`` (``attr`` may be dotted)."""

    _ensure_cwd_importable()

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


def _discover(module_path: str) -> Simulation:
    """Find the single Simulation (preferred) or registry a module exposes."""

    _ensure_cwd_importable()
    module = importlib.import_module(module_path)
    public = [value for name, value in vars(module).items() if not name.startswith("_")]

    simulations = [value for value in public if isinstance(value, Simulation)]

    if len(simulations) == 1:
        return simulations[0]

    if len(simulations) > 1:
        raise ValueError(
            f"{module_path!r} exposes several Simulations — name one with 'module:attr'"
        )

    registries = [
        value for value in public if isinstance(value, FrozenOperationRegistry)
    ]

    if len(registries) == 1:
        return _coerce(registries[0])  # type: ignore[return-value]  # not None for a registry

    if len(registries) > 1:
        raise ValueError(
            f"{module_path!r} exposes several registries — name one with 'module:attr'"
        )

    raise ValueError(
        f"{module_path!r} exposes no Simulation or FrozenOperationRegistry; "
        "expose one, or point at it with 'module:attribute'"
    )


# ....................... #


def load_simulation(ref: str) -> Simulation:
    """Resolve *ref* to a :class:`~forze_dst.Simulation` (see the module docstring)."""

    if ":" not in ref:
        return _discover(ref)

    obj = load_object(ref)

    simulation = _coerce(obj)

    if simulation is not None:
        return simulation

    if callable(obj):
        simulation = _coerce(obj())

        if simulation is not None:
            return simulation

    raise TypeError(
        f"{ref!r} is not a Simulation or FrozenOperationRegistry "
        "(or a zero-arg callable returning one)"
    )
