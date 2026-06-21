"""Workload value types shared by the harness facade and its engines.

Small, dependency-free data classes lifted out of the harness so the engine modules can build
and consume workloads without importing the facade (which would be a cycle): an
:class:`OperationCase` is the public knob the ``OP_CASE`` strategy picks from, and a :class:`_Call`
is one concrete generated invocation (op + built input).
"""

from __future__ import annotations

import random
from typing import Any, Callable, final

import attrs

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class OperationCase:
    """One operation the workload may pick: its key, selection weight, and input source."""

    op: str
    """The operation to pick."""

    weight: float = 1.0
    """The weight of the operation."""

    inputs: Callable[[random.Random], Any] | None = None
    """Build an input for this op from a seeded RNG. ``None`` → auto-generate from the
    operation's declared ``input_type`` (``None`` input if it declares none)."""


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class Call:
    """One concrete generated invocation: an operation and its built argument (package-internal)."""

    op: str
    arg: Any
