"""Dependency key value object."""

from typing import TypeVar, final

import attrs

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DepKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is used for diagnostics and error messages; type information
    is carried through the type parameter ``T`` for static resolution.
    """

    name: str
    """Human-readable name for diagnostics and error messages."""
