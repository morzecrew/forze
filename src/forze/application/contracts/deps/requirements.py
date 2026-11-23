"""What a provider resolves when it is called, declared so a registry can refuse wiring that lacks it."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, final, runtime_checkable

import attrs

from forze.base.primitives import StrKey

from .keys import DepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Requirement:
    """A dependency a provider resolves when it is called: a key, on a route.

    It is satisfied when the route is registered for the key, or a plain registration of the key
    is, the lookup resolution falls back to. ``DepsRegistry.freeze`` refuses wiring that leaves one
    unsatisfied, so a provider that could never be built fails the boot instead of every call.
    """

    key: DepKey[Any]
    """The key the provider resolves."""

    route: StrKey
    """The route it resolves the key on."""

    remedy: str
    """What to wire instead, for the refusal. Names keys and choices, never values."""


# ....................... #


@runtime_checkable
class DeclaresRequirements(Protocol):
    """A provider that names the dependencies it resolves when called.

    The registry reads them once, when it is frozen. A class registered as a factory does
    not declare: only an instance of one does.
    """

    def requirements(self) -> Iterable[Requirement]:
        """The dependencies this provider resolves when called."""
        ...  # pragma: no cover
