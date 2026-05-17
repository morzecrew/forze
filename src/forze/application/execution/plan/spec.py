"""Middleware and transaction specifications."""

from enum import StrEnum
from typing import Iterable, final

import attrs

from .types import MiddlewareFactory

# ----------------------- #


def frozenset_capability_keys(
    values: frozenset[str] | set[str] | Iterable[str | StrEnum] | None,
) -> frozenset[str]:
    """Normalize ``requires`` / ``provides`` inputs to a ``frozenset[str]``."""

    if values is None:
        return frozenset()

    if isinstance(values, frozenset):
        return frozenset(str(x) for x in values)

    if isinstance(values, set):
        return frozenset(str(x) for x in values)

    return frozenset(str(x) for x in values)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareSpec:
    """Specification for a middleware attached to an operation plan."""

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    factory: MiddlewareFactory
    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    step_label: str | None = None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TransactionSpec:
    """Specification for a transaction attached to an operation plan."""

    route: str | StrEnum
