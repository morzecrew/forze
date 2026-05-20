from typing import Mapping, final

import attrs

from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Skip:
    """Base value object for result of skipped execution."""

    reason: str | None = None
    """Reason for skipping the execution."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Success[R]:
    """Base value object for result of successful execution."""

    value: R
    """Result of the successful execution."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Failure:
    """Base value object for result of failed execution."""

    exc: Exception
    """Exception that caused the failure."""


# ....................... #

type Outcome[R] = Success[R] | Failure
"""Union type for the result of execution."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Graph[G]:
    """Execution graph."""

    steps: Mapping[StrKey, G] = attrs.field(factory=dict[StrKey, G])
    """Steps for this graph."""

    waves: tuple[tuple[StrKey, ...], ...] = attrs.field(factory=tuple)
    """Waves for this graph."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Pipeline[P]:
    """Execution pipeline."""

    steps: tuple[P, ...] = attrs.field(factory=tuple)
    """Steps for this pipeline."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Step:
    """Execution step."""

    id: StrKey
    """Unique identifier for the step."""

    priority: int
    """Priority of the step."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphStep(Step):
    """Graph step."""

    requires: tuple[StrKey, ...] = attrs.field(factory=tuple)
    """Capabilities required by this step."""

    provides: tuple[StrKey, ...] = attrs.field(factory=tuple)
    """Capabilities provided by this step."""

    depends_on: tuple[StrKey, ...] = attrs.field(factory=tuple)
    """Other steps IDs this step depends on."""
