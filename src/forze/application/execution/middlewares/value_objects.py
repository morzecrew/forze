"""Middleware protocols and implementations for usecase chains."""

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Skip:
    """Return this from a schedulable hook to skip without aborting the usecase."""

    reason: str | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Success[R]:
    """Successful usecase outcome passed to :class:`Finally` hooks."""

    value: R


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Failure:
    """Failed usecase outcome passed to :class:`Finally` hooks."""

    exc: Exception
