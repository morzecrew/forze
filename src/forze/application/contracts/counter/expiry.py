"""Counter lifetimes: the ``ttl`` the write verbs take, and the capability that says a backend
can honour one.

A counter keyed by period — a fixed-window lockout bucket, a daily quota, a monthly sequence —
is dead the moment its period ends, and a store that keeps every dead key grows with traffic
for as long as the application runs. ``ttl`` lets the writer say when that is.

Expiry is a **capability**, not a promise every backend makes. A backend that can expire keys
reports it through :class:`CounterExpiryAware`; one that cannot refuses a ``ttl`` with
``configuration`` (:data:`COUNTER_TTL_UNSUPPORTED_CODE`) rather than storing a counter that
outlives what the caller asked for. Silence reads as "cannot": a port that reports nothing is
treated as one that does not expire, the same fail-closed default the lock and stream
capabilities use.
"""

from collections.abc import Mapping
from datetime import timedelta
from typing import Final, Protocol, final, runtime_checkable

import attrs

from forze.base.exceptions import CoreException, exc

from ..deps import backend_of

# ----------------------- #

COUNTER_TTL_UNSUPPORTED_CODE: Final[str] = "counter_ttl_unsupported"
"""Error code for a ``ttl`` sent to a counter backend that cannot expire it."""

COUNTER_MAX_TTL: Final[timedelta] = timedelta(days=36_500)
"""Longest lifetime a counter takes — about a century.

Bounded because every backend adds it to a clock: past this, ``now + ttl`` leaves Python's
``datetime`` range or a store's timestamp range, and each backend would fail differently. A
counter meant to outlive this should take no ``ttl`` at all."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CounterCapabilities:
    """What a counter backend supports — its reported, fail-closed contract.

    Reported via the opt-in :class:`CounterExpiryAware` protocol. Mirrors
    :class:`~forze.application.contracts.dlock.DistributedLockCapabilities`.
    """

    expiry: bool
    """Whether the write verbs honour ``ttl``: a counter written with one reads as absent once
    it has lapsed, and its storage is reclaimed (natively, or by a documented purge)."""


# ....................... #


@runtime_checkable
class CounterExpiryAware(Protocol):
    """Opt-in extension for counter ports that report their capabilities.

    Kept separate from :class:`~forze.application.contracts.counter.CounterPort` so a backend
    opts in only when it can report. Mirrors
    :class:`~forze.application.contracts.dlock.FencingAware`.
    """

    def capabilities(self) -> CounterCapabilities:
        """Report the counter features this backend supports."""
        ...  # pragma: no cover


# ....................... #


def counter_capabilities(port: object) -> CounterCapabilities:
    """*port*'s reported capabilities, or none when it reports nothing.

    Reads the backend behind the wrappers resolution adds, which forward methods through
    ``__getattr__`` and so never satisfy a protocol check themselves.
    """

    backend = backend_of(port)
    report = getattr(backend, "capabilities", None)
    caps = report() if callable(report) else None

    if isinstance(caps, CounterCapabilities):
        return caps

    return CounterCapabilities(expiry=False)


# ....................... #


def validate_counter_ttl(ttl: timedelta | None) -> None:
    """Refuse a lifetime that has already ended, or one no backend can add to a clock.

    A zero or negative ``ttl`` would create a counter that is expired as it is written, which
    is never what a caller meant and which the backends would each read differently.

    :raises CoreException: ``precondition`` when *ttl* is not positive or exceeds
        :data:`COUNTER_MAX_TTL`.
    """

    if ttl is not None and not timedelta(0) < ttl <= COUNTER_MAX_TTL:
        raise exc.precondition(
            f"Counter ttl must be positive and at most {COUNTER_MAX_TTL!r}, got {ttl!r}"
        )


# ....................... #


def counter_ttl_unsupported(
    reason: str,
    *,
    details: Mapping[str, str] | None = None,
) -> CoreException:
    """The refusal a backend raises for a ``ttl`` it cannot honour, worded the same everywhere.

    :param details: Where the refusal applies (a table, a partition), so a caller that falls
        back on it can tell one store from another.
    """

    return exc.configuration(
        f"Counter ttl requested, but {reason}. A ttl is refused rather than ignored: a counter "
        "stored without its expiry would outlive what the caller asked for.",
        code=COUNTER_TTL_UNSUPPORTED_CODE,
        details=details,
    )
