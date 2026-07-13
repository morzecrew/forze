"""Volume control for per-request access logs (the largest steady-state log source).

A transport's access-log middleware logs once per request; on a busy service that is
the dominant volume. This sampler makes that quiet by default without hiding what
matters: health/readiness probes are dropped entirely, error responses are always
logged, and successful responses are sampled 1-in-N. Transports own the counter; the
policy is shared so FastAPI and MCP behave the same.
"""

from enum import StrEnum
from typing import Final, final

import attrs

# ----------------------- #

DEFAULT_HEALTH_PATHS: Final[frozenset[str]] = frozenset(
    {
        "/health",
        "/healthz",
        "/livez",
        "/liveness",
        "/live",
        "/readyz",
        "/readiness",
        "/ready",
        "/ping",
        "/metrics",
    }
)
"""Probe/scrape paths excluded from access logs by default — pure noise in steady state."""


@final
class AccessLogMode(StrEnum):
    """How much per-request access logging to emit."""

    FULL = "full"
    """Log every request (except excluded subjects)."""

    SAMPLED = "sampled"
    """Log every error, and one in ``sample_rate`` successful requests."""

    OFF = "off"
    """Emit no access logs at all."""


# ....................... #


@attrs.define(slots=True, kw_only=True, eq=False)
class AccessLogSampler:
    """Decide whether a given request should be access-logged.

    Stateful (holds the sampling counter), so a transport middleware constructs one and
    calls :meth:`should_log` per request. Defaults to the quiet policy: sampled, errors
    always kept, probes excluded by the caller-supplied *exclude* set.
    """

    mode: AccessLogMode = attrs.field(default=AccessLogMode.SAMPLED, converter=AccessLogMode)
    """Access-log volume mode (a plain ``"full"`` / ``"sampled"`` / ``"off"`` string is coerced)."""

    sample_rate: int = 10
    """In ``sampled`` mode, keep one in this many successful requests (``<=1`` keeps all)."""

    always_log_errors: bool = True
    """Always log error responses, even under sampling (never drop a failure)."""

    exclude: frozenset[str] = frozenset()
    """Subjects (request paths, method names) never logged — e.g. health probes."""

    _count: int = attrs.field(default=0, init=False)

    # ....................... #

    def should_log(self, *, subject: str | None, is_error: bool) -> bool:
        """Whether to log a request identified by *subject* with *is_error* outcome."""

        if self.mode is AccessLogMode.OFF:
            return False

        if subject is not None and subject in self.exclude:
            return False

        if is_error and self.always_log_errors:
            return True

        if self.mode is AccessLogMode.FULL:
            return True

        # Sampled: a rate of 1 (or less) keeps every request; otherwise keep the
        # first of every ``sample_rate`` successful requests.
        if self.sample_rate <= 1:
            return True

        self._count += 1

        return self._count % self.sample_rate == 1
