"""OpenTelemetry instrumentation for the grants-cache broadcast.

``instrument_grants_invalidation`` exports each :class:`GrantsInvalidation`'s cumulative counters
as always-on observable counters, as ``forze_identity.authn.instrument_revocation_states`` does
for the revocation fast path. OpenTelemetry is imported lazily, so importing this module does not
pull ``opentelemetry`` into an uninstrumented app's import path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING

from .services.grants_cache import GrantsInvalidation, GrantsInvalidationEvent

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

PUBLISH_FAILURES_COUNTER = "forze.authz.grants_invalidation.publish_failures"
FLUSHES_COUNTER = "forze.authz.grants_invalidation.flushes"

_COUNTERS: tuple[tuple[str, GrantsInvalidationEvent, str], ...] = (
    (
        PUBLISH_FAILURES_COUNTER,
        "publish_failure",
        "Committed grants invalidations whose broadcast write failed.",
    ),
    (
        FLUSHES_COUNTER,
        "flush",
        "Whole grants caches dropped on a stream gap or a catalog change.",
    ),
)

# ....................... #


def instrument_grants_invalidation(
    invalidations: Mapping[str, GrantsInvalidation],
    *,
    meter: Meter | None = None,
) -> None:
    """Export the grants-cache broadcast's counters as OTel observable counters.

    *invalidations* maps a label (e.g. ``"default"``) to the :class:`GrantsInvalidation` on an
    authz kernel. Emits, labelled ``forze.grants_invalidation``:

    - ``forze.authz.grants_invalidation.publish_failures`` — committed invalidations whose marker
      write failed; each leaves other processes on the old grants for up to the cache's TTL.
    - ``forze.authz.grants_invalidation.flushes`` — whole grants caches this process dropped, on
      a gap in the stream or a catalog change. A steady rate means a flapping connection: the
      cache stays correct and stops helping.

    Emits via the global OTel meter unless *meter* is supplied. Call once at assembly time.
    """

    from opentelemetry import metrics
    from opentelemetry.metrics import Observation

    meter = meter or metrics.get_meter("forze")

    def _observe(
        event: GrantsInvalidationEvent,
    ) -> Callable[[CallbackOptions], Iterable[Observation]]:
        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            for label, invalidation in invalidations.items():
                yield Observation(
                    invalidation.counters.get(event),
                    {"forze.grants_invalidation": label},
                )

        return callback

    for name, event, description in _COUNTERS:
        meter.create_observable_counter(
            name,
            callbacks=[_observe(event)],
            unit="1",
            description=description,
        )
