"""OpenTelemetry instrumentation for access-token signing and the revocation-states fast path.

The identity-plane analog of ``forze.application.execution.instrument_crypto``:
``instrument_signing`` exports each :class:`AccessTokenService`'s cumulative sign/verify
counters as always-on observable counters, so a deployment can watch token issuance and
verification rates per signing key. ``instrument_revocation_states`` does the same for the
fast path's hits, misses, fallbacks and failed write-throughs. OpenTelemetry is imported lazily,
so importing this module does not pull ``opentelemetry`` into an uninstrumented app's import
path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, get_args

from .services.access_token import SigningStats
from .services.revocation_states import RevocationCredential, RevocationEvent, RevocationStates

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

TOKENS_SIGNED_COUNTER = "forze.authn.tokens.signed"
TOKENS_VERIFIED_COUNTER = "forze.authn.tokens.verified"
TOKENS_VERIFY_FAILED_COUNTER = "forze.authn.tokens.verify_failed"

REVOCATION_HITS_COUNTER = "forze.authn.revocation_states.hits"
REVOCATION_MISSES_COUNTER = "forze.authn.revocation_states.misses"
REVOCATION_FALLBACKS_COUNTER = "forze.authn.revocation_states.fallbacks"
REVOCATION_WRITE_FAILURES_COUNTER = "forze.authn.revocation_states.write_failures"

_REVOCATION_COUNTERS: tuple[tuple[str, RevocationEvent, str], ...] = (
    (REVOCATION_HITS_COUNTER, "hit", "Verifications answered from cached states alone."),
    (REVOCATION_MISSES_COUNTER, "miss", "Verifications that read the database on a miss."),
    (
        REVOCATION_FALLBACKS_COUNTER,
        "fallback",
        "Verifications that read the database because the states store failed.",
    ),
    (
        REVOCATION_WRITE_FAILURES_COUNTER,
        "write_failure",
        "Revocations whose write-through to the states store failed every attempt.",
    ),
)

# ....................... #


def instrument_signing(
    services: dict[str, Any],
    *,
    meter: Meter | None = None,
) -> None:
    """Export each access-token service's sign/verify counters as OTel observable counters.

    *services* maps a label (e.g. ``"default"``) to anything exposing
    ``signing_stats() -> SigningStats`` — every :class:`AccessTokenService` does. Emits,
    per service (labelled ``forze.signer`` plus the issuing signer's
    ``forze.signer.algorithm`` and, when set, ``forze.signer.kid``):

    - ``forze.authn.tokens.signed`` — access tokens issued (the BYOK signer's sign count;
      for a KMS-held key this tracks ``transit/sign`` round-trips).
    - ``forze.authn.tokens.verified`` — tokens that verified successfully.
    - ``forze.authn.tokens.verify_failed`` — verifications rejected as expired/invalid; a
      rising rate is the signal that matters (key rotation gaps, clock skew, forgeries).

    Emits via the global OTel meter unless *meter* is supplied. Call once at assembly time.
    """

    from opentelemetry import metrics
    from opentelemetry.metrics import Observation

    meter = meter or metrics.get_meter("forze")

    def _observe(
        pick: Callable[[SigningStats], int],
    ) -> Callable[[CallbackOptions], Iterable[Observation]]:
        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            for label, service in services.items():
                stats: SigningStats = service.signing_stats()
                attributes: dict[str, str] = {
                    "forze.signer": label,
                    "forze.signer.algorithm": stats.algorithm,
                }

                if stats.kid is not None:
                    attributes["forze.signer.kid"] = stats.kid

                yield Observation(pick(stats), attributes)

        return callback

    meter.create_observable_counter(
        TOKENS_SIGNED_COUNTER,
        callbacks=[_observe(lambda s: s.signed)],
        unit="1",
        description="Cumulative access tokens signed.",
    )
    meter.create_observable_counter(
        TOKENS_VERIFIED_COUNTER,
        callbacks=[_observe(lambda s: s.verified)],
        unit="1",
        description="Cumulative access tokens verified successfully.",
    )
    meter.create_observable_counter(
        TOKENS_VERIFY_FAILED_COUNTER,
        callbacks=[_observe(lambda s: s.verify_failed)],
        unit="1",
        description="Cumulative access-token verifications rejected (expired/invalid).",
    )


# ....................... #


def instrument_revocation_states(
    states: Mapping[str, RevocationStates],
    *,
    meter: Meter | None = None,
) -> None:
    """Export the revocation-states fast path's counters as OTel observable counters.

    *states* maps a label (e.g. ``"default"``) to the :class:`RevocationStates` value on an
    authn kernel; every verifier and writer built from that kernel counts on it. Emits, labelled
    ``forze.revocation_states`` and ``forze.credential`` (``session`` or ``api_key``):

    - ``forze.authn.revocation_states.hits`` — verifications answered from Redis alone, a
      refusal from a cached revocation included.
    - ``forze.authn.revocation_states.misses`` — verifications that read the database for what
      Redis lacked, or held in a state it could not trust.
    - ``forze.authn.revocation_states.fallbacks`` — verifications whose Redis read failed or
      timed out, or met an open breaker, and that read the database instead.
    - ``forze.authn.revocation_states.write_failures`` — revocations whose write-through failed
      every attempt; each leaves a revoked credential admitted for up to ``ttl``.

    Emits via the global OTel meter unless *meter* is supplied. Call once at assembly time.
    """

    from opentelemetry import metrics
    from opentelemetry.metrics import Observation

    meter = meter or metrics.get_meter("forze")

    def _observe(event: RevocationEvent) -> Callable[[CallbackOptions], Iterable[Observation]]:
        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            for label, config in states.items():
                for credential in get_args(RevocationCredential):
                    yield Observation(
                        config.counters.get(event, credential),
                        {"forze.revocation_states": label, "forze.credential": credential},
                    )

        return callback

    for name, event, description in _REVOCATION_COUNTERS:
        meter.create_observable_counter(
            name,
            callbacks=[_observe(event)],
            unit="1",
            description=description,
        )
