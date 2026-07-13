"""OpenTelemetry instrumentation for access-token signing.

The identity-plane analog of ``forze.application.execution.instrument_crypto``:
``instrument_signing`` exports each :class:`AccessTokenService`'s cumulative sign/verify
counters as always-on observable counters, so a deployment can watch token issuance and
verification rates per signing key. OpenTelemetry is imported lazily, so importing this
module does not pull ``opentelemetry`` into an uninstrumented app's import path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from .services.access_token import SigningStats

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

TOKENS_SIGNED_COUNTER = "forze.authn.tokens.signed"
TOKENS_VERIFIED_COUNTER = "forze.authn.tokens.verified"
TOKENS_VERIFY_FAILED_COUNTER = "forze.authn.tokens.verify_failed"

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
