"""Per-backend inference capabilities + fail-closed validators.

The inference port presents one surface, but backends diverge on what they can serve: an
in-process model vectorizes a batch in one call while a remote endpoint may accept only one
instance per request; some backends can stream chunked scoring, others cannot; only some can
run offline batch jobs. :class:`InferenceCapabilities` makes that surface **declarative**
(mirroring :class:`~forze.application.contracts.search.capabilities.SearchCapabilities`): each
adapter publishes what it can serve, and a request that strays is rejected up front with a
clean :func:`~forze.base.exceptions.exc.precondition` (code ``inference_feature_unsupported``)
naming the feature and backend — never a silent degradation. The in-memory mock is the
canonical superset (:data:`FULL_INFERENCE_CAPABILITIES`).
"""

from typing import Final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

UNSUPPORTED_INFERENCE_FEATURE_CODE = "inference_feature_unsupported"
"""Error code raised when a request uses an inference feature the backend lacks."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InferenceCapabilities:
    """What an inference adapter can serve, declared per backend.

    Defaults describe the narrowest surface — a single-instance request/response backend:
    ``predict_many`` loops adapter-side, no chunked streaming, no offline jobs, no
    determinism promise. Richer adapters widen the surface via :func:`attrs.evolve`.
    """

    native_batch: bool = False
    """Whether ``predict_many`` is served as one backend call (vectorized) rather than an
    adapter-side loop over single-instance requests. Informational — both are honored —
    but a caller sizing batches can use it to reason about cost and latency."""

    max_batch_size: int | None = None
    """Hard per-call instance cap imposed by the backend, or ``None`` for unbounded. An
    adapter with a cap refuses an oversized ``predict_many`` up front (precondition) rather
    than silently splitting a call the caller asked to be atomic (all-or-nothing)."""

    supports_stream: bool = False
    """Whether ``predict_stream`` (bounded-memory chunked scoring) is honored. Backends
    that cannot serve it refuse the stream method up front instead of buffering."""

    supports_async_jobs: bool = False
    """Whether this backend can run offline batch jobs (submit / status over object-storage
    locations). Declared now; the batch job plane ships separately and is gated on it."""

    deterministic: bool = False
    """Whether the backend promises the same output for the same input (a classical model,
    or a seeded temperature-zero configuration). Relevant to simulation oracles — a
    sampling model must leave it off."""


# ....................... #

FULL_INFERENCE_CAPABILITIES: Final[InferenceCapabilities] = InferenceCapabilities(
    native_batch=True,
    max_batch_size=None,
    supports_stream=True,
    supports_async_jobs=True,
    deterministic=True,
)
"""The canonical full surface every backend is a subset of.

The in-memory mock is the reference: it scores batches in one pure-function call, streams
chunks exactly, and is deterministic by construction, so it advertises the superset.
"""

DEFAULT_INFERENCE_CAPABILITIES: Final[InferenceCapabilities] = InferenceCapabilities()
"""The plain single-instance request/response surface (all off) — the default a backend
overrides only when it serves more."""


# ....................... #


def _inference_cap_fail(backend: str, feature: str) -> None:
    raise exc.precondition(
        f"Inference feature {feature} is not supported by the {backend!r} backend.",
        code=UNSUPPORTED_INFERENCE_FEATURE_CODE,
    )


def validate_stream_supported(caps: InferenceCapabilities, *, backend: str) -> None:
    """Raise cleanly if chunked streaming is asked of a *backend* that cannot serve it.

    Call it at the top of ``predict_stream`` (before consuming the first chunk) so an
    incapable backend refuses up front instead of failing partway through iteration.
    """

    if not caps.supports_stream:
        _inference_cap_fail(backend, "chunked streaming (predict_stream)")


def validate_batch_size(
    caps: InferenceCapabilities,
    size: int,
    *,
    backend: str,
) -> None:
    """Raise cleanly if a ``predict_many`` batch exceeds the backend's hard cap.

    ``predict_many`` is all-or-nothing, so an oversized batch is refused whole rather
    than silently split into several backend calls the caller did not ask for.
    """

    if caps.max_batch_size is not None and size > caps.max_batch_size:
        _inference_cap_fail(
            backend,
            f"a batch of {size} instances (cap {caps.max_batch_size})",
        )


# ....................... #

__all__ = [
    "DEFAULT_INFERENCE_CAPABILITIES",
    "FULL_INFERENCE_CAPABILITIES",
    "InferenceCapabilities",
    "UNSUPPORTED_INFERENCE_FEATURE_CODE",
    "validate_batch_size",
    "validate_stream_supported",
]
