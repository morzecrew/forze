"""The inference capability differential — is the oracle told the surface it is standing in for?

Every other plane's differential asks whether the mock *behaves* like the real backend. This
one asks something narrower and sharper: whether the mock **refuses** what the real backend
refuses. The inference port is declarative — each adapter publishes an
:class:`~forze.application.contracts.inference.InferenceCapabilities` and the shared
validators gate requests against it — so "does this call get through?" is a property of the
declaration, not of the model behind it.

That makes the oracle's default the whole problem. ``MockInferenceAdapter`` advertises
:data:`~forze.application.contracts.inference.FULL_INFERENCE_CAPABILITIES` when a route
registers none: unbounded batches, streaming, async jobs, deterministic. It is not lying —
the mock really does serve all of that — but it out-capables every real adapter it stands in
for, so a capability gate that passes against the oracle can still refuse in production.
The registration seam exists to fix that (``MockInferenceRegistry.on(..., capabilities=…)``),
and this scenario is what makes forgetting it fail somewhere other than production.

:func:`run_capability_gates` drives four requests and reduces each to
:class:`GateVerdict` — ``ACCEPTED`` or ``REFUSED``, nothing about predictions. Reducing to a
verdict is what makes the comparison possible at all: two backends running different models
return different numbers, so outputs cannot be compared, while *admission* can. Run it
against a real adapter and against a mock built with that adapter's own declaration and the
two must agree gate for gate; run it against a mock that was never told, and the whole point
is that they must not.

Note ``REFUSED`` means the capability gate refused, specifically — any other failure
propagates. A scenario that swallowed every exception into ``REFUSED`` would report a broken
route as a working gate, which is the one wrong answer that would make all of this useless.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from enum import StrEnum
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import (
    UNSUPPORTED_INFERENCE_FEATURE_CODE,
    InferencePort,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class GateVerdict(StrEnum):
    """Whether a capability-gated request was admitted, normalised across backends."""

    ACCEPTED = "accepted"
    REFUSED = "refused"


# ....................... #


@attrs.frozen(kw_only=True)
class CapabilityGateOutcome:
    """One verdict per gated request — the comparable surface of an inference backend."""

    within_cap_predict_many: GateVerdict
    """The positive control. A backend that refuses everything would otherwise look
    identical to one enforcing its cap correctly, and both would pass a test that only
    checked the refusals."""

    oversized_predict_many: GateVerdict
    """A batch past the declared cap. ``predict_many`` is all-or-nothing, so this is
    refused whole rather than split into calls the caller never asked for."""

    stream: GateVerdict
    """Chunked streaming at all, gated on ``supports_stream``."""

    oversized_stream_chunk: GateVerdict
    """A stream chunk past the same cap. This one must be **served**: the cap is an
    atomicity promise for ``predict_many`` only, and a chunk is a bounded-memory
    convenience, so a backend that refuses here is stricter than the contract and code
    written against the contract fails only against it."""


# ----------------------- #


async def _one_chunk(instances: Sequence[BaseModel]) -> AsyncIterator[Sequence[BaseModel]]:
    yield list(instances)


def _refusal_verdict(error: CoreException) -> GateVerdict:
    """Classify a refusal: only the capability gate is a verdict, everything else propagates.

    The one rule this scenario cannot get wrong. A broken route, a wiring error or a
    validation failure reported as ``REFUSED`` would make a defect look like a correctly
    enforced capability — and two backends would then "agree" for the worst possible reason.
    """

    if error.code == UNSUPPORTED_INFERENCE_FEATURE_CODE:
        return GateVerdict.REFUSED

    raise error


async def _verdict_of(awaitable: Any) -> GateVerdict:
    """Reduce one gated call to a verdict, letting anything that is not a gate through."""

    try:
        await awaitable
    except CoreException as error:
        return _refusal_verdict(error)

    return GateVerdict.ACCEPTED


async def _stream_verdict(
    port: InferencePort[Any, Any],
    instances: Sequence[BaseModel],
) -> GateVerdict:
    """As :func:`_verdict_of`, but the stream gate fires on first iteration, not on call.

    ``predict_stream`` is an async generator, so ``validate_stream_supported`` runs only
    once the caller pulls — a verdict taken at call time would read every backend as
    ACCEPTED, including the ones that cannot stream at all.
    """

    try:
        async for _chunk in port.predict_stream(_one_chunk(instances)):
            pass
    except CoreException as error:
        return _refusal_verdict(error)

    return GateVerdict.ACCEPTED


# ....................... #


async def run_capability_gates(
    port: InferencePort[Any, Any],
    *,
    within_cap: Sequence[BaseModel],
    oversized: Sequence[BaseModel],
) -> CapabilityGateOutcome:
    """Drive the four gated requests against *port* and return the verdicts.

    *within_cap* must be a batch the backend's declared cap admits and *oversized* one it
    does not; the caller owns that sizing because the cap is a wiring fact, not something
    this scenario can discover without tripping the very gate it is measuring.
    """

    return CapabilityGateOutcome(
        within_cap_predict_many=await _verdict_of(port.predict_many(within_cap)),
        oversized_predict_many=await _verdict_of(port.predict_many(oversized)),
        stream=await _stream_verdict(port, within_cap),
        oversized_stream_chunk=await _stream_verdict(port, oversized),
    )
