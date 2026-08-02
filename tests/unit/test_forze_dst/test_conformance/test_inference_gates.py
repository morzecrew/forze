"""Controls for the inference capability-gate scenario.

The scenario reduces four requests to ACCEPTED/REFUSED so a mock and a real backend can be
compared on admission rather than on predictions. That reduction is only trustworthy if it
can distinguish the cases it claims to, so these tests drive it against oracles told
deliberately different surfaces and pin what comes back.

Two of them cover things the integration legs cannot:

- **The stream gate.** Every shipped backend declares ``supports_stream=True``, so the
  ``stream`` verdict is ACCEPTED on every leg and that field would be vacuous everywhere.
  Only an oracle told a stream-less surface can show the gate working at all.
- **Non-gate failures.** A scenario that folded every exception into REFUSED would report a
  broken route as a working gate — the one wrong answer that makes the whole comparison
  worthless, and the one no green leg would ever reveal.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts import inference as inference_contract
from forze.application.contracts.inference import (
    DEFAULT_INFERENCE_CAPABILITIES,
    FULL_INFERENCE_CAPABILITIES,
    InferenceCapabilities,
    InferenceSpec,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_dst.conformance.inference import GateVerdict, run_capability_gates
from forze_mock import MockDepsModule, MockInferenceRegistry

# ----------------------- #

ROUTE = "gates"
CAP = 2


class Features(BaseModel):
    x: float = 0.0


class Scores(BaseModel):
    y: float = 0.0


def _double(instances: Sequence[BaseModel]) -> Sequence[Any]:
    return [{"y": i.x * 2.0} for i in instances if isinstance(i, Features)]


def _route(
    capabilities: InferenceCapabilities | None,
    *,
    programmed: bool = True,
) -> Any:
    registry = MockInferenceRegistry()

    if programmed:
        registry.on(ROUTE, _double, capabilities=capabilities)

    spec = InferenceSpec(name=ROUTE, input=Features, output=Scores)

    return context_from_modules(MockDepsModule(inference=registry)).inference.model(spec)


def _batches() -> dict[str, Sequence[BaseModel]]:
    instances = [Features(x=float(n)) for n in range(CAP + 1)]

    return {"within_cap": instances[:CAP], "oversized": instances}


# ....................... #


async def test_the_full_surface_admits_everything() -> None:
    """The default an unregistered route falls back to — nothing is gated."""

    outcome = await run_capability_gates(_route(FULL_INFERENCE_CAPABILITIES), **_batches())

    assert outcome.within_cap_predict_many is GateVerdict.ACCEPTED
    assert outcome.oversized_predict_many is GateVerdict.ACCEPTED
    assert outcome.stream is GateVerdict.ACCEPTED
    assert outcome.oversized_stream_chunk is GateVerdict.ACCEPTED


async def test_an_unregistered_route_gets_the_full_surface() -> None:
    """The residual this scenario exists for, pinned as the fact it is.

    Registering a route without ``capabilities=`` is not an error and does not warn; the
    oracle simply advertises more than any backend it might stand in for. That is why
    forgetting it has to be caught by a differential rather than by a wiring check.
    """

    assert _route(None).inference_capabilities == FULL_INFERENCE_CAPABILITIES


async def test_a_declared_cap_refuses_a_batch_but_serves_a_stream_chunk() -> None:
    """The asymmetry both halves of ``max_batch_size`` promise, in one outcome.

    ``predict_many`` is all-or-nothing so an oversized batch is refused whole; a stream
    chunk is a bounded-memory convenience, so the same cap sub-batches it instead. An
    oracle that refused in both places would be stricter than the contract, and correct
    streaming code would fail against the oracle alone.
    """

    capped = InferenceCapabilities(native_batch=True, supports_stream=True, max_batch_size=CAP)

    outcome = await run_capability_gates(_route(capped), **_batches())

    assert outcome.within_cap_predict_many is GateVerdict.ACCEPTED
    assert outcome.oversized_predict_many is GateVerdict.REFUSED
    assert outcome.oversized_stream_chunk is GateVerdict.ACCEPTED


async def test_a_stream_less_surface_refuses_the_stream() -> None:
    """The gate no shipped backend exercises — all three declare ``supports_stream=True``."""

    outcome = await run_capability_gates(_route(DEFAULT_INFERENCE_CAPABILITIES), **_batches())

    assert outcome.stream is GateVerdict.REFUSED
    assert outcome.oversized_stream_chunk is GateVerdict.REFUSED
    assert outcome.within_cap_predict_many is GateVerdict.ACCEPTED, (
        "the stream refusal must not be a route that refuses everything"
    )


async def test_a_non_gate_failure_propagates_instead_of_reading_as_refused() -> None:
    """A broken route must not be reported as a working gate.

    An unprogrammed mock route fails closed with ``mock.inference.unprogrammed`` — the
    right behaviour, and exactly the shape that would be silently swallowed by a scenario
    catching exceptions broadly. It has to come out of the scenario, not out of it as a
    verdict.
    """

    with pytest.raises(CoreException) as refused:
        await run_capability_gates(_route(None, programmed=False), **_batches())

    assert refused.value.code == "mock.inference.unprogrammed"


async def test_the_async_job_capability_still_has_no_gate() -> None:
    """A tripwire on the one declared capability nothing enforces.

    ``supports_async_jobs`` is advertised (the oracle claims it) but the port ships no
    job-submit verb and the contract ships no validator for it, so unlike the cap and the
    stream it cannot be part of the scenario above. That is a fair state for a capability
    declared ahead of its plane — but it must not survive the plane landing. When the verb
    arrives this fails, and whoever adds it has to add its gate and its differential in the
    same change, rather than discovering later that the oracle admitted every job a real
    backend would have refused.
    """

    assert FULL_INFERENCE_CAPABILITIES.supports_async_jobs, "the oracle claims the capability"
    assert not hasattr(inference_contract, "validate_async_jobs_supported"), (
        "an async-job gate now exists — extend run_capability_gates to exercise it, and "
        "add the case to CapabilityGateOutcome so the mock is checked against it"
    )
