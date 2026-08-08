"""Controls for the inference capability-gate scenario.

The scenario reduces four requests to ACCEPTED/REFUSED so a mock and a real backend can be
compared on admission rather than on predictions. That reduction is only trustworthy if it
can distinguish the cases it claims to, so these tests drive it against oracles told
deliberately different surfaces and pin what comes back.

Three of them cover things the integration legs cannot:

- **The stream gate.** Every shipped backend declares ``supports_stream=True``, so the
  ``stream`` verdict is ACCEPTED on every leg and that field would be vacuous everywhere.
  Only an oracle told a stream-less surface can show the gate working at all — and a
  companion test pins that premise, so the claim cannot quietly stop being true.
- **Non-gate failures.** A scenario that folded every exception into REFUSED would report a
  broken route as a working gate — the one wrong answer that makes the whole comparison
  worthless, and the one no green leg would ever reveal.
- **The plane that has not landed.** ``supports_async_jobs`` is declared with no ports, no
  gate and no adapter setting it; the tripwire below keeps the oracle from claiming it, and
  fails when the mock starts serving offline batch for real.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import (
    DEFAULT_INFERENCE_CAPABILITIES,
    FULL_INFERENCE_CAPABILITIES,
    InferenceCapabilities,
    InferenceSpec,
)
from forze.application.integrations.inference.local import (
    LocalInferenceAdapter,
    LocalInferenceConfig,
    LocalModelHost,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_dst.conformance.inference import GateVerdict, run_capability_gates
from forze_inference.http.adapters.inference import HttpInferenceAdapter
from forze_inference.http.execution.deps.configs import HttpInferenceConfig
from forze_inference.sagemaker.adapters.inference import SageMakerInferenceAdapter
from forze_inference.sagemaker.execution.deps.configs import SageMakerInferenceConfig
from forze_mock import MockDepsModule, MockInferenceRegistry

# ----------------------- #

ROUTE = "gates"
CAP = 2


class Features(BaseModel):
    x: float = 0.0


class Scores(BaseModel):
    y: float = 0.0


def _never_loaded() -> Any:
    """The local host's loader. Declaring capabilities never loads, so this never runs."""

    raise AssertionError("inference_capabilities must not load the model")


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

    ``supports_async_jobs`` names a plane that has not shipped: no ports, no gate, no
    adapter setting it, so unlike the cap and the stream there is nothing for the scenario
    above to exercise. Its design is locked in the offline-batch-inference RFC and
    deliberately **not** on ``InferencePort`` — decision #1 there is "batch = two new
    ports, never methods on ``InferencePort``" — so watching that port's surface, or
    guessing the gate's future name, would watch a door the plane will not come through.

    Two things catch the landing properly, and neither is a name:

    - the **conformance manifest**. That RFC names the dep keys
      (``inference_batch_command`` / ``inference_batch_query``), and
      ``.github/scripts/conformance_manifest.py`` requires every ``DepKey`` under
      ``contracts/`` to be claimed exactly once by a plane, a declared gap or an
      exemption. The keys cannot land without ``just quality`` failing until someone says
      how the plane is differentially tested.
    - the **assertion below**. That RFC has the mock implementing batch end-to-end
      (synchronous completion at submit, over ``MockStorage``), at which point it should
      declare the capability — and that is exactly when this fails, putting whoever lands
      the plane in this file to add its case to ``CapabilityGateOutcome``.

    Until then the oracle must not claim it. Nothing can serve a job, so a mock
    advertising one out-capables every real adapter on a dimension no test reaches — the
    shape the whole capability differential exists to catch.
    """

    assert not FULL_INFERENCE_CAPABILITIES.supports_async_jobs, (
        "the oracle advertises async jobs again — it has no verb to serve them with. If "
        "the offline batch plane has landed, extend run_capability_gates to drive its "
        "ports and add the case to CapabilityGateOutcome in that same change"
    )


def test_the_stream_gate_is_still_vacuous_on_every_shipped_leg() -> None:
    """Pins the premise behind this module's stream control, so the note cannot rot.

    ``run_capability_gates`` compares a ``stream`` verdict that is ACCEPTED on every real
    leg, because all three shipped adapters declare ``supports_stream=True`` unconditionally
    — unlike ``max_batch_size`` and ``deterministic``, which are config-driven and genuinely
    vary. The field earns its place only through the oracle control above.

    Asked of the adapters themselves rather than of their source: ``inference_capabilities``
    is a property over config, so a minimal instance answers it honestly and a client is
    never touched.

    If this fails, a shipped adapter can no longer stream: the leg's ``stream`` field has
    just become discriminating on its own, and the module docstring's claim that only an
    oracle exercises it is out of date.
    """

    spec = InferenceSpec(name="stream-probe", input=Features, output=Scores)
    http_config = HttpInferenceConfig(
        protocol="kserve_v2",
        model_name="m",
        acknowledge_data_egress=True,
    )
    shipped = {
        "local": LocalInferenceAdapter(
            spec=spec,
            host=LocalModelHost(config=LocalInferenceConfig(loader=_never_loaded)),
        ),
        "http": HttpInferenceAdapter(
            spec=spec,
            client=cast(Any, None),
            config=http_config,
            protocol=http_config.wire_protocol(),
        ),
        "sagemaker": SageMakerInferenceAdapter(
            spec=spec,
            client=cast(Any, None),
            config=SageMakerInferenceConfig(
                endpoint_name="e",
                acknowledge_data_egress=True,
            ),
        ),
    }

    for name, adapter in shipped.items():
        assert adapter.inference_capabilities.supports_stream, (
            f"{name} no longer declares supports_stream — see this test's docstring"
        )
