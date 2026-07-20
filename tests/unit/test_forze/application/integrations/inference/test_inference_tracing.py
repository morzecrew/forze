"""The inference seam is DST-visible: every port call emits a runtime trace event.

Inference emits **no** ``record(...)`` calls of its own — it cannot: the local adapter
lives in the integrations layer, which import-linter forbids from importing
``forze.application.execution``. Visibility instead comes from the port-instrumentation
proxy, which derives the event's ``domain``/``phase`` from the **dep key name**
(``inference_query`` -> ``domain="inference"``, ``phase="query"``) and its ``route`` from
the spec name.

That makes the key name load-bearing rather than cosmetic: renaming it (or resolving a
port outside the deps container) silently removes the seam from every DST oracle and
value trace with nothing else failing. These tests pin the emitted shape so that
regression cannot pass unnoticed.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceDepKey, InferenceSpec
from forze.application.execution import DepsRegistry, ExecutionContext, FrozenDeps
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
)

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


class _DoublingModel:
    def predict_batch(self, instances):
        return [_Score(y=i.x * 2.0) for i in instances]


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


def _traced_deps(*, capture_values: bool = False) -> FrozenDeps:
    module = LocalInferenceDepsModule(
        models={"doubler": LocalInferenceConfig(loader=_DoublingModel)},
    )

    return (
        DepsRegistry.from_modules(module)
        .with_tracing(runtime=True, capture_values=capture_values)
        .freeze()
        .resolve()
    )


# ....................... #


class TestInferenceRuntimeTracing:
    @pytest.mark.asyncio
    async def test_predict_emits_a_typed_inference_event(self) -> None:
        deps = _traced_deps()
        port = ExecutionContext(deps=deps).inference.model(_spec())

        await port.predict(_Features(x=2.0))

        (event,) = deps.runtime_trace().events
        assert event.domain == "inference"
        assert event.op == "predict"
        assert event.surface == "inference_query"
        assert event.route == "doubler"
        assert event.phase == "query"

    @pytest.mark.asyncio
    async def test_every_port_method_is_visible(self) -> None:
        deps = _traced_deps()
        port = ExecutionContext(deps=deps).inference.model(_spec())

        async def chunks():
            yield [_Features(x=3.0)]

        await port.predict(_Features(x=1.0))
        await port.predict_many([_Features(x=2.0)])
        async for _ in port.predict_stream(chunks()):
            pass

        ops = [event.op for event in deps.runtime_trace().events]
        assert ops == ["predict", "predict_many", "predict_stream"]

    @pytest.mark.asyncio
    async def test_route_tracks_the_spec_name(self) -> None:
        """A second route must be distinguishable in the trace, not merged."""

        module = LocalInferenceDepsModule(
            models={
                "doubler": LocalInferenceConfig(loader=_DoublingModel),
                "other": LocalInferenceConfig(loader=_DoublingModel),
            },
        )
        deps = DepsRegistry.from_modules(module).with_tracing(runtime=True).freeze().resolve()
        ctx = ExecutionContext(deps=deps)

        await ctx.inference.model(_spec()).predict(_Features(x=1.0))
        await ctx.inference.model(
            InferenceSpec(name="other", input=_Features, output=_Score)
        ).predict(_Features(x=1.0))

        assert [event.route for event in deps.runtime_trace().events] == ["doubler", "other"]

    @pytest.mark.asyncio
    async def test_inputs_are_redacted_in_capture_by_default(self) -> None:
        """Features are PII-dense and cannot be field-encrypted, so a captured trace must
        not carry them verbatim unless the author opted in."""

        deps = _traced_deps(capture_values=True)
        port = ExecutionContext(deps=deps).inference.model(_spec())

        await port.predict(_Features(x=1234.5))

        payloads = [e.payload for e in deps.runtime_trace().events if e.payload is not None]
        assert payloads == [{"x": "<redacted>"}]

    @pytest.mark.asyncio
    async def test_capture_inputs_opts_into_verbatim_values(self) -> None:
        deps = _traced_deps(capture_values=True)
        spec = InferenceSpec(
            name="doubler",
            input=_Features,
            output=_Score,
            capture_inputs=True,
        )
        port = ExecutionContext(deps=deps).inference.model(spec)

        await port.predict(_Features(x=1234.5))

        payloads = [e.payload for e in deps.runtime_trace().events if e.payload is not None]
        assert payloads == [{"x": 1234.5}]

    def test_redaction_covers_every_input_field(self) -> None:
        """The mask is derived from the input model, not a hand-listed subset — adding a
        field to the model must not silently start leaking it."""

        spec = _spec()
        assert spec.sensitive_capture_fields == frozenset({"x"})
        assert (
            InferenceSpec(
                name="doubler", input=_Features, output=_Score, capture_inputs=True
            ).sensitive_capture_fields
            == frozenset()
        )

    def test_dep_key_name_is_the_load_bearing_signal(self) -> None:
        """``domain``/``phase`` above are derived from this name — renaming it un-traces
        the seam, so the name is pinned here next to the events it produces."""

        assert InferenceDepKey.name == "inference_query"
