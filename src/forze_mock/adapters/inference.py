"""In-memory :class:`~forze.application.contracts.inference.InferencePort` for tests / simulation.

The mock cannot run a real model, so each inference route is answered by a **pure sync
function** registered on a :class:`MockInferenceRegistry`. The function receives the
validated input instances and returns the predictions (spec-output instances, or mappings
decoded through the output codec). Purity is the contract: a deterministic function of its
inputs keeps simulation replays exact — which is also why the mock advertises the full
capability surface including ``deterministic``.

Outputs pass through the same boundary shaping as every real adapter
(:func:`~forze.application.integrations.inference.adapter_common.shape_outputs`), so a
mis-shaped stub fails under the mock exactly where a mis-shaped backend would fail in
production — the differential-conformance property.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable, Sequence
from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import (
    FULL_INFERENCE_CAPABILITIES,
    InferenceCapabilities,
    InferencePort,
    InferenceRunOptions,
    InferenceSpec,
)
from forze.application.integrations.inference.adapter_common import (
    bind_run_options,
    shape_outputs,
    validated_instances,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #

MOCK_INFERENCE_BACKEND = "mock"
"""Backend label used in boundary errors."""

MockInferencePredict = Callable[[Sequence[BaseModel]], Sequence[Any]]
"""Pure scoring function for one route: receives the validated input instances and returns
one prediction per instance, in order — spec-output instances or mappings. Must be a
deterministic function of its inputs (no ambient time, randomness, or I/O), so simulation
replays stay exact."""


@final
@attrs.define(slots=True)
class MockInferenceRegistry:
    """Programmable in-memory scoring functions, keyed by route (spec) name."""

    _predictors: dict[str, MockInferencePredict] = attrs.field(
        factory=dict[str, MockInferencePredict],
    )

    def on(
        self,
        route: StrKey | str,
        predict: MockInferencePredict,
    ) -> MockInferenceRegistry:
        """Register *predict* for inference *route*. Returns self (chainable)."""

        self._predictors[str(route)] = predict
        return self

    def predictor_for(self, route: str) -> MockInferencePredict | None:
        return self._predictors.get(route)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockInferenceAdapter[In: BaseModel, Out: BaseModel](InferencePort[In, Out]):
    """In-memory ``InferencePort`` bound to one spec + a scoring-function registry."""

    spec: InferenceSpec[In, Out]
    registry: MockInferenceRegistry

    # ....................... #

    @property
    def inference_capabilities(self) -> InferenceCapabilities:
        return FULL_INFERENCE_CAPABILITIES

    # ....................... #

    async def predict(
        self,
        instance: In,
        *,
        options: InferenceRunOptions | None = None,
    ) -> Out:
        return (await self.predict_many((instance,), options=options))[0]

    # ....................... #

    async def predict_many(
        self,
        instances: Sequence[In],
        *,
        options: InferenceRunOptions | None = None,
    ) -> Sequence[Out]:
        prepared = validated_instances(self.spec, instances)

        if not prepared:
            return []

        predict = self.registry.predictor_for(str(self.spec.name))

        if predict is None:
            raise exc.configuration(
                f"MockInference {self.spec.name!r}: no scoring function registered — "
                "register one via MockInferenceRegistry.on()",
                code="mock.inference.unprogrammed",
            )

        with bind_run_options(options):
            raw = predict(prepared)

        return shape_outputs(
            self.spec,
            raw,
            expected=len(prepared),
            backend=MOCK_INFERENCE_BACKEND,
        )

    # ....................... #

    async def predict_stream(
        self,
        instances: AsyncIterator[Sequence[In]],
        *,
        options: InferenceRunOptions | None = None,
    ) -> AsyncGenerator[Sequence[Out]]:
        async for chunk in instances:
            yield await self.predict_many(chunk, options=options)
