"""In-memory :class:`~forze.application.contracts.inference.InferencePort` for tests / simulation.

The mock cannot run a real model, so each inference route is answered by a **pure sync
function** registered on a :class:`MockInferenceRegistry`. The function receives the
validated input instances and returns the predictions (spec-output instances, or mappings
decoded through the output codec). Purity is the contract: a deterministic function of its
inputs keeps simulation replays exact — which is also why the mock defaults to the full
capability surface including ``deterministic``. A route standing in for a *specific*
backend should register that backend's declared capabilities
(``registry.on(..., capabilities=...)``): the mock then enforces them (batch cap, stream
refusal), so a capability gate fails against the oracle exactly where production would
instead of only in production.

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
    validate_batch_size,
    validate_stream_supported,
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
    _capabilities: dict[str, InferenceCapabilities] = attrs.field(
        factory=dict[str, InferenceCapabilities],
    )

    def on(
        self,
        route: StrKey | str,
        predict: MockInferencePredict,
        *,
        capabilities: InferenceCapabilities | None = None,
    ) -> MockInferenceRegistry:
        """Register *predict* for inference *route*. Returns self (chainable).

        Pass *capabilities* to mirror the declared surface of the real backend this
        route stands in for (e.g. the HTTP adapter's partial set). The mock otherwise
        advertises the full surface — every capability gate then passes against the
        oracle and can only fail in production, exactly the divergence the mock exists
        to catch. With the real adapter's declaration pinned here, a gated request
        fails under the mock precisely where the deployed backend would refuse it.
        """

        self._predictors[str(route)] = predict

        if capabilities is not None:
            self._capabilities[str(route)] = capabilities

        else:
            # Re-registering without capabilities restores the documented full-surface
            # default — a stale earlier declaration must not silently linger.
            self._capabilities.pop(str(route), None)

        return self

    def predictor_for(self, route: str) -> MockInferencePredict | None:
        return self._predictors.get(route)

    def capabilities_for(self, route: str) -> InferenceCapabilities | None:
        return self._capabilities.get(route)


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
        """The route's registered capabilities, or the full surface when none are.

        The full-surface default is deliberate — the mock genuinely serves every
        feature, so generic tests can exercise them all — but it silently out-capables
        any real adapter it stands in for: a capability gate that passes here can
        still refuse in production. A route mirroring a specific backend should
        register that backend's declared capabilities
        (``MockInferenceRegistry.on(..., capabilities=...)``) so the gates fail on
        the oracle exactly where the deployment would.
        """

        declared = self.registry.capabilities_for(str(self.spec.name))

        return declared if declared is not None else FULL_INFERENCE_CAPABILITIES

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

        # Enforce the route's *declared* capabilities the way the real adapters
        # enforce theirs — under registered partial capabilities the mock refuses
        # exactly what the mirrored backend would (a no-op under the full default).
        validate_batch_size(
            self.inference_capabilities,
            len(prepared),
            backend=MOCK_INFERENCE_BACKEND,
        )

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
        # Refuse up front when the route's registered capabilities mirror a backend
        # without chunked streaming (a no-op under the full default).
        validate_stream_supported(self.inference_capabilities, backend=MOCK_INFERENCE_BACKEND)

        async for chunk in instances:
            yield await self.predict_many(chunk, options=options)
