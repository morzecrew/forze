"""SageMaker realtime ``InferencePort`` (JSON-record scope)."""

import json
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from itertools import batched
from typing import TYPE_CHECKING, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import (
    DEFAULT_INFERENCE_CAPABILITIES,
    InferenceCapabilities,
    InferencePort,
    InferenceRunOptions,
    InferenceSpec,
    validate_batch_size,
)
from forze.application.contracts.resolution import resolve_scoped_namespace
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.inference import (
    bind_run_options,
    shape_outputs,
    validated_instances,
)
from forze.base.primitives import OnceCell, remaining_time
from forze_inference.records import decode_predictions_body

from ..kernel import SageMakerRuntimeClientPort

if TYPE_CHECKING:
    from ..execution.deps.configs import SageMakerInferenceConfig

# ----------------------- #

SAGEMAKER_BACKEND = "sagemaker"
"""Backend label used in capability refusals and boundary errors."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SageMakerInferenceAdapter[In: BaseModel, Out: BaseModel](
    TenancyMixin,
    InferencePort[In, Out],
):
    """One SageMaker realtime route: encode a batch, invoke, decode typed predictions."""

    spec: InferenceSpec[In, Out]
    client: SageMakerRuntimeClientPort
    config: "SageMakerInferenceConfig"

    _endpoint_name_cell: OnceCell[str] = attrs.field(
        factory=OnceCell[str],
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    @property
    def inference_capabilities(self) -> InferenceCapabilities:
        return attrs.evolve(
            DEFAULT_INFERENCE_CAPABILITIES,
            native_batch=True,
            supports_stream=True,
            max_batch_size=self.config.max_batch_size,
            deterministic=self.config.deterministic,
        )

    # ....................... #

    async def _endpoint_name(self) -> str:
        self.require_tenant_if_aware()

        return await resolve_scoped_namespace(
            self.config.endpoint_name,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._endpoint_name_cell,
        )

    # ....................... #

    async def _score(self, prepared: Sequence[In]) -> Sequence[Out]:
        """One endpoint invocation for one already-validated, already-capped batch."""

        endpoint = await self._endpoint_name()

        # Explicit JSON mode: the default codec keeps UUID/datetime/Decimal live in
        # python mode, which is not wire-safe.
        body = json.dumps(
            {"instances": [instance.model_dump(mode="json") for instance in prepared]}
        ).encode()

        response = await self.client.invoke_endpoint(
            endpoint,
            body=body,
            content_type=self.config.content_type,
            accept=self.config.accept,
            target_variant=self.config.target_variant,
            timeout=remaining_time(),
        )

        records = decode_predictions_body(self.spec, response, backend=SAGEMAKER_BACKEND)

        return shape_outputs(
            self.spec,
            records,
            expected=len(prepared),
            backend=SAGEMAKER_BACKEND,
        )

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

        # All-or-nothing: an oversized batch is refused whole, never silently split.
        validate_batch_size(
            self.inference_capabilities,
            len(prepared),
            backend=SAGEMAKER_BACKEND,
        )

        with bind_run_options(options):
            return await self._score(prepared)

    # ....................... #

    async def predict_stream(
        self,
        instances: AsyncIterator[Sequence[In]],
        *,
        options: InferenceRunOptions | None = None,
    ) -> AsyncGenerator[Sequence[Out]]:
        # Streaming sub-batches its wire calls to the effective cap (the tighter of the
        # per-call option and the endpoint's hard cap) while preserving the caller's
        # chunk boundaries: one yielded chunk per input chunk.
        caps = [
            cap
            for cap in (
                (options or {}).get("max_batch_size"),
                self.config.max_batch_size,
            )
            if cap is not None
        ]
        wire_cap = min(caps) if caps else None

        async for chunk in instances:
            prepared = validated_instances(self.spec, chunk)

            if not prepared:
                yield []
                continue

            # The per-call deadline covers the wire calls only. Yielding inside the bound
            # context would charge the consumer's own processing time to the model's
            # budget, and would reset the deadline token from whatever context finalizes
            # the generator if the consumer abandons it mid-stream.
            scored: list[Out]

            with bind_run_options(options):
                if wire_cap is None:
                    scored = list(await self._score(prepared))
                else:
                    scored = []

                    for sub_batch in batched(prepared, wire_cap, strict=False):
                        scored.extend(await self._score(list(sub_batch)))

            yield scored
