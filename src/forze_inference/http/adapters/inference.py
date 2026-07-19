"""Served-model ``InferencePort`` over an HTTP wire protocol."""

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

from ..kernel import InferenceHttpClientPort
from ..protocols import WireProtocol

if TYPE_CHECKING:
    from ..execution.deps.configs import HttpInferenceConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpInferenceAdapter[In: BaseModel, Out: BaseModel](
    TenancyMixin,
    InferencePort[In, Out],
):
    """One served-model route: encode a batch, POST it, decode typed predictions."""

    spec: InferenceSpec[In, Out]
    client: InferenceHttpClientPort
    config: "HttpInferenceConfig"
    protocol: WireProtocol

    _model_name_cell: OnceCell[str] = attrs.field(
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

    async def _model_name(self) -> str:
        self.require_tenant_if_aware()

        return await resolve_scoped_namespace(
            self.config.model_name,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._model_name_cell,
        )

    # ....................... #

    async def _score(self, prepared: Sequence[In]) -> Sequence[Out]:
        """One wire call for one already-validated, already-capped batch."""

        model_name = await self._model_name()
        path, body = self.protocol.encode_request(
            self.spec,
            prepared,
            model_name=model_name,
        )

        response = await self.client.post_json(
            path,
            body,
            timeout=remaining_time(),
        )

        records = self.protocol.decode_response(
            self.spec,
            response,
            expected=len(prepared),
        )

        return shape_outputs(
            self.spec,
            records,
            expected=len(prepared),
            backend=self.config.protocol,
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
            backend=self.config.protocol,
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

            with bind_run_options(options):
                if wire_cap is None:
                    yield await self._score(prepared)
                    continue

                scored: list[Out] = []

                for sub_batch in batched(prepared, wire_cap, strict=False):
                    scored.extend(await self._score(list(sub_batch)))

                yield scored
