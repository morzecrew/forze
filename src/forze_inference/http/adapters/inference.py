"""Served-model ``InferencePort`` over an HTTP wire protocol."""

from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
from itertools import batched
from typing import TYPE_CHECKING, Any, final

import attrs
from opentelemetry import trace
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
    ensure_budget,
    resolve_wire_cap,
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
            # The dialect decides: one request per batch is vectorized scoring, one
            # request per instance is not. Declaring it unconditionally would leave the
            # capability claiming a batch call the wire never makes, and the in-memory
            # oracle mirrors this declaration to refuse where a deployment would.
            native_batch=self.protocol.instances_per_request is None,
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

    async def _wire_call(
        self,
        group: Sequence[In],
        *,
        model_name: str,
        usage: dict[str, int],
    ) -> Sequence[Mapping[str, Any]]:
        """One request/response for one group of instances the dialect can carry."""

        # Inside the loop, not before it: a fan-out spends the budget as it goes, and a
        # request the deadline can no longer cover must not be sent.
        ensure_budget(backend=self.config.protocol)

        path, body = self.protocol.encode_request(
            self.spec,
            group,
            model_name=model_name,
        )

        response = await self.client.post_json(
            path,
            body,
            timeout=remaining_time(),
        )

        for attribute, value in self.protocol.usage_attributes(response).items():
            usage[attribute] = usage.get(attribute, 0) + value

        return self.protocol.decode_response(
            self.spec,
            response,
            expected=len(group),
        )

    # ....................... #

    async def _score(self, prepared: Sequence[In]) -> Sequence[Out]:
        """Score one already-validated, already-capped batch over one or more wire calls."""

        model_name = await self._model_name()
        per_request = self.protocol.instances_per_request
        groups: Sequence[Sequence[In]] = (
            [prepared]
            if per_request is None
            else [list(group) for group in batched(prepared, per_request, strict=False)]
        )

        records: list[Mapping[str, Any]] = []
        usage: dict[str, int] = {}

        for group in groups:
            records.extend(await self._wire_call(group, model_name=model_name, usage=usage))

        # Summed over the fan-out and set once, so the attribute reports what the port
        # call spent rather than what its last request did. It lands on whichever span is
        # current — the port's CLIENT span where per-port spans are on, the operation span
        # otherwise — and a non-recording span drops it.
        if usage:
            span = trace.get_current_span()

            for attribute, total in usage.items():
                span.set_attribute(attribute, total)

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
        wire_cap = resolve_wire_cap(
            options,
            self.inference_capabilities,
            backend=self.config.protocol,
        )

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
