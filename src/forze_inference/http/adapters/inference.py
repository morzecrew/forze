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
from forze.base.exceptions import exc
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
    def _instances_per_request(self) -> int | None:
        """What the dialect carries per request, or ``None`` for a whole batch.

        Read with a fallback rather than off the attribute, because ``WireProtocol`` is a
        public extension point and a dialect written against its earlier shape declares
        neither member. ``None`` *is* that shape's semantics — its ``encode_request`` took
        the whole batch — so an existing custom dialect keeps serving instead of raising
        ``AttributeError`` before a request is even made.
        """

        return getattr(self.protocol, "instances_per_request", None)

    # ....................... #

    @property
    def inference_capabilities(self) -> InferenceCapabilities:
        return attrs.evolve(
            DEFAULT_INFERENCE_CAPABILITIES,
            # The dialect decides: one request per batch is vectorized scoring, one
            # request per instance is not. Declaring it unconditionally would leave the
            # capability claiming a batch call the wire never makes, and the in-memory
            # oracle mirrors this declaration to refuse where a deployment would.
            native_batch=self._instances_per_request is None,
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

        # Same fallback as the batch declaration: a dialect from before this member
        # reports no usage rather than failing the call.
        report = getattr(self.protocol, "usage_attributes", None)

        for attribute, value in (report(response) if report is not None else {}).items():
            usage[attribute] = usage.get(attribute, 0) + value

        return self.protocol.decode_response(
            self.spec,
            response,
            expected=len(group),
        )

    # ....................... #

    @staticmethod
    def _record_usage(usage: Mapping[str, int]) -> None:
        """Put what one port call spent on whichever span is current.

        The port's CLIENT span where per-port spans are on, the enclosing operation span
        otherwise, and dropped by a non-recording span. Called once per port call rather
        than once per wire call, because a span attribute is overwritten rather than
        accumulated: a stream of three chunks would otherwise report its last chunk.
        """

        if not usage:
            return

        span = trace.get_current_span()

        for attribute, total in usage.items():
            span.set_attribute(attribute, total)

    # ....................... #

    async def _score(self, prepared: Sequence[In], usage: dict[str, int]) -> Sequence[Out]:
        """Score one already-validated, already-capped batch over one or more wire calls.

        *usage* accumulates across the whole port call; the caller records it.
        """

        # Before the model name, not after: resolving it can call an application's tenant
        # resolver, and this code's refusal promises that nothing ran and nothing was
        # billed. The per-request check inside the loop is the one that stops a fan-out
        # part way; this one is what keeps that promise for the first request.
        ensure_budget(backend=self.config.protocol)

        model_name = await self._model_name()
        per_request = self._instances_per_request

        # `itertools.batched` raises a bare ValueError below 1, and a dialect declaring 0
        # can serve nothing at all — refused by name, the way the per-call batch hint is.
        if per_request is not None and per_request < 1:
            raise exc.configuration(
                f"Wire dialect {self.config.protocol!r} declares "
                f"instances_per_request={per_request}; it must be at least 1, or None for "
                "a dialect that carries a whole batch."
            )

        groups: Sequence[Sequence[In]] = (
            [prepared]
            if per_request is None
            else [list(group) for group in batched(prepared, per_request, strict=False)]
        )

        records: list[Mapping[str, Any]] = []

        for group in groups:
            records.extend(await self._wire_call(group, model_name=model_name, usage=usage))

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

        usage: dict[str, int] = {}

        # In a `finally` because a fan-out that fails half way has still spent what its
        # earlier requests consumed, and losing the count exactly when something went wrong
        # is losing it when it matters. Nothing here can outrank the outcome:
        # `set_attribute` does not raise for the ints this collects.
        try:
            with bind_run_options(options):
                return await self._score(prepared, usage)

        finally:
            self._record_usage(usage)

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

        # One accumulator for the whole stream: the attribute is overwritten rather than
        # summed, so recording per chunk would report only the last one. The `finally`
        # covers an abandoned generator too, which is closed rather than exhausted.
        usage: dict[str, int] = {}

        try:
            async for chunk in instances:
                yield await self._score_chunk(chunk, options=options, usage=usage, cap=wire_cap)

        finally:
            self._record_usage(usage)

    # ....................... #

    async def _score_chunk(
        self,
        chunk: Sequence[In],
        *,
        options: InferenceRunOptions | None,
        usage: dict[str, int],
        cap: int | None,
    ) -> list[Out]:
        """One yielded chunk of a stream: validate, sub-batch to *cap*, score."""

        prepared = validated_instances(self.spec, chunk)

        if not prepared:
            return []

        # The per-call deadline covers the wire calls only. Yielding inside the bound
        # context would charge the consumer's own processing time to the model's budget,
        # and would reset the deadline token from whatever context finalizes the generator
        # if the consumer abandons it mid-stream — which is why the scoring is a method
        # the generator awaits rather than a block it yields inside.
        scored: list[Out] = []

        with bind_run_options(options):
            if cap is None:
                return list(await self._score(prepared, usage))

            for sub_batch in batched(prepared, cap, strict=False):
                scored.extend(await self._score(list(sub_batch), usage))

        return scored
