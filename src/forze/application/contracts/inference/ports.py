"""Inference port definition (read-plane).

An :class:`~forze.application.contracts.inference.specs.InferenceSpec` names one logical
inference task with typed input/output models; the backend adapter maps it to a physical
model — an in-process artifact, a served endpoint, or a cloud endpoint — declared in the
wiring config. Handlers pass typed instances only: no model URIs, artifact formats, or wire
protocols cross this port.

The port is **read-plane**: invoking a model is a pure read of it (no state mutates in the
application's domain), so it is resolvable inside a read-only (``QUERY``) operation — the
same stance the analytics query port takes for warehouse reads that also cost money and
leave the process. Offline batch job submission (which launches paid external work) is a
separate command-plane port and ships with the batch plane.
"""

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Sequence
from typing import (
    Any,
    Protocol,
    runtime_checkable,
)

from pydantic import BaseModel

from .capabilities import DEFAULT_INFERENCE_CAPABILITIES, InferenceCapabilities
from .specs import InferenceSpec
from .types import InferenceRunOptions

# ----------------------- #


@runtime_checkable
class BaseInferencePort(Protocol):
    """Shared ``spec`` binding for inference adapters."""

    spec: InferenceSpec[Any, Any]
    """``InferenceSpec`` for this port instance."""


# ....................... #


class InferencePort[In: BaseModel, Out: BaseModel](BaseInferencePort, Protocol):
    """One logical inference task, bound to one spec."""

    def predict(
        self,
        instance: In,
        *,
        options: InferenceRunOptions | None = None,
    ) -> Awaitable[Out]:
        """Score a single instance and return its typed prediction.

        :param instance: Input instance (an ``InferenceSpec.input`` model).
        :param options: Optional per-call knobs (timeout, chunking hints).
        :returns: The prediction as the spec's output model.
        """
        ...  # pragma: no cover

    # ....................... #

    def predict_many(
        self,
        instances: Sequence[In],
        *,
        options: InferenceRunOptions | None = None,
    ) -> Awaitable[Sequence[Out]]:
        """Score a batch of instances in one call — order-preserving, **all-or-nothing**.

        One prediction per input, in input order. The call either returns a prediction for
        every instance or raises for the whole batch: an invalid instance or a backend
        failure fails the call, never a silent partial result. A backend with a hard batch
        cap refuses an oversized batch up front (see
        :func:`~forze.application.contracts.inference.capabilities.validate_batch_size`)
        rather than splitting a call the caller asked to be atomic.

        :param instances: Input instances (``InferenceSpec.input`` models).
        :param options: Optional per-call knobs (timeout).
        :returns: Predictions, one per input, in input order.
        """
        ...  # pragma: no cover

    # ....................... #

    def predict_stream(
        self,
        instances: AsyncIterator[Sequence[In]],
        *,
        options: InferenceRunOptions | None = None,
    ) -> AsyncGenerator[Sequence[Out]]:
        """Score a chunked stream of instances with bounded memory.

        Consumes input chunks lazily and yields one prediction chunk per input chunk, in
        order — the inference analog of the document/analytics ``*_chunked`` convention.
        Chunk boundaries are preserved (chunk *N* of predictions answers chunk *N* of
        inputs). Streams *instances*, never tokens: partial-response delta streaming (a
        generative model emitting one prediction incrementally) is a different signature
        and deliberately not this method.

        Backends that cannot serve it refuse up front via
        :func:`~forze.application.contracts.inference.capabilities.validate_stream_supported`.

        :param instances: Chunked input instances, consumed lazily.
        :param options: Optional per-call knobs (timeout, per-chunk sub-batching cap).
        :returns: Prediction chunks, one per input chunk, in order.
        """
        ...  # pragma: no cover

    # ....................... #

    @property
    def inference_capabilities(self) -> InferenceCapabilities:
        """What this adapter can serve (see :class:`InferenceCapabilities`).

        The default describes the narrowest single-instance backend; adapters that serve
        more override the property.
        """
        ...  # pragma: no cover
        return DEFAULT_INFERENCE_CAPABILITIES
