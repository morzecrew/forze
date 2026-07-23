"""In-process inference over a user-supplied model, executed off the event loop.

The hexagonal contract of the local adapter is a **callable, not an artifact format**: the
app supplies a loader returning a :class:`LocalModel` (a sync ``predict_batch``), and the
framework only schedules it — loading and every prediction run under
:func:`~forze.base.primitives.run_cpu` (bounded pool, invocation-deadline kill,
cancellation, context propagation). The framework never deserializes model artifacts:
unpickling is arbitrary code execution, so that trust decision stays with the artifact's
owner, inside the loader.

Thread safety: ``run_cpu``'s shared executor means concurrent calls hit the same model
object from multiple worker threads. A :class:`LocalModel` must therefore be thread-safe
(sklearn and ONNX Runtime sessions generally are); for one that is not, set
``serialize_calls=True`` on the config to serialize the route's calls — correctness over
throughput. The serialization happens **on the loop, before dispatch**: waiters park as
coroutines, never as blocked worker threads inside the shared CPU pool.
"""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Sequence
from typing import Any, Protocol, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import (
    DEFAULT_INFERENCE_CAPABILITIES,
    InferenceCapabilities,
    InferencePort,
    InferenceRunOptions,
    InferenceSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import OnceCell, run_cpu

from .adapter_common import bind_run_options, shape_outputs, validated_instances

# ----------------------- #

LOCAL_INFERENCE_BACKEND = "local"
"""Backend label used in capability refusals and boundary errors."""

# ....................... #


class LocalModel[In: BaseModel, Out: BaseModel](Protocol):
    """App-author-implemented synchronous model.

    Loading the artifact (pickle, safetensors, ONNX, joblib, …) — and trusting it — is
    entirely the loader's code; the framework only invokes this method on worker threads.
    Must be thread-safe unless the route sets ``serialize_calls=True``.
    """

    def predict_batch(self, instances: Sequence[In]) -> Sequence[Out]:
        """Score a batch, returning one prediction per instance, in input order."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalInferenceConfig:
    """Wiring config for one local inference route."""

    loader: Callable[[], LocalModel[Any, Any]] = attrs.field(repr=False)
    """Zero-argument callable returning the loaded model. Runs off the event loop, once
    per process (at startup by default). May block; must not return ``None``."""

    warm_on_startup: bool = True
    """Load the model at application startup (via the lifecycle step) and **fail boot
    closed** on a loader error — a service that would fail its first prediction should not
    come up. ``False`` defers loading to the first call."""

    serialize_calls: bool = False
    """Serialize the route's predictions for a model that is not thread-safe. Default
    off: the model is expected to tolerate concurrent worker-thread calls. The lock is
    awaited on the loop before dispatching to the CPU pool, so a waiting prediction
    never occupies a worker-thread slot."""

    deterministic: bool = False
    """Declare that the model returns the same output for the same input (advertised via
    capabilities; the adapter cannot verify it)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not callable(self.loader):
            raise exc.configuration(
                "LocalInferenceConfig.loader must be a zero-argument callable returning "
                "the loaded model."
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, eq=False)
class LocalModelHost:
    """Load-once holder for one route's model, shared across scopes.

    Owns the memoized model, the load guard (so concurrent first calls load once), and
    the optional serialization lock. Lives on the deps module — adapters are per-scope,
    the loaded model is per-process.
    """

    config: LocalInferenceConfig

    _cell: OnceCell[LocalModel[Any, Any]] = attrs.field(
        factory=OnceCell[LocalModel[Any, Any]],
        init=False,
    )
    _load_guard: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    _serialize_guard: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    """The ``serialize_calls`` lock — an ``asyncio.Lock`` held across the dispatch, never
    a ``threading.Lock`` inside the worker. A thread-side lock made every *waiting*
    prediction occupy a slot in the process-wide bounded executor (the same pool Argon2
    hashing and codec work depend on), and a parked thread cannot be cancelled — so the
    invocation deadline could not free the slots either. An awaiting coroutine costs no
    slot and cancels cleanly."""

    # ....................... #

    async def model(self) -> LocalModel[Any, Any]:
        """Return the loaded model, loading it off-loop exactly once."""

        cached = self._cell.peek()

        if cached is not None:
            return cached

        async with self._load_guard:
            cached = self._cell.peek()

            if cached is not None:
                return cached

            # deadline=False: loading is one-time plumbing — a request's deadline must not
            # kill the shared warm-up another caller will reuse.
            model = await run_cpu(self.config.loader, deadline=False)

            if model is None:  # pyright: ignore[reportUnnecessaryComparison]
                raise exc.configuration("Local inference loader returned None instead of a model.")

            return self._cell.set(model)

    # ....................... #

    async def run(
        self,
        model: LocalModel[Any, Any],
        instances: Sequence[Any],
    ) -> Sequence[Any]:
        """Dispatch one prediction batch to the CPU pool, serialized when configured.

        With ``serialize_calls`` the lock is acquired **here, on the loop, before the
        dispatch** — a waiter is a parked coroutine, not a worker thread blocked inside
        the shared executor (see ``_serialize_guard``). The worker thread only ever runs
        the model call itself.
        """

        if not self.config.serialize_calls:
            return await run_cpu(self._invoke, model, instances)

        async with self._serialize_guard:
            return await run_cpu(self._invoke, model, instances)

    # ....................... #

    @staticmethod
    def _invoke(
        model: LocalModel[Any, Any],
        instances: Sequence[Any],
    ) -> Sequence[Any]:
        """The worker-thread body: exactly the model call, nothing that can park."""

        return model.predict_batch(instances)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalInferenceAdapter[In: BaseModel, Out: BaseModel](InferencePort[In, Out]):
    """In-process ``InferencePort`` over a user-supplied model."""

    spec: InferenceSpec[In, Out]
    host: LocalModelHost

    # ....................... #

    @property
    def inference_capabilities(self) -> InferenceCapabilities:
        return attrs.evolve(
            DEFAULT_INFERENCE_CAPABILITIES,
            native_batch=True,
            supports_stream=True,
            deterministic=self.host.config.deterministic,
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

        model = await self.host.model()

        with bind_run_options(options):
            raw = await self.host.run(model, prepared)

        return shape_outputs(
            self.spec,
            raw,
            expected=len(prepared),
            backend=LOCAL_INFERENCE_BACKEND,
        )

    # ....................... #

    async def predict_stream(
        self,
        instances: AsyncIterator[Sequence[In]],
        *,
        options: InferenceRunOptions | None = None,
    ) -> AsyncGenerator[Sequence[Out]]:
        # Each chunk goes through predict_many, so every chunk boundary is a deadline
        # check and a cancellation point (run_cpu_map semantics without the buffering).
        async for chunk in instances:
            yield await self.predict_many(chunk, options=options)
