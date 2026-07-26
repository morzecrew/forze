"""Shared inference-plane conformance battery: one behaviour set, every adapter.

The inference port is served by four independent implementations — the in-memory oracle
(``MockInferenceAdapter``), the in-process one (``LocalInferenceAdapter``), and the served
one over two wire dialects (``kserve_v2``, ``mlflow``) — and the boundary rules they share
live in one module (``integrations.inference.adapter_common``) while the *enforcement*
sites do not. That is exactly where they drifted: three of the checks below exist because
running the same call against every leg produced different answers.

What each check pins:

1. ``predict_many`` refuses a batch over the backend's cap — all-or-nothing, never split.
2. ``predict_stream`` **serves** a chunk over that same cap instead of refusing it. The
   oracle used to route stream chunks through ``predict_many``, so it refused a chunk the
   served adapters answer by splitting their wire calls: correct streaming code failed
   under the mock and only under the mock. Check 1 is this check's positive control — the
   same oversized batch must still be refused by ``predict_many``, proving the cap is
   declared and live rather than quietly absent.
3. An empty batch is a no-op returning ``[]``, with no backend call.
4. An empty stream chunk yields an empty chunk, and does not end or shift the stream.
5. Plain mappings decode through the spec's input codec, like real instances.
6. An already-spent invocation budget refuses **before** the backend is called, with one
   shared code. The oracle used to serve a prediction here (a pure function cannot observe
   a deadline), so a deadline was unobservable anywhere but a live endpoint; the served and
   in-process adapters refused with codes of their own, so no caller could branch portably.
7. ``predict`` equals ``predict_many`` of one instance.
8. Chunk boundaries survive: chunk *N* of predictions answers chunk *N* of inputs.
9. One off-spec instance fails the whole call — no partial batch.

Scalar-shaped predictions (a backend answering one bare value per instance rather than a
record) are pinned per-adapter instead of here: only some legs can be made to produce them
— the ``kserve_v2`` dialect is columnar and never does — so a battery check would pass
vacuously on the rest.

SageMaker is deliberately not a leg: moto answers from a canned queue keyed by request
body, so the harness would have to know each check's wire-call pattern in advance. Its
equivalents live in ``tests/integration/test_forze_inference/test_sagemaker_live.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from datetime import timedelta
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import (
    UNSUPPORTED_INFERENCE_FEATURE_CODE,
    InferencePort,
)
from forze.application.integrations.inference import BUDGET_EXHAUSTED_CODE
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

BATCH_CAP = 2
"""The declared cap the two cap checks wire, chosen so a 3-instance call exceeds it."""

SPENT_BUDGET = timedelta(0)
"""A per-call timeout that is already spent when the adapter reads it — the tighten-only
binding makes it the effective deadline, so the pre-flight check sees zero remaining."""


class Features(BaseModel):
    """The battery's input model: one float and one string, both flat scalars.

    Flat scalars because the ``kserve_v2`` columnar dialect refuses nested fields at wiring
    time — the narrowest input every leg can serve.
    """

    x: float = 0.0
    tag: str = ""


class Scores(BaseModel):
    """The battery's output model: ``y`` doubles ``x``, ``tag_len`` measures ``tag``."""

    y: float = 0.0
    tag_len: int = 0


InferenceRoute = InferencePort[Features, Scores]
"""A port bound to the battery's spec, whose model doubles ``x`` and measures ``tag``."""


# ....................... #


async def one_chunk(*instances: Features) -> AsyncIterator[Sequence[Features]]:
    """Yield *instances* as a single stream chunk."""

    yield list(instances)


async def chunks(*groups: Sequence[Features]) -> AsyncIterator[Sequence[Features]]:
    """Yield each of *groups* as its own stream chunk, in order."""

    for group in groups:
        yield list(group)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InferenceHarness:
    """One backend's seams for the battery."""

    port: InferenceRoute
    """A port on a route whose model doubles ``x`` and reports ``len(tag)``."""

    backend: str
    """Label used in skip reasons, so a skipped leg names itself in the report."""

    capped_port: Callable[[int], InferenceRoute] | None = None
    """Build the same route with a declared ``max_batch_size``, or ``None``.

    A seam rather than a fixed port because the cap is a *wiring* fact on the served
    adapters (a config field) and a *registration* fact on the oracle, and neither can be
    imposed on an already-built port. ``None`` for a backend with no cap concept at all:
    the in-process adapter talks to no transport, so it has no batch limit to declare and
    the two cap checks have nothing to assert against it.
    """


Check = Callable[[InferenceHarness], Any]
"""One battery check. Async, but typed loosely so the tuple stays homogeneous."""


# ....................... #


def _capped(h: InferenceHarness) -> InferenceRoute:
    if h.capped_port is None:
        pytest.skip(f"the {h.backend} backend declares no batch cap")

    return h.capped_port(BATCH_CAP)


def _over_cap() -> list[Features]:
    return [Features(x=float(n), tag="t" * n) for n in range(BATCH_CAP + 1)]


def _doubled(instances: Sequence[Features]) -> list[tuple[float, int]]:
    return [(i.x * 2.0, len(i.tag)) for i in instances]


def _observed(scored: Sequence[Scores]) -> list[tuple[float, int]]:
    return [(s.y, s.tag_len) for s in scored]


# ....................... #


async def check_predict_many_refuses_a_batch_over_the_cap(h: InferenceHarness) -> None:
    """All-or-nothing: an oversized batch is refused whole, never silently split."""

    port = _capped(h)
    batch = _over_cap()

    # Within the cap the same route serves, so the refusal is the cap talking and not a
    # broken route.
    assert _observed(await port.predict_many(batch[:BATCH_CAP])) == _doubled(batch[:BATCH_CAP])

    with pytest.raises(CoreException) as ei:
        await port.predict_many(batch)

    assert ei.value.code == UNSUPPORTED_INFERENCE_FEATURE_CODE
    assert ei.value.kind == ExceptionKind.PRECONDITION


async def check_a_stream_chunk_over_the_cap_is_served_not_refused(h: InferenceHarness) -> None:
    """A cap sub-batches a stream chunk; only ``predict_many`` refuses on it.

    The positive control is the refusal itself: the same oversized batch must still be
    rejected through ``predict_many``, so this check cannot pass by the cap having
    silently gone missing. That the backend calls are actually *split* is a wire-level
    fact each adapter pins for itself — from here both a split and an unsplit call look
    the same, which is the point: the caller's chunking is independent of the transport.
    """

    port = _capped(h)
    batch = _over_cap()

    served = [chunk async for chunk in port.predict_stream(one_chunk(*batch))]

    assert len(served) == 1, "the caller's chunk boundary must survive sub-batching"
    assert _observed(served[0]) == _doubled(batch)

    with pytest.raises(CoreException) as ei:
        await port.predict_many(batch)

    assert ei.value.code == UNSUPPORTED_INFERENCE_FEATURE_CODE


async def check_an_empty_batch_is_a_no_op(h: InferenceHarness) -> None:
    """``predict_many([])`` returns ``[]`` without calling the backend."""

    assert list(await h.port.predict_many([])) == []


async def check_an_empty_chunk_yields_an_empty_chunk(h: InferenceHarness) -> None:
    """An empty chunk neither ends the stream nor shifts the ones around it."""

    served = [
        _observed(chunk)
        async for chunk in h.port.predict_stream(
            chunks([Features(x=1.0, tag="a")], [], [Features(x=2.0, tag="bb")])
        )
    ]

    assert served == [[(2.0, 1)], [], [(4.0, 2)]]


async def check_mapping_instances_decode_through_the_input_codec(h: InferenceHarness) -> None:
    """A plain mapping is accepted and scored exactly like a real instance."""

    records: list[Any] = [{"x": 1.5, "tag": "ab"}, {"x": -2.0, "tag": ""}]
    equivalent = [Features(x=1.5, tag="ab"), Features(x=-2.0, tag="")]

    assert _observed(await h.port.predict_many(records)) == _observed(
        await h.port.predict_many(equivalent)
    )


async def check_an_exhausted_budget_refuses_before_the_backend(h: InferenceHarness) -> None:
    """A spent budget is a pre-flight refusal, with the same code on every backend.

    The shared code is the load-bearing part: it is what lets a caller tell "the model was
    never asked" (nothing ran, nothing was billed) from a mid-call timeout, without
    knowing which backend it is talking to.
    """

    with pytest.raises(CoreException) as ei:
        await h.port.predict(Features(x=1.0, tag="a"), options={"timeout": SPENT_BUDGET})

    assert ei.value.code == BUDGET_EXHAUSTED_CODE
    assert ei.value.kind == ExceptionKind.TIMEOUT

    # Without the spent budget the same call succeeds — so the refusal is the deadline and
    # not an unrelated failure on the route.
    assert (await h.port.predict(Features(x=1.0, tag="a"))).y == 2.0


async def check_predict_matches_predict_many_of_one(h: InferenceHarness) -> None:
    """The single-instance convenience is the one-element batch, not its own path."""

    instance = Features(x=3.25, tag="abc")

    single = await h.port.predict(instance)
    batched = await h.port.predict_many([instance])

    assert [single] == list(batched)


async def check_predict_stream_preserves_chunk_boundaries(h: InferenceHarness) -> None:
    """Chunk *N* of predictions answers chunk *N* of inputs, in order."""

    first = [Features(x=1.0, tag="a"), Features(x=2.0, tag="bb")]
    second = [Features(x=3.0, tag="ccc")]

    served = [_observed(chunk) async for chunk in h.port.predict_stream(chunks(first, second))]

    assert served == [_doubled(first), _doubled(second)]


async def check_an_off_spec_instance_fails_the_whole_call(h: InferenceHarness) -> None:
    """One unusable instance fails the batch — there is no partial result."""

    batch: list[Any] = [Features(x=1.0, tag="a"), object()]

    with pytest.raises(CoreException) as ei:
        await h.port.predict_many(batch)

    assert ei.value.kind == ExceptionKind.VALIDATION


# ....................... #

INFERENCE_BATTERY: tuple[Check, ...] = (
    check_predict_many_refuses_a_batch_over_the_cap,
    check_a_stream_chunk_over_the_cap_is_served_not_refused,
    check_an_empty_batch_is_a_no_op,
    check_an_empty_chunk_yields_an_empty_chunk,
    check_mapping_instances_decode_through_the_input_codec,
    check_an_exhausted_budget_refuses_before_the_backend,
    check_predict_matches_predict_many_of_one,
    check_predict_stream_preserves_chunk_boundaries,
    check_an_off_spec_instance_fails_the_whole_call,
)
