"""Shared input/output handling for inference adapters.

Every adapter applies the same boundary policy: inputs pass through untouched when the
caller hands real spec-input instances (no hot-path re-validation — per-instance Pydantic
validation of a large batch is a real cost the contract consciously skips), mappings are
decoded through the spec's input codec, and anything else is rejected; outputs that are
not already spec-output instances are decoded through the output codec so a backend
response that does not fit the declared type fails **at the port boundary** instead of
leaking a foreign shape into handler code.

The policy is shared rather than per-adapter on purpose: the boundary is where a backend
and the in-memory oracle must agree, so any rule that lives in one adapter is a rule the
others silently do not have.
"""

from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, cast

from pydantic import BaseModel

from forze.application.contracts.inference import (
    InferenceCapabilities,
    InferenceRunOptions,
    InferenceSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import bind_deadline, remaining_time

# ----------------------- #

OUTPUT_MISMATCH_CODE = "inference_output_mismatch"
"""Error code raised when a backend's response does not decode to the spec's output model."""

BUDGET_EXHAUSTED_CODE = "inference_budget_exhausted"
"""Error code raised when the invocation budget is gone *before* the backend is called.

Distinct from a backend's own timeout code on purpose: this one says the model was never
asked, so nothing ran, nothing was billed, and the call has no side effect to reason about
— while a mid-call timeout leaves all three questions open. Every adapter raises it from
the same pre-flight check, so a caller can branch on it portably.
"""

# ....................... #


def validated_instances[In: BaseModel](
    spec: InferenceSpec[In, Any],
    instances: Sequence[Any],
) -> Sequence[In]:
    """Return *instances* as spec-input models, decoding mappings, rejecting the rest.

    A real ``spec.input`` instance passes through without re-validation (the hot path); a
    mapping is decoded through the input codec; anything else fails the whole call — the
    all-or-nothing contract of ``predict_many``.
    """

    checked: list[In] = []

    for position, instance in enumerate(instances):
        if isinstance(instance, spec.input):
            checked.append(instance)
            continue

        if isinstance(instance, Mapping):
            # isinstance narrows Any to Mapping[Unknown, Unknown]; the wire is JSON.
            record = dict(cast(Mapping[str, Any], instance))

            try:
                checked.append(spec.resolved_input_codec.decode_mapping(record))

            except Exception as e:
                # A mapping that does not fit the input model is a caller error, not an
                # internal one — surface it in the port's taxonomy like a bad output does,
                # instead of leaking the codec's own exception type.
                raise exc.validation(
                    f"Inference {spec.name!r} instance {position} does not decode to "
                    f"{spec.input.__name__}."
                ) from e

            continue

        raise exc.validation(
            f"Inference {spec.name!r} instance {position} must be a "
            f"{spec.input.__name__} instance, got {type(instance).__name__}."
        )

    return checked


# ....................... #


def scalar_output_field(
    spec: InferenceSpec[Any, Any],
    *,
    backend: str,
) -> str:
    """The single output field a bare scalar prediction wraps into, fail-closed.

    Returning one value per instance rather than a record is the most common shape a real
    model produces (``sklearn.predict`` yields an array of floats, and both the MLflow and
    SageMaker ``predictions`` dialects pass scalars straight through), so the boundary
    wraps it — but only into a one-field output model, where the target is unambiguous. A
    scalar against a multi-field output model has no such reading and is a wire mismatch.
    """

    fields = list(spec.output.model_fields)

    if len(fields) != 1:
        raise exc.validation(
            f"Inference {spec.name!r}: the {backend!r} backend returned scalar predictions "
            f"but {spec.output.__name__} has {len(fields)} fields.",
            code=OUTPUT_MISMATCH_CODE,
        )

    return fields[0]


# ....................... #


def shape_outputs[Out: BaseModel](
    spec: InferenceSpec[Any, Out],
    raw: Sequence[Any],
    *,
    expected: int,
    backend: str,
) -> list[Out]:
    """Decode a backend's raw predictions to spec-output models, fail-closed.

    Enforces cardinality (*expected* predictions, in input order — the backend cannot
    silently drop or reorder) and shape: a real ``spec.output`` instance passes through, a
    mapping decodes through the output codec, and a bare scalar wraps into a single-field
    output model (see :func:`scalar_output_field`). Anything that still does not fit the
    declared type raises ``validation`` with code :data:`OUTPUT_MISMATCH_CODE` at the port
    boundary.
    """

    if len(raw) != expected:
        raise exc.validation(
            f"Inference {spec.name!r}: the {backend!r} backend returned {len(raw)} "
            f"predictions for {expected} instances.",
            code=OUTPUT_MISMATCH_CODE,
        )

    shaped: list[Out] = []

    for position, prediction in enumerate(raw):
        if isinstance(prediction, spec.output):
            shaped.append(prediction)
            continue

        record = (
            dict(cast(Mapping[str, Any], prediction))
            if isinstance(prediction, Mapping)
            # A non-record prediction wraps into the one-field output model, so an
            # in-process model and a served endpoint that both answer with one value
            # per instance are shaped identically instead of only the remote one working.
            else {scalar_output_field(spec, backend=backend): prediction}
        )

        try:
            shaped.append(spec.resolved_output_codec.decode_mapping(record))

        except Exception as e:
            raise exc.validation(
                f"Inference {spec.name!r}: prediction {position} from the {backend!r} "
                f"backend does not decode to {spec.output.__name__}.",
                code=OUTPUT_MISMATCH_CODE,
            ) from e

    return shaped


# ....................... #


def resolve_wire_cap(
    options: InferenceRunOptions | None,
    caps: InferenceCapabilities,
    *,
    backend: str,
) -> int | None:
    """The effective per-call batch size for ``predict_stream``, or ``None`` for unbounded.

    The tighter of the caller's ``max_batch_size`` hint and the backend's declared cap.
    Shared because all three streaming adapters need the same answer, and because the
    result feeds :func:`itertools.batched`, which raises a bare ``ValueError`` below 1 —
    a caller passing ``0`` would crash the stream instead of being refused. The declared
    cap is already validated at construction
    (:class:`~forze.application.contracts.inference.capabilities.InferenceCapabilities`),
    so only the per-call hint is checked here, and as a caller error rather than a
    configuration one.

    :raises CoreException: ``precondition`` when the per-call hint is below 1.
    """

    requested = (options or {}).get("max_batch_size")

    if requested is not None and requested < 1:
        raise exc.precondition(
            f"Inference max_batch_size={requested} must be at least 1; omit it for the "
            f"{backend!r} backend's own cap.",
        )

    caps_list = [cap for cap in (requested, caps.max_batch_size) if cap is not None]

    return min(caps_list) if caps_list else None


# ....................... #


def ensure_budget(*, backend: str) -> None:
    """Refuse before calling *backend* when the invocation budget is already gone.

    Called by every adapter at the top of its scoring path, inside the per-call option
    binding so a ``timeout`` of zero counts. Without it an adapter whose backend call
    cannot observe a deadline — an in-memory oracle answering from a pure function — would
    serve a prediction that every real backend refuses, and a deadline could not be
    exercised anywhere except against a live endpoint.

    :raises CoreException: ``timeout`` with code :data:`BUDGET_EXHAUSTED_CODE`.
    """

    remaining = remaining_time()

    if remaining is not None and remaining <= 0.0:
        raise exc.timeout(
            f"Inference budget is exhausted; the {backend!r} backend was not called.",
            code=BUDGET_EXHAUSTED_CODE,
        )


# ....................... #


@contextmanager
def bind_run_options(options: InferenceRunOptions | None) -> Generator[None]:
    """Apply per-call run options for the duration of one port call.

    ``timeout`` binds a tighten-only deadline (the effective budget is the earlier of the
    per-call timeout and the ambient invocation deadline); an absent option is a no-op
    passthrough, so adapters wrap every call unconditionally.
    """

    timeout = options.get("timeout") if options else None

    with bind_deadline(timeout.total_seconds() if timeout is not None else None):
        yield
