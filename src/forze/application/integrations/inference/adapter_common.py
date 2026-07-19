"""Shared input/output handling for inference adapters.

Every adapter applies the same boundary policy: inputs pass through untouched when the
caller hands real spec-input instances (no hot-path re-validation — per-instance Pydantic
validation of a large batch is a real cost the contract consciously skips), mappings are
decoded through the spec's input codec, and anything else is rejected; outputs that are
not already spec-output instances are decoded through the output codec so a backend
response that does not fit the declared type fails **at the port boundary** instead of
leaking a foreign shape into handler code.
"""

from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, cast

from pydantic import BaseModel

from forze.application.contracts.inference import InferenceRunOptions, InferenceSpec
from forze.base.exceptions import exc
from forze.base.primitives import bind_deadline

# ----------------------- #

OUTPUT_MISMATCH_CODE = "inference_output_mismatch"
"""Error code raised when a backend's response does not decode to the spec's output model."""

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
            checked.append(spec.resolved_input_codec.decode_mapping(record))
            continue

        raise exc.validation(
            f"Inference {spec.name!r} instance {position} must be a "
            f"{spec.input.__name__} instance, got {type(instance).__name__}."
        )

    return checked


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
    mapping decodes through the output codec, anything else raises ``validation`` with
    code :data:`OUTPUT_MISMATCH_CODE` at the port boundary.
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

        if isinstance(prediction, Mapping):
            record = dict(cast(Mapping[str, Any], prediction))
            try:
                shaped.append(spec.resolved_output_codec.decode_mapping(record))
            except Exception as e:
                raise exc.validation(
                    f"Inference {spec.name!r}: prediction {position} from the {backend!r} "
                    f"backend does not decode to {spec.output.__name__}.",
                    code=OUTPUT_MISMATCH_CODE,
                ) from e
            continue

        raise exc.validation(
            f"Inference {spec.name!r}: prediction {position} from the {backend!r} backend "
            f"must be a {spec.output.__name__} or a mapping, got {type(prediction).__name__}.",
            code=OUTPUT_MISMATCH_CODE,
        )

    return shaped


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
