"""Dependency-free record helpers shared by the remote inference submodules.

Lives at the package top level (outside any extra-gated submodule) so ``http`` and
``sagemaker`` can share it without dragging in each other's client dependencies.
"""

from collections.abc import Mapping, Sequence
from typing import Any, cast

from forze.application.contracts.inference import InferenceSpec
from forze.application.integrations.inference import scalar_output_field
from forze.base.exceptions import exc

# ----------------------- #


def wrap_scalar_predictions(
    spec: InferenceSpec[Any, Any],
    predictions: Sequence[Any],
    *,
    backend: str,
) -> list[Mapping[str, Any]]:
    """Normalize response items to record mappings.

    A mapping item passes through; a scalar item wraps into the output model's single
    field — the response-side twin of "a scalar prediction wraps in a one-field model".
    A scalar response against a multi-field output model is a wire mismatch.

    The wrapping *rule* is the shared boundary one
    (:func:`~forze.application.integrations.inference.scalar_output_field`), so the wire
    decoders and the output shaping every adapter runs cannot drift apart on it. This
    helper only applies it early, while the response is still records.
    """

    records: list[Mapping[str, Any]] = []

    for item in predictions:
        if isinstance(item, Mapping):
            # isinstance narrows Any to Mapping[Unknown, Unknown]; the wire is JSON.
            records.append(cast(Mapping[str, Any], item))
            continue

        records.append({scalar_output_field(spec, backend=backend): item})

    return records


# ....................... #


def decode_predictions_body(
    spec: InferenceSpec[Any, Any],
    body: Mapping[str, Any],
    *,
    backend: str,
) -> list[Mapping[str, Any]]:
    """Decode the common ``{"predictions": [...]}`` response shape into records."""

    predictions = body.get("predictions")

    if not isinstance(predictions, Sequence) or isinstance(predictions, (str, bytes)):
        raise exc.validation(
            f"Inference {spec.name!r}: the {backend!r} response has no 'predictions' list.",
            code="inference_output_mismatch",
        )

    # mypy narrows the isinstance to Sequence[Any] (cast "redundant"); pyright narrows
    # to Sequence[Unknown] and needs it — the house pattern for this checker conflict.
    items = cast(Sequence[Any], predictions)  # type: ignore[redundant-cast]

    return wrap_scalar_predictions(spec, items, backend=backend)
