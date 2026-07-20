"""KServe V2 / Open Inference Protocol, JSON-record (columnar) scope.

Each flat scalar input field becomes one named V2 input tensor over the batch (the
``content_type: "pd"`` columnar convention mlserver's pandas codec understands); output
tensors zip back into per-instance records by name. Only flat scalar fields (``bool`` /
``int`` / ``float`` / ``str``) are supported in this scope — nested models and binary
tensor encodings are refused at wiring time, not silently mangled.
"""

from collections.abc import Mapping, Sequence
from typing import Any, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.base.exceptions import exc

from .base import WireRequest

# ----------------------- #

KSERVE_V2_BACKEND = "kserve_v2"

_SCALAR_DATATYPES: dict[type, str] = {
    bool: "BOOL",  # before int: bool is an int subclass
    int: "INT64",
    float: "FP64",
    str: "BYTES",
}


def validate_flat_scalar_fields(
    spec: InferenceSpec[Any, Any],
) -> None:
    """Fail-closed wiring check: the V2 columnar scope needs flat scalar input fields."""

    offending = [
        name
        for name, field in spec.input.model_fields.items()
        if field.annotation not in _SCALAR_DATATYPES
    ]

    if offending:
        raise exc.configuration(
            f"Inference {spec.name!r}: the kserve_v2 protocol (JSON-record scope) "
            f"requires flat scalar input fields (bool/int/float/str); offending: "
            f"{', '.join(sorted(offending))}. Use the mlflow protocol for nested "
            "records, or a custom codec."
        )


# ....................... #


def _column_datatype(spec_name: str, field: str, values: Sequence[Any]) -> str:
    kinds: set[type[Any]] = {type(v) for v in values}

    if len(kinds) != 1 or next(iter(kinds)) not in _SCALAR_DATATYPES:
        raise exc.validation(
            f"Inference {spec_name!r}: field {field!r} must hold one flat scalar type "
            "across the batch for the kserve_v2 columnar encoding."
        )

    return _SCALAR_DATATYPES[next(iter(kinds))]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class KserveV2Protocol:
    """The Open Inference Protocol dialect (KServe, mlserver, Seldon, Triton HTTP)."""

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> WireRequest:
        rows = [instance.model_dump(mode="json") for instance in instances]
        count = len(rows)

        inputs: list[dict[str, Any]] = []

        for field in spec.input.model_fields:
            values = [row[field] for row in rows]
            inputs.append(
                {
                    "name": field,
                    "shape": [count],
                    "datatype": _column_datatype(str(spec.name), field, values),
                    "data": values,
                }
            )

        return (
            f"/v2/models/{model_name}/infer",
            {"inputs": inputs, "parameters": {"content_type": "pd"}},
        )

    # ....................... #

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:
        raw_outputs = body.get("outputs")

        # `list`, not `Sequence`: a JSON array decodes to a list, while a bare string is
        # also a Sequence — accepting one would iterate an error message character by
        # character instead of refusing the response.
        if not isinstance(raw_outputs, list) or not raw_outputs:
            raise exc.validation(
                f"Inference {spec.name!r}: kserve_v2 response has no 'outputs'.",
                code="inference_output_mismatch",
            )

        # mypy narrows the isinstance to list[Any] (cast "redundant"); pyright narrows to
        # list[Unknown] and needs it.
        outputs = cast(list[Any], raw_outputs)  # type: ignore[redundant-cast]
        columns: dict[str, Sequence[Any]] = {}

        for item in outputs:
            if not isinstance(item, Mapping):
                raise exc.validation(
                    f"Inference {spec.name!r}: malformed kserve_v2 output tensor.",
                    code="inference_output_mismatch",
                )

            tensor = cast(Mapping[str, Any], item)
            raw_data = tensor.get("data")
            data = (
                cast(list[Any], raw_data)  # type: ignore[redundant-cast]
                if isinstance(raw_data, list)
                else None
            )

            if data is None or len(data) != expected:
                raise exc.validation(
                    f"Inference {spec.name!r}: kserve_v2 output tensor "
                    f"{tensor.get('name')!r} does not hold one value per instance.",
                    code="inference_output_mismatch",
                )

            columns[str(tensor.get("name"))] = data

        out_fields = list(spec.output.model_fields)

        # Match output tensors by field name; a single unnamed/mismatched tensor maps
        # positionally onto a single-field output model.
        if not all(field in columns for field in out_fields):
            if len(out_fields) == 1 and len(columns) == 1:
                columns = {out_fields[0]: next(iter(columns.values()))}
            else:
                missing = sorted(set(out_fields) - set(columns))
                raise exc.validation(
                    f"Inference {spec.name!r}: kserve_v2 response is missing output "
                    f"tensors for: {', '.join(missing)}.",
                    code="inference_output_mismatch",
                )

        return [
            {field: columns[field][position] for field in out_fields}
            for position in range(expected)
        ]
