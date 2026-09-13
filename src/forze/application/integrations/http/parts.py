"""Split request models into HTTP path, query, and body parts."""

from math import isfinite
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec, path_param_names
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


def _dump_model(model: BaseModel) -> JsonDict:
    return model.model_dump(mode="json", exclude_unset=True)


# ....................... #


def request_parts(
    op: HttpOperationSpec[Any, Any],
    args: BaseModel | None,
) -> tuple[str, JsonDict | None, JsonDict | None]:
    """Return ``(path, query_params, json_body)`` for an HTTP operation.

    :param op: Operation specification.
    :param args: Request arguments, or ``None`` when the operation has no inputs.
    :raises exc.validation: When ``args`` is required but missing.
    """

    placeholders = path_param_names(op.path)
    path = op.path

    if args is None:
        if placeholders:
            raise exc.validation(
                f"HTTP operation {op.name!r} requires arguments for path placeholders",
            )

        return path, None, None

    data = _dump_model(args)

    for name in placeholders:
        if name not in data:
            raise exc.validation(
                f"HTTP operation {op.name!r}: missing path parameter {name!r}",
            )

        segment = quote(str(data.pop(name)), safe="")
        path = path.replace(f"{{{name}}}", segment)

    query: JsonDict | None = None
    body: JsonDict | None = None

    if op.method == "GET":
        if op.query_from:
            query = {k: data[k] for k in op.query_from if k in data}
            remainder = {k: v for k, v in data.items() if k not in op.query_from}

            if remainder:
                raise exc.validation(
                    f"HTTP GET operation {op.name!r} does not allow body fields: "
                    f"{sorted(remainder)}",
                )
        elif data:
            query = dict(data)

        return path, query or None, None

    if op.query_from:
        query = {k: data.pop(k) for k in op.query_from if k in data}

    body = data or None

    return path, query, body


# ....................... #


def form_fields(op: HttpOperationSpec[Any, Any], body: JsonDict) -> dict[str, str]:
    """Flatten *body* into ``application/x-www-form-urlencoded`` fields.

    ``form`` bodies are flat by definition — the encoding has no way to express a nested
    object or a list — so this refuses anything that is not a scalar, naming the field.
    The alternative is letting the transport stringify a mapping into ``"{'a': 1}"`` and
    sending a request no server can parse, which fails as a provider error rather than as
    the wiring mistake it is.

    A non-finite number is refused for the same reason a mapping is: ``str(float("nan"))``
    is ``"nan"``, which a form endpoint rejects or misreads.

    ``None`` is omitted rather than sent as an empty value: a field a caller left unset and
    a field it set to empty text mean different things to a token endpoint. Booleans
    serialize lowercase, which is what every form-encoded API this exists for reads.

    :param op: The operation, for naming in a refusal.
    :param body: The dumped request model.
    :returns: Field names mapped to their encoded values.
    :raises CoreException: ``validation`` when a value cannot be a form field.
    """

    fields: dict[str, str] = {}

    for name, value in body.items():
        if value is None:
            continue

        if isinstance(value, bool):
            fields[name] = "true" if value else "false"

            continue

        if isinstance(value, (int, float)) and not isfinite(value):
            # `str(float("nan"))` is `"nan"` — a value a form endpoint either rejects or
            # reads as something else entirely. It is a scalar by type and not one a form
            # can carry, which is the same line the refusal below draws.
            raise exc.validation(
                f"HTTP operation {op.name!r}: field {name!r} is {value} and a "
                "form-encoded body carries finite numbers only",
                details={"op": str(op.name), "field": name},
            )

        if isinstance(value, (str, int, float)):
            fields[name] = str(value)

            continue

        raise exc.validation(
            f"HTTP operation {op.name!r}: field {name!r} is a "
            f"{type(value).__name__} and a form-encoded body carries scalars only",
            details={"op": str(op.name), "field": name},
        )

    return fields
