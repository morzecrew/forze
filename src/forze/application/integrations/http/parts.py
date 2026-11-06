"""Split request models into HTTP path, query, and body parts."""

from math import isfinite
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec, path_param_names
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


def request_parts(
    op: HttpOperationSpec[Any, Any],
    args: BaseModel | None,
) -> tuple[str, JsonDict | None, JsonDict | None]:
    """Return ``(path, query_params, json_body)`` for an HTTP operation.

    Path and query values name what is requested, so they read every field, declared
    defaults included, and leave out a ``None``: such a query parameter is not sent and
    such a path placeholder is refused as missing. The body carries only the fields the
    caller set (``exclude_unset``), so an explicit ``None`` goes out as ``null`` and an
    unset field not at all, which is how a PATCH tells "clear it" from "leave it".

    :param op: Operation specification.
    :param args: Request arguments, or ``None`` when the operation has no inputs.
    :raises exc.validation: When ``args`` is required but missing, or a path parameter is
        ``None``.
    """

    placeholders = path_param_names(op.path)
    path = op.path

    if args is None:
        if placeholders:
            raise exc.validation(
                f"HTTP operation {op.name!r} requires arguments for path placeholders",
            )

        return path, None, None

    # Only path and query fields need their defaults: a body field left unset is never
    # dumped, so a default the encoder cannot serialize does not fail the request.
    if op.method == "GET" and not op.query_from:
        values = args.model_dump(mode="json", exclude_none=True)

    else:
        named = set(placeholders) | set(op.query_from or ())
        values = args.model_dump(mode="json", exclude_none=True, include=named)

    body = {
        k: v
        for k, v in args.model_dump(mode="json", exclude_unset=True).items()
        if k not in placeholders
    }

    for name in placeholders:
        if name not in values:
            raise exc.validation(
                f"HTTP operation {op.name!r}: missing path parameter {name!r}",
            )

        segment = quote(str(values.pop(name)), safe="")
        path = path.replace(f"{{{name}}}", segment)

    query_names = op.query_from

    if op.method == "GET":
        if not query_names:
            # Without `query_from`, every field of a GET that is not in the path is a query
            # parameter — the body's keys too, since an explicit `None` is only there.
            query_names = frozenset(values.keys() | body.keys())

        remainder = body.keys() - query_names

        if remainder:
            raise exc.validation(
                f"HTTP GET operation {op.name!r} does not allow body fields: {sorted(remainder)}",
            )

    query = {k: values[k] for k in query_names if k in values}
    body = {k: v for k, v in body.items() if k not in query_names}

    return path, query or None, body or None


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
