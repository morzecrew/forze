"""Split request models into HTTP path, query, and body parts."""

from typing import Any

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

        path = path.replace(f"{{{name}}}", str(data.pop(name)))

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
