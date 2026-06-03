"""Build :class:`HttpServiceSpec` from declarative integration classes."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.base.exceptions import exc

from .descriptors import BaseHttpIntegration, async_http_op

# ----------------------- #


def build_http_service_spec(
    cls: type[BaseHttpIntegration],
    *,
    name: str | StrEnum,
) -> HttpServiceSpec:
    """Materialize a frozen :class:`HttpServiceSpec` from a facade class.

    Operations are collected from the class MRO (subclass descriptors override
    namesakes on bases). Each :class:`async_http_op` must be assigned on a class
    body so :meth:`~async_http_op.__set_name__` binds :attr:`~async_http_op.op_name`.
    """

    descriptors = _collect_http_operations(cls)
    operations: dict[str, HttpOperationSpec[Any, Any]] = {}
    seen_op_names: set[str] = set()

    for attr_name, descriptor in descriptors.items():
        op_name = descriptor.op_name or attr_name

        if op_name in seen_op_names:
            raise exc.configuration(
                f"Duplicate HTTP operation name {op_name!r} on {cls.__name__} "
                f"(attribute {attr_name!r})",
            )

        seen_op_names.add(op_name)
        operations[op_name] = _operation_spec(op_name, descriptor)

    return HttpServiceSpec(name=name, operations=operations)


# ....................... #


def _collect_http_operations(
    cls: type[BaseHttpIntegration],
) -> dict[str, async_http_op[Any, Any]]:
    """Collect ``async_http_op`` descriptors from ``cls`` and its bases (child wins)."""

    collected: dict[str, async_http_op[Any, Any]] = {}

    for base in cls.__mro__:
        if base is object:
            continue

        for attr_name, value in base.__dict__.items():
            if not isinstance(value, async_http_op):
                continue

            if attr_name in collected:
                continue

            if value.op_name is None:
                raise exc.configuration(
                    f"HTTP operation {attr_name!r} on {base.__name__} was not bound; "
                    "assign async_http_op on the class body",
                )

            collected[attr_name] = value

    return collected


# ....................... #


def _operation_spec(
    op_name: str,
    descriptor: async_http_op[Any, Any],
) -> HttpOperationSpec[Any, Any]:
    return HttpOperationSpec(
        name=op_name,
        method=descriptor.method,
        path=descriptor.path,
        args_type=descriptor.request,
        return_type=descriptor.response,
        query_from=frozenset(descriptor.query_from),
        idempotent=descriptor.idempotent,
        site=descriptor.site,
        allows_empty_body=descriptor.allows_empty_body,
    )
