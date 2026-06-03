"""Specifications for outbound HTTP service integrations."""

from __future__ import annotations

import re
from typing import Any, Generic, Literal, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import BaseSpec
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #

HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

_PATH_PARAM_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

# ....................... #


def path_param_names(path: str) -> frozenset[str]:
    """Return placeholder names from an HTTP path template."""

    return frozenset(_PATH_PARAM_RE.findall(path))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpOperationSpec(Generic[In, Out]):
    """Specification for a single HTTP operation on a remote service."""

    name: StrKey
    """Logical operation name (route within the service spec)."""

    method: HttpMethod
    """HTTP method."""

    path: str
    """Path template relative to the service base URL (e.g. ``/v1/orders/{order_id}``)."""

    args_type: type[In] | None
    """Request argument model; ``None`` for bodyless calls with no path params."""

    return_type: type[Out]
    """Response model validated from JSON."""

    query_from: frozenset[str] = attrs.field(factory=frozenset)
    """Request fields serialized as query parameters."""

    idempotent: bool = False
    """Whether the operation is safe to retry."""

    site: str | None = None
    """Optional tracing / exception site override."""

    allows_empty_body: bool = False
    """When ``True``, an empty response body yields ``return_type.model_construct()``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        placeholders = path_param_names(self.path)

        if placeholders and self.args_type is None:
            raise exc.configuration(
                f"HTTP operation {self.name!r} path {self.path!r} requires "
                "args_type when path contains placeholders",
            )

        if self.args_type is not None and placeholders:
            field_names = set(self.args_type.model_fields)
            missing = placeholders - field_names

            if missing:
                raise exc.configuration(
                    f"HTTP operation {self.name!r}: path placeholders {sorted(missing)} "
                    f"are not fields on {self.args_type.__name__}",
                )

        if self.query_from:
            if self.args_type is None:
                raise exc.configuration(
                    f"HTTP operation {self.name!r}: query_from requires args_type",
                )

            unknown = self.query_from - set(self.args_type.model_fields)

            if unknown:
                raise exc.configuration(
                    f"HTTP operation {self.name!r}: query_from unknown fields "
                    f"{sorted(unknown)}",
                )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpServiceSpec(BaseSpec):
    """Catalog of HTTP operations for a logical remote service."""

    operations: dict[StrKey, HttpOperationSpec[Any, Any]]
    """Operations keyed by logical name."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.operations:
            raise exc.configuration(
                f"HttpServiceSpec {self.name!r} must declare at least one operation",
            )

        for key, op in self.operations.items():
            if str(op.name) != str(key):
                raise exc.configuration(
                    f"HttpServiceSpec {self.name!r}: operation key {key!r} "
                    f"does not match op.name {op.name!r}",
                )
