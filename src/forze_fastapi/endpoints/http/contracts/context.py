from typing import TYPE_CHECKING, Generic

import attrs
from fastapi import Request

from forze.application.execution import ExecutionContext
from forze.base.primitives import JsonDict

from .typevars import B, C, F, H, In, P, Q, R, Raw

if TYPE_CHECKING:
    from .specs import HttpEndpointSpec, HttpRequestDTO

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpEndpointContext(Generic[Q, P, H, C, B, In, Raw, R, F]):
    """Context for an HTTP endpoint."""

    raw_request: Request
    """The raw request."""

    raw_kwargs: JsonDict
    """The raw kwargs."""

    exec_ctx: ExecutionContext
    """The execution context."""

    facade: F
    """Typed usecases facade."""

    dto: "HttpRequestDTO[Q, P, H, C, B]"
    """The request DTO."""

    input: In
    """Mapped usecase input."""

    spec: "HttpEndpointSpec[Q, P, H, C, B, In, Raw, R, F]"
    """The endpoint specification."""

    operation_id: str
    """Fully-qualified operation id."""
