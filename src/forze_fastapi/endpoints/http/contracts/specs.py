from typing import Any, Generic, Literal, NotRequired, Sequence, TypedDict

import attrs

from forze.application.contracts.mapping import MapperPort
from forze.application.execution import FacadeOpRef
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO

from .constants import HttpBodyMode
from .ports import HttpEndpointFeaturePort
from .typevars import B, C, F, H, In, P, Q, R, Raw

# ----------------------- #


class HttpSpec(TypedDict):
    """Specification for an HTTP endpoint."""

    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"]
    """The HTTP method of the endpoint."""

    path: str
    """The path of the endpoint."""

    status_code: NotRequired[int]
    """The status code of the endpoint."""


# ....................... #


class HttpMetadataSpec(TypedDict, total=False):
    """Specification for endpoint metadata."""

    summary: str
    """The summary of the endpoint."""

    description: str
    """The description of the endpoint."""

    dependencies: Sequence[Any]
    """Per-route FastAPI dependencies (for example ``Security(bearer)``) merged into OpenAPI."""

    openapi_extra: dict[str, Any]
    """Merged into the generated OpenAPI operation (for example ``security`` requirements)."""

    responses: dict[int | str, dict[str, Any]]
    """Additional OpenAPI responses for this route."""

    include_in_schema: bool
    """When ``False``, omit the route from the OpenAPI schema."""


# ....................... #


class HttpRequestSpec(TypedDict, Generic[Q, P, H, C, B], total=False):
    """Specification for an endpoint request."""

    query_type: type[Q]
    """The type of the request query parameters model."""

    path_type: type[P]
    """The type of the request path parameters model."""

    header_type: type[H]
    """The type of the request header parameters model."""

    cookie_type: type[C]
    """The type of the request cookie parameters model."""

    body_type: type[B]
    """The type of the request body model."""

    body_mode: HttpBodyMode
    """The mode of the request body. Defaults to 'json'."""


# ....................... #


class HttpRequestDTO(BaseDTO, Generic[Q, P, H, C, B]):
    """The request DTO for the endpoint."""

    query: Q | None = None
    """The query parameters of the request."""

    path: P | None = None
    """The path parameters of the request."""

    header: H | None = None
    """The header parameters of the request."""

    cookie: C | None = None
    """The cookie parameters of the request."""

    body: B | None = None
    """The body of the request."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpEndpointSpec(Generic[Q, P, H, C, B, In, Raw, R, F]):
    """Specification for an HTTP endpoint."""

    http: HttpSpec
    """The HTTP specification of the endpoint."""

    metadata: HttpMetadataSpec | None = attrs.field(default=None)
    """The metadata specification of the endpoint."""

    features: Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F]] | None = (
        attrs.field(default=None)
    )
    """The features specification of the endpoint."""

    request: HttpRequestSpec[Q, P, H, C, B] | None = attrs.field(default=None)
    """The request specification of the endpoint."""

    response: type[R | None] = attrs.field(default=type(None))
    """The response model type of the endpoint."""

    mapper: MapperPort[HttpRequestDTO[Q, P, H, C, B], In]
    """The mapper that maps the request to the input model."""

    response_mapper: MapperPort[Raw, R] | None = attrs.field(default=None)
    """Maps usecase output to the HTTP response model; omit when Raw is R (identity)."""

    facade_type: type[F]
    """The type of the usecases facade to use for the endpoint."""

    call: FacadeOpRef[In, Raw]
    """The call operation to use for the endpoint."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        path_params_presented = any(x in self.http["path"] for x in ["{", "}"])
        path_type_provided = (
            self.request is not None and self.request.get("path_type") is not None
        )

        if path_params_presented and not path_type_provided:
            raise CoreError(
                "path_type must be provided if path contains path parameters"
            )

        if not path_params_presented and path_type_provided:
            raise CoreError(
                "path_type must not be provided if path does not contain path parameters"
            )

        if self.http["method"] == "DELETE":
            if self.http.get("status_code", 204) != 204:
                raise CoreError("DELETE method must have status code 204")

            if self.response is not type(None):
                raise CoreError("DELETE method must not have a response model")


# ....................... #


class SimpleHttpEndpointSpec(TypedDict, total=False):
    """Extra contract for built-in HTTP endpoints."""

    path_override: str
    metadata: HttpMetadataSpec
