"""Specifications for outbound HTTP service integrations."""

from __future__ import annotations

import re
from typing import Any, Final, Generic, Literal, TypeVar, final

import attrs
from pydantic import BaseModel, ConfigDict

from forze.application.contracts.base import BaseSpec
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #

HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
HttpBodyEncoding = Literal["json", "form"]
HttpResponseDecoding = Literal["json", "raw"]

RESPONSE_ERROR_DETAIL: Final[str] = "response_error"
"""``details`` key carrying the validated body of a rejected response.

Present only on an operation that declared :attr:`HttpOperationSpec.error_type`, and holding
only that model's own fields — the channel is the scrubbed ``details`` every other error
context travels on, so an undeclared body never reaches it."""

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

_PATH_PARAM_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

# ....................... #


class RawBody(BaseModel):
    """A success response body handed over undecoded.

    What an operation declaring ``response_decoding="raw"`` returns — CSV, plain text, XML,
    an archive: anything the caller parses itself. A subclass may add parsing helpers or a
    validator over :attr:`media_type`; one that refuses the response surfaces as the error
    a JSON body that fails validation raises."""

    model_config = ConfigDict(frozen=True)

    content: bytes
    """The body exactly as received; ``b""`` for an empty one, such as a ``204``."""

    media_type: str | None = None
    """The response ``Content-Type`` without its parameters, lower-cased; ``None`` when absent."""

    charset: str | None = None
    """The ``charset`` parameter of the response ``Content-Type``, or ``None``."""

    # ....................... #

    @property
    def text(self) -> str:
        """:attr:`content` decoded with :attr:`charset`, or UTF-8 when there is none.

        Undecodable bytes are replaced rather than raised, and so is a charset Python does not
        know: the counterparty names it, so a typo there must not fail a body that is
        otherwise fine."""

        try:
            return self.content.decode(self.charset or "utf-8", errors="replace")

        except LookupError:
            return self.content.decode("utf-8", errors="replace")


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
    """Response model: validated from JSON, or :class:`RawBody` (or a subclass) when
    :attr:`response_decoding` is ``"raw"``."""

    query_from: frozenset[str] = attrs.field(factory=frozenset[str])
    """Request fields serialized as query parameters.

    On a ``GET`` an empty set means every field outside the path. A query field goes out
    with its declared default when the caller leaves it unset, and stays out when its value
    is ``None``. Body fields are sent only when the caller set them. See
    :func:`~forze.application.integrations.http.request_parts`."""

    idempotent: bool = False
    """Whether the operation is safe to retry."""

    site: str | None = None
    """Optional tracing / exception site override."""

    allows_empty_body: bool = False
    """When ``True``, an empty response body yields ``return_type.model_construct()``.

    Inert on a ``"raw"`` operation, where an empty body is ``RawBody(content=b"")``."""

    body_encoding: HttpBodyEncoding = "json"
    """How a request body is encoded — JSON, or ``application/x-www-form-urlencoded``.

    Per operation rather than per service because one provider legitimately mixes the two:
    an OAuth token endpoint requires form encoding (RFC 6749) while the same service's data
    API speaks JSON, and they share a base URL and a credential. A form body carries scalars
    only — see :func:`~forze.application.integrations.http.form_fields`. Inert on a bodyless
    method: it describes how a body is encoded, and a ``GET`` has none."""

    response_decoding: HttpResponseDecoding = "json"
    """How a success response body is decoded — validated as JSON, or handed over raw.

    ``"raw"`` is for a counterparty that does not answer JSON: a CSV export, plain text, XML,
    a binary payload. The operation then returns the whole body as :attr:`return_type`, which
    must be :class:`RawBody` or a subclass, and the caller parses it. Only the success body
    changes: a rejected response is mapped exactly as on a JSON operation, :attr:`error_type`
    included. For a body read as it arrives rather than whole, use
    :meth:`~forze.application.contracts.http.HttpServicePort.stream`."""

    error_type: type[BaseModel] | None = None
    """Model an operation declares for its error responses, or ``None``.

    When set, a response the transport rejects has its body validated against this model and
    the result attached to the raised exception's ``details`` under
    :data:`~forze.application.contracts.http.RESPONSE_ERROR_DETAIL`, so a caller can read the
    fields it declared — a provider's ``error`` code, typically, which decides whether a
    failure is worth retrying. Only declared fields travel, and they ride the same scrubbed
    ``details`` channel every other error context uses. A body that does not validate
    changes nothing: the exception is exactly the one raised with no ``error_type`` at all."""

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
                    f"HTTP operation {self.name!r}: query_from unknown fields {sorted(unknown)}",
                )

        if self.response_decoding == "raw" and not issubclass(self.return_type, RawBody):
            raise exc.configuration(
                f"HTTP operation {self.name!r}: response_decoding 'raw' requires return_type "
                f"RawBody or a subclass, got {self.return_type.__name__}",
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

    # ....................... #

    def operation(self, op: StrKey | HttpOperationSpec[Any, Any]) -> HttpOperationSpec[Any, Any]:
        """The operation *op* names, whether it arrives as a key or as the spec itself.

        Two forms because callers have two different things in hand. A name is what config
        and a registry have; the spec object is what carries :attr:`HttpOperationSpec.args_type`
        and :attr:`HttpOperationSpec.return_type`, and handing it in is what lets those reach
        the caller's own variable instead of arriving as a bare ``BaseModel``.

        Passing a spec asserts that *this* service declares it. A spec lifted from a sibling
        service can share a name and declare a different ``return_type``, and the mismatch
        would surface as a validation failure against the wrong model — or, worse, as a
        success, if both models happen to accept the payload.

        :raises CoreException: ``validation`` when this service declares no operation under
            that name, or declares a different one.
        """

        key = str(op.name) if isinstance(op, HttpOperationSpec) else str(getattr(op, "value", op))
        declared = self.operations.get(key)

        if declared is None:
            raise exc.validation(f"Unknown HTTP operation {key!r} for {self.name!r}")

        if isinstance(op, HttpOperationSpec) and declared != op:
            raise exc.validation(
                f"HTTP operation {key!r} is not the one {self.name!r} declares: the spec "
                "passed belongs to another service, or is a stale copy of this one's.",
            )

        return declared
