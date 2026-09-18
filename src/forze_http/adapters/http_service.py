"""httpx-backed :class:`~forze.application.contracts.http.HttpServicePort`."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, final, overload

import attrs
import httpx
from opentelemetry import propagate, trace
from pydantic import BaseModel, ValidationError

from forze.application.contracts.egress import EGRESS_SENSITIVE_ATTRIBUTE
from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.contracts.http import (
    RESPONSE_ERROR_DETAIL,
    HttpOperationSpec,
    HttpServicePort,
    HttpServiceSpec,
)
from forze.application.execution.context import remaining_time
from forze.application.integrations.http import form_fields, request_parts
from forze.base.exceptions import exc
from forze.base.exceptions._utils import reraise_mapped
from forze.base.primitives import StrKey
from forze.base.scrubbing import sanitize_pydantic_errors
from forze_http.adapters._logger import logger
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.kernel.client import HttpClientPort
from forze_http.kernel.client.errors import exc_interceptor

# ----------------------- #


def _return_type_allows_empty(return_type: type[BaseModel]) -> bool:
    return not any(field_info.is_required() for field_info in return_type.model_fields.values())


# ....................... #


def _declared_error(
    operation: HttpOperationSpec[Any, Any],
    error: BaseException,
) -> dict[str, Any] | None:
    """The declared fields of a rejected response, or ``None`` when there are none.

    Returns ``None`` for every case that is not "this operation declared an error model and
    the counterparty sent a body matching it" — no declaration, a non-status failure, an
    unreadable or non-conforming body. A malformed error response must not become a second
    failure: the caller is already being told something went wrong, and replacing that with
    a validation error about the *error* would lose the original.
    """

    if operation.error_type is None or not isinstance(error, httpx.HTTPStatusError):
        return None

    try:
        declared = operation.error_type.model_validate_json(error.response.content)

        # Projected to the model's own fields rather than dumped whole: a model configured
        # `extra="allow"` keeps whatever the provider sent, and dumping that would carry
        # undeclared fields — a trace id, an internal message, a token — into an exception
        # that reaches a log. Narrowing here rather than validating with `extra="forbid"`
        # keeps a provider free to add a field (RFC 6749 defines `error_uri`, and plenty of
        # providers send more) without the declaration silently going missing.
        projected = declared.model_dump(mode="json", include=set(type(declared).model_fields))

    except Exception:
        # Anything at all: a validation error, an unreadable body, a custom validator
        # raising something of its own. This is an optional decoration and the caller is
        # already being told the call failed, so a failure to describe that failure must
        # not become the failure — it would lose the original.
        return None

    return {RESPONSE_ERROR_DETAIL: projected}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class HttpServiceAdapter(HttpServicePort):
    """Invoke HTTP operations using a shared or routed httpx client."""

    client: HttpClientPort
    config: HttpServiceConfig
    spec: HttpServiceSpec

    # ....................... #

    @overload
    def invoke[In: BaseModel, Out: BaseModel](
        self,
        op: HttpOperationSpec[In, Out],
        args: In | None = None,
    ) -> Awaitable[Out]: ...

    @overload
    def invoke(
        self,
        op: StrKey,
        args: BaseModel | None = None,
    ) -> Awaitable[BaseModel]: ...

    # Repeated from the port rather than inherited: this class overrides `invoke`, and an
    # override replaces the overloads it is declared against. Without them a caller holding
    # the adapter itself gets `Any` back where the same call through the port gives the
    # operation's declared model.
    async def invoke(
        self,
        op: StrKey | HttpOperationSpec[Any, Any],
        args: BaseModel | None = None,
    ) -> Any:
        operation = self.spec.operation(op)
        path, query, body = request_parts(operation, args)
        site = operation.site or f"http.{self.spec.name}.{operation.name}"
        details: dict[str, Any] = {
            "op": str(operation.name),
            "method": operation.method,
            "service": str(self.spec.name),
        }

        try:
            url, headers = self._resolve_url_and_headers(path)
            details["url"] = url

            if self.config.propagate_deadline:
                budget = remaining_time()

                if budget is not None:
                    headers = {
                        **(headers or {}),
                        HTTP_HEADER_DEADLINE_BUDGET: f"{budget:.3f}",
                    }

            # Continue the distributed trace into the downstream service: inject the active span's W3C
            # context (traceparent + tracestate, via the global propagator so an app's choice is
            # honoured) into the outgoing headers. A no-op when no span is active (uninstrumented app);
            # if the app also instruments httpx, that instrumentation may overwrite with its own
            # client-span id — the trace linkage is preserved either way. No flag needed (the messaging
            # side gates on a column migration; HTTP carries no schema).
            headers = dict(headers or {})
            propagate.inject(headers)

            # Mark the call as leaving the trust boundary, so sensitive egress is queryable
            # rather than only reviewable in the wiring. The attribute lands on whichever
            # span is current — the port's CLIENT span where per-port spans are enabled, the
            # enclosing operation span otherwise — and on an uninstrumented app the
            # non-recording span drops it. There is nothing for forze_http to create: it has
            # never opened a span of its own.
            #
            # This marks the calls made through a declared service. A bare HttpClient an
            # app points at a provider itself has no HttpServiceConfig behind it, so it
            # carries neither the tag nor the wiring gate — and there is nowhere to put
            # one, since HttpConfig configures a transport and names no destination. The
            # governed unit is the service, which is what declares where data goes.
            if self.config.egress_sensitive:
                trace.get_current_span().set_attribute(EGRESS_SENSITIVE_ATTRIBUTE, True)

            form = operation.body_encoding == "form"
            declares_errors = operation.error_type is not None
            response = await self.client.request(
                operation.method,
                url,
                params=query,
                json=None if form else body,
                data=form_fields(operation, body) if form and body is not None else None,
                headers=headers,
                timeout=self.config.timeout.total_seconds(),
                # An operation that declared an error model needs the response, and the
                # client raises and maps a rejection before a caller can read one. So the
                # refusal moves here for those operations only — every other operation
                # keeps raising exactly where it always did.
                raise_for_status=not declares_errors,
            )

            if declares_errors:
                # Raised into the same `except` below, so the kind, the code and the
                # summary stay whatever the shared mapper already makes of this status.
                response.raise_for_status()

            return self._parse_response(operation, response.content)

        except Exception as error:
            logger.debug(
                "http.invoke.failed",
                site=site,
                op=str(operation.name),
                method=operation.method,
            )
            # The declared fields of a rejected response ride the same `details` the
            # mapper already carries, so the scrubber and the per-kind egress policy govern
            # them exactly as they govern every other error context.
            reraise_mapped(
                exc_interceptor.mapper,
                error,
                site=site,
                details={**details, **(_declared_error(operation, error) or {})},
            )

    # ....................... #

    def _parse_response(self, operation: HttpOperationSpec[Any, Any], content: bytes) -> BaseModel:
        if content:
            try:
                return operation.return_type.model_validate_json(content)

            except ValidationError as error:
                raise exc.validation(
                    f"HTTP operation {operation.name!r}: response failed validation",
                    code="http.response.validation",
                    details={"errors": sanitize_pydantic_errors(list(error.errors()))},
                ) from error

        if operation.allows_empty_body or _return_type_allows_empty(operation.return_type):
            return operation.return_type.model_construct()

        raise exc.validation(
            f"HTTP operation {operation.name!r} returned an empty body",
        )

    # ....................... #

    def _resolve_url_and_headers(self, path: str) -> tuple[str, dict[str, str] | None]:
        headers = self._static_headers()

        if self.config.tenant_aware:
            return path, headers or None

        base = self.config.base_url

        if base is None:
            raise exc.configuration(
                "HttpServiceConfig.base_url is required for non-tenant routes",
            )

        url = f"{base.rstrip('/')}/{path.lstrip('/')}"
        return url, headers or None

    # ....................... #

    def _static_headers(self) -> dict[str, str]:
        headers = dict(self.config.default_headers)

        if self.config.auth is not None:
            headers.update(self.config.auth.auth_headers())

        return headers
