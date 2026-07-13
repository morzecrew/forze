"""httpx-backed :class:`~forze.application.contracts.http.HttpServicePort`."""

from __future__ import annotations

from typing import Any, final

import attrs
from opentelemetry import propagate
from pydantic import BaseModel, ValidationError

from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.contracts.http import (
    HttpOperationSpec,
    HttpServicePort,
    HttpServiceSpec,
)
from forze.application.execution.context import remaining_time
from forze.application.integrations.http import request_parts
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


@final
@attrs.define(slots=True, kw_only=True)
class HttpServiceAdapter(HttpServicePort):
    """Invoke HTTP operations using a shared or routed httpx client."""

    client: HttpClientPort
    config: HttpServiceConfig
    spec: HttpServiceSpec

    # ....................... #

    async def invoke(
        self,
        op: StrKey,
        args: BaseModel | None = None,
    ) -> BaseModel:
        operation = self._operation(op)
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

            response = await self.client.request(
                operation.method,
                url,
                params=query,
                json=body,
                headers=headers,
                timeout=self.config.timeout.total_seconds(),
            )

            return self._parse_response(operation, response.content)

        except Exception as error:
            logger.debug(
                "http.invoke.failed",
                site=site,
                op=str(operation.name),
                method=operation.method,
            )
            reraise_mapped(exc_interceptor.mapper, error, site=site, details=details)

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

    def _operation(self, op: StrKey) -> HttpOperationSpec[Any, Any]:
        key = str(getattr(op, "value", op))

        if key not in self.spec.operations:
            raise exc.validation(f"Unknown HTTP operation {key!r} for {self.spec.name!r}")

        return self.spec.operations[key]

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
