"""Httpx-backed :class:`~forze.application.contracts.http.HttpServicePort`."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.http import (
    HttpOperationSpec,
    HttpServicePort,
    HttpServiceSpec,
)
from forze.application.integrations.http import request_parts
from forze.base.exceptions import exc
from forze.base.exceptions._utils import reraise_mapped
from forze.base.primitives import StrKey
from forze_http.adapters._logger import logger
from forze_http.execution.deps.configs import HttpxHttpServiceConfig
from forze_http.kernel.client import HttpxClientPort
from forze_http.kernel.client.errors import httpx_chain_mapper

# ----------------------- #


def _return_type_allows_empty(return_type: type[BaseModel]) -> bool:
    return not any(
        field_info.is_required() for field_info in return_type.model_fields.values()
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class HttpxHttpServiceAdapter(HttpServicePort):
    """Invoke HTTP operations using a shared or routed httpx client."""

    client: HttpxClientPort
    config: HttpxHttpServiceConfig
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

            response = await self.client.request(
                operation.method,
                url,
                params=query,
                json=body,
                headers=headers,
                timeout=self.config.timeout.total_seconds(),
            )

            return self._parse_response(operation, response.content)

        except BaseException as error:
            logger.debug(
                "http.invoke.failed",
                site=site,
                op=str(operation.name),
                method=operation.method,
            )
            reraise_mapped(httpx_chain_mapper, error, site=site, details=details)

    # ....................... #

    def _parse_response(self, operation: HttpOperationSpec[Any, Any], content: bytes) -> BaseModel:
        if content:
            return operation.return_type.model_validate_json(content)

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
                "HttpxHttpServiceConfig.base_url is required for non-tenant routes",
            )

        url = f"{base.rstrip('/')}/{path.lstrip('/')}"
        return url, headers or None

    # ....................... #

    def _static_headers(self) -> dict[str, str]:
        headers = dict(self.config.default_headers)

        if self.config.auth is not None:
            headers.update(self.config.auth.auth_headers())

        return headers
