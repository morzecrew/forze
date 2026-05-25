"""ETag OpenAPI policy (handler logic lives in :mod:`etag.response`)."""

from collections.abc import Sequence
from typing import Any, final

import attrs
from fastapi import Header
from fastapi.params import Depends
from fastapi.routing import APIRoute

from forze_fastapi.transport.http.etag.constants import IF_NONE_MATCH_HEADER_KEY

from .base import Policy

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ETagPolicy(Policy):
    """Document the ``If-None-Match`` header on OpenAPI routes."""

    def route_dependencies(self) -> Sequence[Any]:
        async def _document_if_none_match(
            _value: str | None = Header(
                default=None,
                alias=IF_NONE_MATCH_HEADER_KEY,
                description="Return 304 when the resource ETag matches.",
            ),
        ) -> None:
            return None

        return [Depends(_document_if_none_match)]

    def openapi_extra(self) -> dict[str, Any] | None:
        return None

    def route_class(self) -> type[APIRoute] | None:
        return None
