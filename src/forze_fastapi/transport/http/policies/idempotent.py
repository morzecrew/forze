"""Idempotency OpenAPI policy (handler logic lives in :mod:`idempotency.runner`)."""

from collections.abc import Sequence
from typing import Any, final

import attrs
from fastapi import Header
from fastapi.params import Depends
from fastapi.routing import APIRoute

from forze_fastapi.transport.http.idempotency.constants import IDEMPOTENCY_KEY_HEADER

from .base import Policy

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotentPolicy(Policy):
    """Document the idempotency header on OpenAPI routes."""

    def route_dependencies(self) -> Sequence[Any]:
        async def _document_idempotency_key(
            _key: str | None = Header(
                default=None,
                alias=IDEMPOTENCY_KEY_HEADER,
                description="Unique key for idempotent request replay.",
            ),
        ) -> None:
            return None

        return [Depends(_document_idempotency_key)]

    def openapi_extra(self) -> dict[str, Any] | None:
        return None

    def route_class(self) -> type[APIRoute] | None:
        return None
