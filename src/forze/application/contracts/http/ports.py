"""Ports for outbound HTTP service integrations."""

from typing import Awaitable, Protocol, runtime_checkable

from pydantic import BaseModel

from forze.base.primitives import StrKey

from .specs import HttpServiceSpec

# ----------------------- #


@runtime_checkable
class HttpServicePort(Protocol):
    """Port for invoking operations on a configured remote HTTP service."""

    spec: HttpServiceSpec
    """Service specification bound to this port instance."""

    def invoke(
        self,
        op: StrKey,
        args: BaseModel | None = None,
    ) -> Awaitable[BaseModel]:
        """Execute operation ``op`` and return a validated response model."""

        ...  # pragma: no cover
