"""Unit tests for persistence gateway mixins."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.application.integrations.persistence import (
    FilterParserMixin,
    ModelCodecGatewayMixin,
    TenantResolvedRelationMixin,
)
from forze.base.serialization import ModelCodec, default_model_codec


class _Model(BaseModel):
    id: UUID
    name: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class _CodecGateway(ModelCodecGatewayMixin[_Model]):
    model_type: type[_Model]
    codec: ModelCodec[_Model, Any]


@attrs.define(slots=True, kw_only=True, frozen=True)
class _FilterGateway(FilterParserMixin):
    filter_limits: QueryFilterLimits | None = None
    filter_parser: QueryFilterExpressionParser = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self.init_filter_parser()


@attrs.define(slots=True, kw_only=True, frozen=True)
class _TenantGateway(TenantResolvedRelationMixin):
    tenant_aware: bool = True
    tenant_provider: Any = None


def test_model_codec_gateway_read_fields_cached() -> None:
    gw = _CodecGateway(
        model_type=_Model,
        codec=default_model_codec(_Model),
    )
    assert gw.read_fields == frozenset({"id", "name"})
    assert gw.read_fields is gw.read_fields


def test_filter_parser_compile_filters_none() -> None:
    gw = _FilterGateway(filter_limits=None)
    assert gw.compile_filters(None) is None


def test_tenant_id_for_resolve_requires_tenant_when_aware() -> None:
    gw = _TenantGateway(tenant_provider=lambda: None)
    with pytest.raises(Exception, match="Tenant ID is required"):
        gw._tenant_id_for_resolve()


def test_tenant_id_for_resolve_returns_id() -> None:
    tid = uuid4()

    @attrs.define
    class _Tenant:
        tenant_id: UUID

    gw = _TenantGateway(tenant_provider=lambda: _Tenant(tenant_id=tid))
    assert gw._tenant_id_for_resolve() == tid
