"""``require_tenant_global``: the startup refusal of a port no tenant-less call can use."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.tenancy import (
    TENANT_GLOBAL_REQUIRED_CODE,
    require_tenant_global,
)
from forze.application.contracts.tenancy.routed_client_base import RoutedTenantClientBase
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class _Routed(RoutedTenantClientBase[Any]):
    async def resolve_credentials(self, tenant_id: Any) -> Any:  # pragma: no cover - unused
        raise NotImplementedError

    async def initialize_client(self, tenant_id: Any, creds: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


def _routed(tenant: UUID | None = None) -> _Routed:
    return _Routed(secrets=MagicMock(), secret_ref_for_tenant={}, tenant_provider=lambda: tenant)


@attrs.define(slots=True, kw_only=True)
class _Port:
    tenant_aware: bool = False
    client: Any = None


@attrs.define(slots=True, kw_only=True)
class _DocumentAdapter:
    read_gw: Any
    write_gw: Any = None


class _Proxy:
    """Forwards every attribute, as the port proxies and the write guard do."""

    def __init__(self, target: Any) -> None:
        self._target = target

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)


def _refuse(port: object) -> CoreException:
    with pytest.raises(CoreException) as caught:
        require_tenant_global(port, what="The step")

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == TENANT_GLOBAL_REQUIRED_CODE

    return caught.value


# ....................... #


@pytest.mark.parametrize(
    "port",
    [
        pytest.param(_Port(tenant_aware=True), id="tenant-aware"),
        pytest.param(_Port(client=_routed()), id="routed-client"),
        pytest.param(_routed(), id="the-routed-client-itself"),
        pytest.param(_DocumentAdapter(read_gw=_Port(tenant_aware=True)), id="read-gateway"),
        pytest.param(
            _DocumentAdapter(read_gw=_Port(), write_gw=_Port(client=_routed())),
            id="write-gateway-routed",
        ),
        pytest.param(_Proxy(_Port(tenant_aware=True)), id="behind-a-proxy"),
    ],
)
def test_a_tenant_scoped_port_is_refused(port: object) -> None:
    error = _refuse(port)

    assert "The step runs with no tenant bound" in error.summary


@pytest.mark.parametrize(
    "port",
    [
        pytest.param(_Port(), id="plain"),
        pytest.param(_Port(client=object()), id="plain-client"),
        pytest.param(_DocumentAdapter(read_gw=_Port(), write_gw=_Port()), id="document"),
        pytest.param(MagicMock(), id="a-double-answering-every-attribute"),
        # A routed client whose own provider finds a tenant here (a deployment's default
        # tenant, say) can serve the call; only one with nothing to route by is refused.
        pytest.param(_Port(client=_routed(uuid4())), id="routed-client-with-a-tenant"),
    ],
)
def test_a_tenant_global_port_passes(port: object) -> None:
    require_tenant_global(port, what="The step")
