"""Unit tests for :mod:`forze_socketio.realtime`."""

from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimeDepKey, RealtimePort
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_socketio.realtime import SocketIORealtimeAdapter, socketio_realtime_deps

# ----------------------- #

_TENANT = TenantIdentity(tenant_id=UUID("11111111-1111-1111-1111-111111111111"))


class StubSocketIOServer:
    """Minimal Socket.IO server stub recording emit calls."""

    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []

    async def emit(
        self,
        event: str,
        data: Any = None,
        *,
        namespace: str | None = None,
        to: str | None = None,
        room: str | None = None,
        skip_sid: str | list[str] | None = None,
    ) -> None:
        self.emits.append({"event": event, "data": data, "namespace": namespace, "room": room})


class _Msg(BaseModel):
    text: str


# ----------------------- #


def _adapter(
    sio: StubSocketIOServer,
    *,
    namespace: str = "/",
    tenant_aware: bool = False,
    tenant: TenantIdentity | None = None,
) -> SocketIORealtimeAdapter:
    return SocketIORealtimeAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        namespace=namespace,
        tenant_aware=tenant_aware,
        tenant_provider=lambda: tenant,
    )


# ....................... #


async def test_emit_untenanted_uses_logical_room() -> None:
    sio = StubSocketIOServer()

    await _adapter(sio).emit(Audience.topic("chat"), "message.new", _Msg(text="hi"))

    call = sio.emits[0]
    assert call["event"] == "message.new"
    assert call["room"] == "topic:chat"  # no tenant bound → unscoped
    assert call["data"] == {"text": "hi"}
    assert call["namespace"] == "/"


# ....................... #


async def test_emit_scopes_room_by_bound_tenant() -> None:
    sio = StubSocketIOServer()

    await _adapter(sio, tenant=_TENANT).emit(Audience.topic("chat"), "evt", _Msg(text="x"))

    # caller said topic("chat"); the adapter applies the ambient tenant
    assert sio.emits[0]["room"] == f"t:{_TENANT.tenant_id}:topic:chat"


# ....................... #


async def test_two_tenants_never_share_a_room() -> None:
    other = TenantIdentity(tenant_id=UUID("22222222-2222-2222-2222-222222222222"))
    sio_a, sio_b = StubSocketIOServer(), StubSocketIOServer()

    await _adapter(sio_a, tenant=_TENANT).emit(Audience.topic("room"), "e", _Msg(text="a"))
    await _adapter(sio_b, tenant=other).emit(Audience.topic("room"), "e", _Msg(text="b"))

    assert sio_a.emits[0]["room"] != sio_b.emits[0]["room"]


# ....................... #


async def test_principal_room_is_scoped() -> None:
    sio = StubSocketIOServer()

    await _adapter(sio, tenant=_TENANT).emit(Audience.principal("u-1"), "e", _Msg(text="x"))

    assert sio.emits[0]["room"] == f"t:{_TENANT.tenant_id}:principal:u-1"


# ....................... #


async def test_tenant_broadcast_room() -> None:
    sio = StubSocketIOServer()

    await _adapter(sio, tenant=_TENANT).emit(Audience.tenant(), "announce", _Msg(text="x"))

    assert sio.emits[0]["room"] == f"t:{_TENANT.tenant_id}"


# ....................... #


async def test_tenant_aware_fails_closed_without_tenant() -> None:
    sio = StubSocketIOServer()
    adapter = _adapter(sio, tenant_aware=True, tenant=None)

    with pytest.raises(CoreException) as err:
        await adapter.emit(Audience.topic("chat"), "e", _Msg(text="x"))

    assert err.value.code == "tenant_required"
    assert not sio.emits  # nothing leaked


# ....................... #


async def test_emit_uses_adapter_namespace() -> None:
    sio = StubSocketIOServer()

    await _adapter(sio, namespace="/chat").emit(Audience.tenant(), "evt", _Msg(text="x"))

    assert sio.emits[0]["namespace"] == "/chat"


# ....................... #


def test_adapter_is_a_realtime_port() -> None:
    assert isinstance(_adapter(StubSocketIOServer()), RealtimePort)


# ....................... #


def test_socketio_realtime_deps_registers_adapter() -> None:
    sio = StubSocketIOServer()

    frag = socketio_realtime_deps(sio, namespace="/chat", tenant_aware=True)  # pyright: ignore[reportArgumentType]
    provider = frag.store.plain_deps[RealtimeDepKey]

    class _Ctx:
        class inv_ctx:
            @staticmethod
            def get_tenant() -> TenantIdentity | None:
                return _TENANT

    adapter = provider(_Ctx)

    assert isinstance(adapter, SocketIORealtimeAdapter)
    assert adapter.sio is sio
    assert adapter.namespace == "/chat"
    assert adapter.tenant_aware is True
