"""Protocol negotiation at connect — the versioned handshake (W1 of the wire contract).

# covers: forze_socketio.connection (_ConnectionLifecycle.on_connect protocol handshake)

The connection speaks exactly one protocol version for its lifetime: negotiated from the
``auth`` payload's ``protocol`` field before any auth work, missing means 1 (pre-versioning
clients), and an unsupported version is refused with a client-safe message naming the
supported range — never silently downgraded.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze_socketio import RealtimeConnection, attach_realtime_connection
from forze_socketio.routing import SocketIOConnect

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")


class _StubSio:
    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.entered: list[tuple[str, str]] = []
        self.sessions: dict[str, dict[str, Any]] = {}

    def on(self, event: str, handler: Any, namespace: str | None = None) -> None:
        self.handlers[event] = handler

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.entered.append((sid, room))

    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, Any]:
        return self.sessions.setdefault(sid, {})

    async def save_session(
        self, sid: str, session: dict[str, Any], namespace: str | None = None
    ) -> None:
        self.sessions[sid] = session


def _attach(sio: _StubSio) -> None:
    async def resolve(_c: SocketIOConnect) -> RealtimeConnection:
        return RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))

    attach_realtime_connection(sio, resolve=resolve)  # pyright: ignore[reportArgumentType]


# ----------------------- #


@pytest.mark.parametrize("auth", [None, {"token": "t"}, {"protocol": 1}, {"protocol": "1"}])
async def test_missing_or_current_protocol_connects(auth: Any) -> None:
    sio = _StubSio()
    _attach(sio)

    await sio.handlers["connect"]("sid-1", {}, auth)

    assert sio.entered  # the principal room was joined — the connection was admitted


@pytest.mark.parametrize("bad", [2, "2", "x", True])
async def test_unsupported_protocol_is_refused_before_auth(bad: Any) -> None:
    sio = _StubSio()
    _attach(sio)

    with pytest.raises(SocketIOConnectionRefusedError) as caught:
        await sio.handlers["connect"]("sid-1", {}, {"token": "t", "protocol": bad})

    # client-safe refusal naming the supported range; nothing was joined or stored
    assert "Unsupported realtime protocol" in str(caught.value)
    assert not sio.entered
    assert not sio.sessions
