"""AsyncAPI export — catalog/router parity, envelope shape, honest extensions.

# covers: forze_socketio.asyncapi (asyncapi_document, envelope message, ack channel,
#         command receive operations, duplicate refusal)

The parity discipline mirrors the OpenAPI routes: every catalog event must appear in
the document and every documented channel must trace back to the catalog, the built-in
ack, or a registered command — so the artifact cannot rot away from the code.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import (
    AudienceKind,
    RealtimeEvent,
    RealtimeEventCatalog,
)
from forze.application.integrations.realtime import REALTIME_PROTOCOL_VERSION
from forze.base.exceptions import CoreException
from forze_socketio import ACK_EVENT, SocketIONamespaceRouter, asyncapi_document

# ----------------------- #


class _OrderView(BaseModel):
    order_id: str
    total: int


class _NoteView(BaseModel):
    text: str


class _CreateNote(BaseModel):
    text: str


class _NoteAck(BaseModel):
    note_id: str


_CATALOG = RealtimeEventCatalog.of(
    RealtimeEvent(
        name="order.updated",
        payload_type=_OrderView,
        audience_kinds=frozenset({AudienceKind.PRINCIPAL}),
    ),
    RealtimeEvent(name="note.posted", payload_type=_NoteView, offline_delivery=False),
)


def _router() -> SocketIONamespaceRouter:
    router = SocketIONamespaceRouter(namespace="/")
    router.command(
        event="note.create", operation="note_create", payload_type=_CreateNote, ack_type=_NoteAck
    )

    return router


# ----------------------- #


class TestParity:
    def test_every_catalog_event_has_channel_message_and_send_operation(self) -> None:
        document = asyncapi_document(_CATALOG, _router())

        for event in _CATALOG:
            assert event.name in document["channels"]
            assert event.name in document["components"]["messages"]
            assert document["operations"][f"send.{event.name}"]["action"] == "send"

    def test_every_channel_traces_back_to_catalog_ack_or_command(self) -> None:
        router = _router()
        document = asyncapi_document(_CATALOG, router)

        declared = (
            {event.name for event in _CATALOG}
            | {ACK_EVENT}
            | {route.event for route in router.commands}
        )
        assert set(document["channels"]) == declared
        assert set(document["components"]["messages"]) == declared

    def test_operations_and_channels_stay_in_lockstep(self) -> None:
        document = asyncapi_document(_CATALOG, _router())

        for operation in document["operations"].values():
            ref = operation["channel"]["$ref"]
            assert ref.removeprefix("#/channels/") in document["channels"]


class TestShapes:
    def test_egress_message_wraps_the_id_data_envelope(self) -> None:
        document = asyncapi_document(_CATALOG)
        payload = document["components"]["messages"]["order.updated"]["payload"]

        assert payload["required"] == ["id", "data"]
        assert payload["properties"]["id"]["type"] == ["string", "null"]
        assert payload["additionalProperties"] is True  # clients ignore unknown fields
        assert "order_id" in payload["properties"]["data"]["properties"]

    def test_audience_and_offline_metadata_ride_extensions(self) -> None:
        document = asyncapi_document(_CATALOG)

        scoped = document["channels"]["order.updated"]
        assert scoped["x-forze-audience-kinds"] == ["principal"]
        assert scoped["x-forze-offline-delivery"] is True

        unscoped = document["channels"]["note.posted"]
        assert unscoped["x-forze-audience-kinds"] == ["principal", "topic"]
        assert unscoped["x-forze-offline-delivery"] is False

    def test_ack_is_always_a_receive_operation(self) -> None:
        document = asyncapi_document(_CATALOG)  # no router at all

        assert document["operations"][f"receive.{ACK_EVENT}"]["action"] == "receive"
        payload = document["components"]["messages"][ACK_EVENT]["payload"]
        assert payload["required"] == ["up_to"]

    def test_command_carries_raw_payload_and_ack_extension(self) -> None:
        document = asyncapi_document(_CATALOG, _router())

        payload = document["components"]["messages"]["note.create"]["payload"]
        assert "text" in payload["properties"]  # raw command payload, no envelope
        assert "id" not in payload.get("required", [])

        operation = document["operations"]["receive.note.create"]
        assert operation["action"] == "receive"
        assert "note_id" in operation["x-forze-ack-schema"]["properties"]
        assert document["channels"]["note.create"]["x-forze-operation"] == "note_create"

    def test_info_and_servers(self) -> None:
        document = asyncapi_document(
            _CATALOG,
            title="Orders realtime",
            version="2.1.0",
            description="Egress contract",
            servers={
                "socketio": {"host": "api.example.com", "protocol": "wss", "pathname": "/socket.io"},
                "sse": {"host": "api.example.com", "protocol": "https", "pathname": "/realtime/sse"},
            },
        )

        assert document["asyncapi"] == "3.0.0"
        assert document["info"]["title"] == "Orders realtime"
        assert document["info"]["x-forze-realtime-protocol"] == REALTIME_PROTOCOL_VERSION
        assert set(document["servers"]) == {"socketio", "sse"}
        assert document["defaultContentType"] == "application/json"

    def test_catalog_command_name_collision_is_refused(self) -> None:
        router = SocketIONamespaceRouter(namespace="/")
        router.command(event="order.updated", operation="clash", payload_type=_CreateNote)

        with pytest.raises(CoreException):
            asyncapi_document(_CATALOG, router)
