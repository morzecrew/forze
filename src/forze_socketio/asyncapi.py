"""AsyncAPI 3 export — the egress twin of the OpenAPI story, generated, never hand-written.

The typed truth already exists server-side: the :class:`RealtimeEventCatalog` declares
every egress event (name, payload model, audience kinds, offline delivery) and the
:class:`SocketIONamespaceRouter` declares every inbound command. This module projects
both into one AsyncAPI document a client team can review, diff, and generate types
from — the same doctrine as the FastAPI route generators: catalog-driven, attach-only,
and parity-tested so the artifact cannot rot.

Perspective is the **application's**: egress events are ``send`` operations (the
server sends), inbound commands are ``receive`` operations. Every egress message is
wrapped in the shared ``{id, data}`` delivery envelope (versioned per the realtime
wire protocol; see ``x-forze-realtime-protocol`` in ``info``); command payloads are
raw, exactly as the router validates them. Socket.IO acknowledgements are RPC-style
callbacks with no channel of their own, so a typed command ack is recorded as an
``x-forze-ack-schema`` extension on its receive operation rather than a fake reply
channel. Serve the document or dump it to disk; TS codegen is standard AsyncAPI
tooling applied to the output.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from collections.abc import Mapping
from typing import Any

from pydantic import TypeAdapter

from forze.application.contracts.realtime import (
    AudienceKind,
    RealtimeEvent,
    RealtimeEventCatalog,
)
from forze.application.integrations.realtime import REALTIME_PROTOCOL_VERSION
from forze.base.exceptions import exc

from .routing import SocketIONamespaceRouter

# ----------------------- #

__all__ = [
    "ACK_EVENT",
    "asyncapi_document",
]

ACK_EVENT = "realtime.ack"
"""The cumulative-ack event every replay-enabled connection layer registers."""


def _payload_schema(payload_type: Any) -> dict[str, Any]:
    """The JSON Schema of a payload model, self-contained (``$defs`` stay inline)."""

    return TypeAdapter[Any](payload_type).json_schema()


def _envelope_message(event: RealtimeEvent[Any]) -> dict[str, Any]:
    """An egress message: the shared ``{id, data}`` delivery envelope around the model."""

    kinds = event.audience_kinds if event.audience_kinds is not None else frozenset(AudienceKind)

    return {
        "name": event.name,
        "title": event.name,
        "contentType": "application/json",
        "payload": {
            "type": "object",
            "required": ["id", "data"],
            # forward-compat rule: unknown envelope fields MUST be ignored by clients
            "additionalProperties": True,
            "properties": {
                "id": {
                    "type": ["string", "null"],
                    "description": (
                        "Durable event id (dedup + ack anchor); null for ephemeral signals."
                    ),
                },
                "data": _payload_schema(event.payload_type),
            },
        },
        "x-forze-audience-kinds": sorted(kind.value for kind in kinds),
        "x-forze-offline-delivery": event.offline_delivery,
    }


# ....................... #


def asyncapi_document(
    catalog: RealtimeEventCatalog,
    router: SocketIONamespaceRouter | None = None,
    *,
    title: str = "Realtime API",
    version: str = "0.1.0",
    description: str | None = None,
    servers: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Project the catalog (and optionally a command router) into an AsyncAPI 3 document.

    :param catalog: The frozen egress event catalog — one channel + ``send`` operation
        per event, each message wrapped in the versioned ``{id, data}`` envelope.
    :param router: Optional inbound command router — one channel + ``receive`` operation
        per registered command (raw payload schema; typed acks as ``x-forze-ack-schema``).
        The built-in ``realtime.ack`` receive operation is always included.
    :param servers: AsyncAPI server entries, caller-supplied — e.g. the Socket.IO
        namespace endpoint and, when the SSE route is attached, the ``text/event-stream``
        endpoint (same envelope, second transport).
    """

    channels: dict[str, Any] = {}
    operations: dict[str, Any] = {}
    messages: dict[str, Any] = {}

    def _add_channel(event_name: str, message: dict[str, Any], *, extras: dict[str, Any]) -> None:
        if event_name in channels:
            raise exc.configuration(
                f"Realtime event {event_name!r} is declared twice (catalog/command overlap) — "
                "one wire event must have exactly one contract"
            )

        messages[event_name] = message
        channels[event_name] = {
            "address": event_name,
            "messages": {event_name: {"$ref": f"#/components/messages/{event_name}"}},
            **extras,
        }

    # egress: one channel + send operation per declared catalog event
    for event in catalog:
        message = _envelope_message(event)
        _add_channel(
            event.name,
            message,
            extras={
                "x-forze-audience-kinds": message["x-forze-audience-kinds"],
                "x-forze-offline-delivery": message["x-forze-offline-delivery"],
            },
        )
        operations[f"send.{event.name}"] = {
            "action": "send",
            "channel": {"$ref": f"#/channels/{event.name}"},
            "summary": f"Deliver {event.name!r} to its audience",
        }

    # ingress: the built-in cumulative ack, plus every registered command
    _add_channel(
        ACK_EVENT,
        {
            "name": ACK_EVENT,
            "title": ACK_EVENT,
            "contentType": "application/json",
            "payload": {
                "type": "object",
                "required": ["up_to"],
                "properties": {
                    "up_to": {
                        "type": "string",
                        "description": "Cumulative: the last delivered durable event id.",
                    }
                },
            },
        },
        extras={},
    )
    operations[f"receive.{ACK_EVENT}"] = {
        "action": "receive",
        "channel": {"$ref": f"#/channels/{ACK_EVENT}"},
        "summary": "Advance this device's replay cursor (cumulative ack)",
    }

    for route in router.commands if router is not None else ():
        _add_channel(
            route.event,
            {
                "name": route.event,
                "title": route.event,
                "contentType": "application/json",
                "payload": _payload_schema(route.payload_type),
            },
            extras={"x-forze-operation": str(route.operation)},
        )
        operation: dict[str, Any] = {
            "action": "receive",
            "channel": {"$ref": f"#/channels/{route.event}"},
            "summary": f"Run operation {route.operation!s}",
        }

        if route.ack_type is not None:
            # Socket.IO acks are RPC-style callbacks without a channel of their own —
            # an honest extension beats a fabricated reply channel.
            operation["x-forze-ack-schema"] = _payload_schema(route.ack_type)

        operations[f"receive.{route.event}"] = operation

    document: dict[str, Any] = {
        "asyncapi": "3.0.0",
        "info": {
            "title": title,
            "version": version,
            "x-forze-realtime-protocol": REALTIME_PROTOCOL_VERSION,
        },
        "defaultContentType": "application/json",
        "channels": channels,
        "operations": operations,
        "components": {"messages": messages},
    }

    if description is not None:
        document["info"]["description"] = description

    if servers:
        document["servers"] = {name: dict(entry) for name, entry in servers.items()}

    return document
