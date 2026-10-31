"""How the SSE routes read in the app's OpenAPI document.

# covers: forze_fastapi.realtime.sse (attach_realtime_sse_route route_options /
#         ack_route_options, RealtimeSseRouteOptions, parameter descriptions,
#         the text/event-stream response)

The routes are attached to a router the app includes under a prefix, the way an app mounts
them: FastAPI derives an included route's parameters from the endpoint again, so a
description patched onto the attached route afterwards would not survive.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import APIRouter, FastAPI

from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_fastapi.realtime import (
    RealtimeSseHub,
    RealtimeSseRouteOptions,
    attach_realtime_sse_route,
)

# ----------------------- #


def _paths(
    route_options: RealtimeSseRouteOptions | None = None,
    ack_route_options: RealtimeSseRouteOptions | None = None,
    *,
    stores: bool = True,
    hub: bool = False,
) -> dict[str, Any]:
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]  # never called
        mailbox_factory=(lambda _ctx: InMemoryRealtimeMailbox()) if stores else None,
        cursors_factory=(lambda _ctx: InMemoryMailboxCursors()) if stores else None,
        hub=RealtimeSseHub() if hub else None,
        route_options=route_options,
        ack_route_options=ack_route_options,
    )
    app = FastAPI()
    app.include_router(router, prefix="/v1")

    return app.openapi()["paths"]


def _stream(paths: dict[str, Any]) -> dict[str, Any]:
    return paths["/v1/realtime/sse"]["get"]


def _ack(paths: dict[str, Any]) -> dict[str, Any]:
    return paths["/v1/realtime/sse/ack"]["post"]


def _descriptions(operation: dict[str, Any]) -> dict[str, str]:
    return {param["name"]: param.get("description", "") for param in operation["parameters"]}


# ....................... #


class TestBuiltInDocumentation:
    def test_every_query_parameter_is_described(self) -> None:
        paths = _paths()

        for operation, names in (
            (_stream(paths), {"protocol", "device_id", "topics"}),
            (_ack(paths), {"device_id"}),
        ):
            described = _descriptions(operation)

            assert set(described) == names
            assert all(text.strip() for text in described.values())

    def test_the_ack_documents_device_id_as_required(self) -> None:
        # The endpoint refuses an ack without one (realtime_ack_requires_device).
        [device_id] = _ack(_paths())["parameters"]

        assert device_id["name"] == "device_id"
        assert device_id["in"] == "query"
        assert device_id["required"] is True
        assert device_id["schema"] == {"type": "string"}

    def test_the_stream_is_documented_as_an_event_stream(self) -> None:
        ok = _stream(_paths())["responses"]["200"]

        assert set(ok["content"]) == {"text/event-stream"}
        assert ok["description"].startswith("An event stream")

    def test_the_summaries_stay_when_no_options_are_given(self) -> None:
        paths = _paths()

        assert _stream(paths)["summary"].startswith("Realtime egress over Server-Sent Events")
        assert _ack(paths)["summary"].startswith("Cumulative realtime ack")

    @pytest.mark.parametrize(
        ("stores", "hub", "mode"),
        [
            (True, True, "(replay, then live)"),
            (True, False, "(replay only)"),
            (False, True, "(live only)"),
        ],
    )
    def test_the_summary_names_the_delivery_mode(self, stores: bool, hub: bool, mode: str) -> None:
        summary = _stream(_paths(stores=stores, hub=hub))["summary"]

        assert summary == f"Realtime egress over Server-Sent Events {mode}"

    def test_device_id_is_described_by_the_delivery_mode(self) -> None:
        durable = _descriptions(_stream(_paths()))["device_id"]
        live = _descriptions(_stream(_paths(stores=False, hub=True)))["device_id"]

        assert "replay cursor" in durable
        assert "Last-Event-ID" in durable
        assert live.startswith("Unused: a live-only stream keeps no replay cursor")
        assert "Last-Event-ID" not in live


class TestRouteOptions:
    def test_the_callers_documentation_reaches_each_operation(self) -> None:
        paths = _paths(
            route_options={
                "operation_id": "realtime.events",
                "description": "The app's event stream.",
                "tags": ["realtime"],
                "openapi_extra": {"x-audience": "browser"},
            },
            ack_route_options={
                "operation_id": "realtime.ack",
                "summary": "Acknowledge events",
                "tags": ["realtime"],
                "deprecated": True,
            },
        )
        stream, ack = _stream(paths), _ack(paths)

        assert stream["operationId"] == "realtime.events"
        assert stream["description"] == "The app's event stream."
        assert stream["tags"] == ["realtime"]
        assert stream["x-audience"] == "browser"
        assert ack["operationId"] == "realtime.ack"
        assert ack["summary"] == "Acknowledge events"
        assert ack["tags"] == ["realtime"]
        assert ack["deprecated"] is True

    def test_the_callers_responses_merge_over_the_built_in_ones(self) -> None:
        responses = _stream(
            _paths(route_options={"responses": {401: {"description": "Not signed in"}}})
        )["responses"]

        assert responses["401"]["description"] == "Not signed in"
        assert set(responses["200"]["content"]) == {"text/event-stream"}

    def test_the_callers_ack_parameters_join_the_documented_device_id(self) -> None:
        trace = {"name": "X-Trace", "in": "header", "schema": {"type": "string"}}
        ack = _ack(
            _paths(
                ack_route_options={
                    "openapi_extra": {"parameters": [trace], "x-audience": "browser"},
                }
            )
        )

        assert [param["name"] for param in ack["parameters"]] == ["device_id", "X-Trace"]
        assert ack["x-audience"] == "browser"

    @pytest.mark.parametrize("key", ["path", "methods", "endpoint", "response_class", "name"])
    def test_a_key_forze_controls_is_refused_at_attach(self, key: str) -> None:
        with pytest.raises(CoreException, match="route_options sets") as caught:
            _paths(route_options={key: "x"})  # type: ignore[misc]

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    def test_options_that_are_not_a_mapping_are_refused_at_attach(self) -> None:
        with pytest.raises(CoreException, match="route_options must be a mapping") as caught:
            _paths(route_options=[("operation_id", "realtime.events")])  # type: ignore[arg-type]

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    def test_an_unknown_ack_key_is_refused_at_attach(self) -> None:
        with pytest.raises(CoreException, match=r"ack_route_options sets.*'operationId'"):
            _paths(ack_route_options={"operationId": "realtime.ack"})  # type: ignore[typeddict-unknown-key]
