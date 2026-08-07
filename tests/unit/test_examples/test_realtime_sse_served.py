"""A commanded signal reaches the matching principal, and only that one.

The control plane's `emit` is the half a traffic generator cannot give you — one signal, at
one audience, on demand. What has to be true for that to be worth anything is that delivery
is *addressed*: a signal for Ada must not appear on Bob's stream, and a device that has
acked must not be shown the same frame twice.

Driven over real HTTP with two real credentials, because the routing under test is the app's
own identity plus its own egress — the served mock only carries the signal between them.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from examples.recipes.realtime_sse.served import ADA, BOB, mock_app, reset_egress
from forze_mock.server import build_mock_server

pytestmark = pytest.mark.unit

_ADA = {"X-API-Key": "ada-key"}
_BOB = {"X-API-Key": "bob-key"}
_DEVICE = "laptop"

# ....................... #


@pytest.fixture
def client() -> Iterator[TestClient]:
    # The mailbox is the *app's* state, not the mock's, so `/_mock/reset` does not touch
    # it — the app offers its own reset, and each test starts from an empty one.
    reset_egress()

    with TestClient(build_mock_server(mock_app)) as running:
        yield running


def _emit(client: TestClient, *, to: UUID, event: str = "order.shipped", **payload: Any) -> None:
    response = client.post(
        "/_mock/emit",
        json={
            "audience_kind": "principal",
            "audience_name": str(to),
            "event": event,
            "payload": payload,
        },
    )

    assert response.status_code == 202, response.text


def _frames(
    client: TestClient,
    headers: dict[str, str],
    *,
    last_event_id: str | None = None,
) -> list[dict[str, Any]]:
    """Connect as the browser's EventSource would and drain the replay."""

    request_headers = dict(headers)

    if last_event_id is not None:
        request_headers["Last-Event-ID"] = last_event_id

    response = client.get("/realtime/sse", params={"device_id": _DEVICE}, headers=request_headers)
    response.raise_for_status()

    frames: list[dict[str, Any]] = []

    for block in response.text.split("\n\n"):
        if not block.strip() or block.startswith(":"):
            continue

        frame: dict[str, Any] = {}

        for line in block.splitlines():
            field, _, value = line.partition(": ")
            frame[field] = json.loads(value) if field == "data" else value

        frames.append(frame)

    return frames


def _ack(client: TestClient, headers: dict[str, str], *, up_to: str) -> None:
    response = client.post(
        "/realtime/sse/ack",
        json={"up_to": up_to},
        params={"device_id": _DEVICE},
        headers=headers,
    )
    response.raise_for_status()


# ....................... #


class TestCommandedDelivery:
    def test_a_signal_reaches_the_principal_it_was_addressed_to(self, client: TestClient) -> None:
        _emit(client, to=ADA, text="your order shipped")

        frames = _frames(client, _ADA)

        assert [frame["event"] for frame in frames] == ["order.shipped"]
        assert frames[0]["data"]["data"] == {"text": "your order shipped"}

    def test_and_reaches_nobody_else(self, client: TestClient) -> None:
        # The assertion that makes addressed delivery mean something: a mailbox that
        # fanned out to every connected client would pass the test above and still be wrong.
        _emit(client, to=ADA, text="for ada only")

        assert _frames(client, _BOB) == []

    def test_each_principal_sees_only_their_own(self, client: TestClient) -> None:
        _emit(client, to=ADA, text="ada")
        _emit(client, to=BOB, text="bob")

        assert [f["data"]["data"]["text"] for f in _frames(client, _ADA)] == ["ada"]
        assert [f["data"]["data"]["text"] for f in _frames(client, _BOB)] == ["bob"]

    def test_an_unauthenticated_stream_is_refused(self, client: TestClient) -> None:
        _emit(client, to=ADA, text="private")

        assert client.get("/realtime/sse", params={"device_id": _DEVICE}).status_code == 401

    def test_emitting_needs_no_credential_because_the_control_plane_is_the_tool(
        self, client: TestClient
    ) -> None:
        # `/_mock/emit` sits beside the app, outside its middleware — that is deliberate,
        # and it is why the server must never be reachable from anywhere but a laptop.
        response = client.post(
            "/_mock/emit",
            json={
                "audience_kind": "principal",
                "audience_name": str(ADA),
                "event": "ping",
                "payload": {},
            },
        )

        assert response.status_code == 202


class TestReconnectReplaysFromTheCursor:
    def test_an_acked_frame_is_not_replayed_on_reconnect(self, client: TestClient) -> None:
        _emit(client, to=ADA, text="first")
        _emit(client, to=ADA, text="second")

        first_pass = _frames(client, _ADA)
        assert [f["data"]["data"]["text"] for f in first_pass] == ["first", "second"]

        _ack(client, _ADA, up_to=first_pass[-1]["id"])

        # Reconnecting after the ack replays nothing: the cursor, not the connection, is
        # what decides — which is the property an at-least-once transport lives or dies on.
        assert _frames(client, _ADA) == []

    def test_a_partial_ack_replays_only_what_follows_it(self, client: TestClient) -> None:
        _emit(client, to=ADA, text="first")
        _emit(client, to=ADA, text="second")
        _emit(client, to=ADA, text="third")

        frames = _frames(client, _ADA)
        _ack(client, _ADA, up_to=frames[0]["id"])

        assert [f["data"]["data"]["text"] for f in _frames(client, _ADA)] == ["second", "third"]

    def test_last_event_id_resumes_the_browser_itself(self, client: TestClient) -> None:
        # EventSource sends `Last-Event-ID` on reconnect and it takes precedence over the
        # stored cursor, so a tab that never acked still resumes where it stopped reading.
        _emit(client, to=ADA, text="first")
        _emit(client, to=ADA, text="second")

        frames = _frames(client, _ADA)
        resumed = _frames(client, _ADA, last_event_id=frames[0]["id"])

        assert [f["data"]["data"]["text"] for f in resumed] == ["second"]

    def test_one_principals_ack_does_not_move_anothers_cursor(self, client: TestClient) -> None:
        _emit(client, to=ADA, text="ada")
        _emit(client, to=BOB, text="bob")

        ada_frames = _frames(client, _ADA)
        _ack(client, _ADA, up_to=ada_frames[-1]["id"])

        assert _frames(client, _ADA) == []
        assert [f["data"]["data"]["text"] for f in _frames(client, _BOB)] == ["bob"]
