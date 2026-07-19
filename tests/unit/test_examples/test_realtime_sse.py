"""The realtime_sse recipe — replay, cumulative ack, and Last-Event-ID resume over SSE."""

from __future__ import annotations

from starlette.testclient import TestClient

from examples.recipes.realtime_sse.app import ack, build_app, connect, seed_mailbox

# ----------------------- #


async def test_replay_ack_and_last_event_id_resume() -> None:
    app, mailbox = build_app()
    client = TestClient(app)
    ids = await seed_mailbox(mailbox, ["packed", "shipped", "delivered"])

    # reconnect drains the whole backlog as {id, data} envelope frames
    frames = connect(client)
    assert [f["id"] for f in frames] == ids
    assert [f["data"]["data"]["text"] for f in frames] == ["packed", "shipped", "delivered"]
    assert frames[0]["event"] == "order.shipped"

    # the browser's native resume beats the stored cursor (before any ack trims)
    resumed = connect(client, last_event_id=ids[0])
    assert [f["data"]["data"]["text"] for f in resumed] == ["shipped", "delivered"]

    # the cumulative ack advances the cursor: only the unacked tail re-replays
    assert ack(client, up_to=ids[1]) is True
    assert [f["data"]["data"]["text"] for f in connect(client)] == ["delivered"]

    # an id no longer known cannot be acked
    assert ack(client, up_to="unknown") is False
