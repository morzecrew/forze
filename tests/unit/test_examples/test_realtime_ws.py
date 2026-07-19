"""The realtime_ws recipe — replay, inline ack, and governed commands on one socket."""

from __future__ import annotations

from examples.recipes.realtime_ws.app import build_app, run_session, seed_mailbox

# ----------------------- #


async def test_replay_ack_and_commands_over_one_socket() -> None:
    app, mailbox = build_app()
    ids = await seed_mailbox(mailbox, ["packed", "shipped"])

    out = run_session(app, replay_count=2)

    # the replay carries the shared envelope with the event name in-band
    assert [f["id"] for f in out["replayed"]] == ids
    assert out["replayed"][0]["event"] == "order.shipped"
    assert out["replayed"][0]["data"] == {"text": "packed"}

    # a command dispatches through the frozen registry and acks with the typed result
    assert out["command_ack"] == {
        "type": "ack",
        "cid": "c1",
        "data": {"note_id": "note:ship it"},
    }

    # a failing command is the shared error envelope, not a dropped socket
    assert out["error_ack"]["cid"] == "c2"
    assert out["error_ack"]["error"]["code"] == "note_blank"
    assert out["error_ack"]["error"]["kind"] == "validation"

    # the inline ack advanced the cursor: a second session replays nothing
    assert run_session(app, replay_count=0)["replayed"] == []
