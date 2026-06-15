"""FIFO entry fields: MessageGroupId = key (ordering), dedup id = event id.

With the outbox relay publishing ``key=ordering_key``, the FIFO dedup id must
NOT follow ``key`` — different events of one aggregate share the key and
key-based dedup would drop them within the five-minute window. The dedup id
priority is: explicit ``message_id(s)`` → ``forze_event_id`` header
(single-message sends) → fresh random id per message.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("aioboto3")

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze_sqs.kernel.client import SQSClient

# ----------------------- #


async def _enqueue_entries(client: SQSClient, **kwargs: Any) -> list[dict[str, Any]]:
    """Run ``enqueue_many`` against a stubbed boto client; return batch entries."""

    mock_boto = AsyncMock()
    mock_boto.send_message_batch = AsyncMock(return_value={"Failed": []})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/queue.fifo"),
    ):
        with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
            with patch.object(client, "_SQSClient__is_fifo_target", return_value=True):
                await client.enqueue_many("orders.fifo", **kwargs)

    entries: list[dict[str, Any]] = []

    for call in mock_boto.send_message_batch.await_args_list:
        entries.extend(call.kwargs["Entries"])

    return entries


# ----------------------- #


@pytest.mark.asyncio
async def test_fifo_group_is_ordering_key_and_dedup_is_event_id_header() -> None:
    """The relay path: key=ordering key, forze_event_id header = event id."""

    event_id = "0190b2f4-0000-7000-8000-000000000001"
    entries = await _enqueue_entries(
        SQSClient(),
        bodies=[b"body"],
        type="order.shipped",
        key="order-42",
        headers={HEADER_EVENT_ID: event_id, "trace": "t-1"},
    )

    [entry] = entries
    # Ordering win: the aggregate key is the FIFO message group.
    assert entry["MessageGroupId"] == "order-42"
    # Dedup safety: the dedup id is the stable EVENT id, never the key —
    # a second event of order-42 must not be swallowed by FIFO dedup.
    assert entry["MessageDeduplicationId"] == event_id


@pytest.mark.asyncio
async def test_fifo_dedup_without_header_is_fresh_per_publish_not_key() -> None:
    """No envelope header: dedup ids stay random — same key never collapses."""

    client = SQSClient()
    first = await _enqueue_entries(client, bodies=[b"a"], key="order-42")
    second = await _enqueue_entries(client, bodies=[b"b"], key="order-42")

    dedup_first = first[0]["MessageDeduplicationId"]
    dedup_second = second[0]["MessageDeduplicationId"]

    assert first[0]["MessageGroupId"] == "order-42"
    assert dedup_first != "order-42"
    assert dedup_second != "order-42"
    assert dedup_first != dedup_second


@pytest.mark.asyncio
async def test_fifo_explicit_message_id_beats_event_id_header() -> None:
    entries = await _enqueue_entries(
        SQSClient(),
        bodies=[b"body"],
        key="order-42",
        message_ids=["caller-dedup-1"],
        headers={HEADER_EVENT_ID: "0190b2f4-0000-7000-8000-000000000002"},
    )

    assert entries[0]["MessageDeduplicationId"] == "caller-dedup-1"


@pytest.mark.asyncio
async def test_fifo_batch_with_shared_header_keeps_per_message_dedup_ids() -> None:
    """Headers are batch-wide; a shared event id must not collapse a batch."""

    entries = await _enqueue_entries(
        SQSClient(),
        bodies=[b"a", b"b", b"c"],
        key="order-42",
        headers={HEADER_EVENT_ID: "0190b2f4-0000-7000-8000-000000000003"},
    )

    dedup_ids = [entry["MessageDeduplicationId"] for entry in entries]
    assert len(set(dedup_ids)) == 3
    assert "0190b2f4-0000-7000-8000-000000000003" not in dedup_ids


@pytest.mark.asyncio
async def test_fifo_default_group_when_no_key() -> None:
    entries = await _enqueue_entries(SQSClient(), bodies=[b"body"])

    assert entries[0]["MessageGroupId"] == "forze"
    assert entries[0]["MessageDeduplicationId"]


@pytest.mark.asyncio
async def test_message_headers_give_each_entry_distinct_event_id_and_dedup() -> None:
    """Per-message headers: one batch send, distinct event ids + dedup ids."""

    mock_boto = AsyncMock()
    mock_boto.send_message_batch = AsyncMock(return_value={"Failed": []})

    client = SQSClient()

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/queue.fifo"),
    ):
        with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
            with patch.object(client, "_SQSClient__is_fifo_target", return_value=True):
                await client.enqueue_many(
                    "orders.fifo",
                    [b"a", b"b"],
                    key="order-42",
                    headers={"trace": "t-shared"},
                    message_headers=[
                        {HEADER_EVENT_ID: "e1"},
                        {HEADER_EVENT_ID: "e2"},
                    ],
                )

    # A single batched SendMessageBatch round-trip preserved.
    assert mock_boto.send_message_batch.await_count == 1

    [call] = mock_boto.send_message_batch.await_args_list
    entries = call.kwargs["Entries"]
    assert len(entries) == 2

    # Each entry carries its own forze_event_id attribute (per-message),
    # plus the shared header rides every entry.
    event_ids = [
        entry["MessageAttributes"][HEADER_EVENT_ID]["StringValue"] for entry in entries
    ]
    assert event_ids == ["e1", "e2"]
    for entry in entries:
        assert entry["MessageAttributes"]["trace"]["StringValue"] == "t-shared"

    # Per-message dedup: the FIFO dedup id derives from each entry's own event id.
    assert entries[0]["MessageDeduplicationId"] == "e1"
    assert entries[1]["MessageDeduplicationId"] == "e2"
