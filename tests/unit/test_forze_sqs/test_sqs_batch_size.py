"""Batch chunking respects BOTH the 10-entry count limit and the 256 KiB size limit."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("aioboto3")

from forze.base.exceptions import CoreException, ExceptionKind
from forze_sqs.kernel.client import SQSClient

# ----------------------- #


async def _enqueue(client: SQSClient, **kwargs: Any) -> AsyncMock:
    """Run ``enqueue_many`` against a stubbed boto client; return the mock to inspect."""

    mock_boto = AsyncMock()
    mock_boto.send_message_batch = AsyncMock(return_value={"Failed": []})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/queue"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto), patch.object(
        client, "_SQSClient__is_fifo_target", return_value=False
    ):
        await client.enqueue_many("orders", **kwargs)

    return mock_boto


# ....................... #


@pytest.mark.asyncio
async def test_large_messages_split_by_size_not_only_count() -> None:
    # Four ~100 KiB bodies fit the 10-entry count limit in one batch, but each base64-inflates
    # to ~133 KiB, so two would already blow the 256 KiB request limit — one per batch.
    big = b"x" * 100_000
    mock = await _enqueue(SQSClient(), bodies=[big, big, big, big])

    assert mock.send_message_batch.await_count == 4
    for call in mock.send_message_batch.await_args_list:
        assert len(call.kwargs["Entries"]) == 1


@pytest.mark.asyncio
async def test_small_messages_still_pack_ten_per_batch() -> None:
    # Tiny bodies are size-irrelevant, so the 10-entry count limit governs: 15 → 10 + 5.
    mock = await _enqueue(SQSClient(), bodies=[b"x"] * 15)

    counts = [len(c.kwargs["Entries"]) for c in mock.send_message_batch.await_args_list]
    assert counts == [10, 5]


@pytest.mark.asyncio
async def test_single_oversized_message_fails_closed() -> None:
    # 200 KiB raw → ~266 KiB base64, past the per-message/batch limit: a clear error
    # instead of an opaque broker rejection.
    with pytest.raises(CoreException) as ei:
        await _enqueue(SQSClient(), bodies=[b"x" * 200_000])

    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.asyncio
async def test_higher_per_queue_limit_packs_more_per_batch() -> None:
    # The same four ~133 KiB-encoded messages fit one batch under a 1 MiB queue limit
    # (AWS's 2025 ceiling) instead of splitting at 256 KiB.
    big = b"x" * 100_000
    mock = await _enqueue(
        SQSClient(),
        bodies=[big, big, big, big],
        max_batch_payload_bytes=1024 * 1024,
    )

    assert mock.send_message_batch.await_count == 1
    assert len(mock.send_message_batch.await_args_list[0].kwargs["Entries"]) == 4


def test_config_rejects_out_of_range_limit() -> None:
    from forze_sqs.execution.deps.configs import SQSQueueConfig

    assert SQSQueueConfig().max_batch_payload_bytes == 256 * 1024  # safe default

    with pytest.raises(CoreException):
        SQSQueueConfig(max_batch_payload_bytes=512)  # below 1 KiB

    with pytest.raises(CoreException):
        SQSQueueConfig(max_batch_payload_bytes=2 * 1024 * 1024)  # above 1 MiB
