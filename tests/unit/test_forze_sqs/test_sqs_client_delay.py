"""Unit tests for SQS delayed enqueue."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from forze.application.contracts.queue import SQS_MAX_DELAY
from forze.base.exceptions import CoreException
from forze_sqs.kernel.client import SQSClient


def test_resolve_sqs_delay_seconds_none_for_immediate() -> None:
    assert SQSClient._resolve_sqs_delay_seconds(delay=None, not_before=None) is None


def test_resolve_sqs_delay_seconds_maps_relative_delay() -> None:
    assert (
        SQSClient._resolve_sqs_delay_seconds(delay=timedelta(seconds=45), not_before=None)
        == 45
    )


def test_resolve_sqs_delay_seconds_rejects_over_max() -> None:
    with pytest.raises(CoreException):
        SQSClient._resolve_sqs_delay_seconds(
            delay=SQS_MAX_DELAY + timedelta(seconds=1),
            not_before=None,
        )


@pytest.mark.asyncio
async def test_enqueue_many_sets_delay_seconds_on_batch_entry() -> None:
    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.send_message_batch = AsyncMock(return_value={"Failed": []})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/queue"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        with patch.object(client, "_SQSClient__is_fifo_target", return_value=False):
            await client.enqueue_many(
                "jobs",
                [b"body"],
                delay=timedelta(seconds=12),
            )

    entries = mock_boto.send_message_batch.await_args.kwargs["Entries"]
    assert entries[0]["DelaySeconds"] == 12


@pytest.mark.asyncio
async def test_enqueue_many_fifo_rejects_per_message_delay() -> None:
    # SQS rejects per-message DelaySeconds on FIFO queues; fail closed before the AWS call.
    client = SQSClient()
    mock_boto = AsyncMock()

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/q.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        with patch.object(client, "_SQSClient__is_fifo_target", return_value=True):
            with pytest.raises(CoreException) as ei:
                await client.enqueue_many(
                    "jobs.fifo", [b"body"], delay=timedelta(seconds=12), key="g1"
                )

    assert ei.value.code == "sqs.fifo_per_message_delay"
    mock_boto.send_message_batch.assert_not_called()  # never reached the AWS call
