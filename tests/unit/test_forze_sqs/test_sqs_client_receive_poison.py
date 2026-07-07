"""SQSClient.receive() isolates a per-message decode failure: one non-base64 body must not
poison the whole batch — the good messages are still returned and registered for ack/nack, and
the poison message is left in-flight (SQS redelivery + redrive → DLQ handles it)."""

import base64
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from forze_sqs.kernel.client import SQSClient


def _b64_msg(mid: str, body: str) -> dict[str, Any]:
    return {
        "MessageId": mid,
        "ReceiptHandle": f"r-{mid}",
        "Body": body,
        "MessageAttributes": {"forze_encoding": {"StringValue": "b64"}},
    }


@pytest.mark.asyncio
async def test_receive_skips_poison_base64_and_returns_good() -> None:
    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={
            "Messages": [
                _b64_msg("good", base64.b64encode(b"hello").decode()),
                _b64_msg("bad", "!!! not base64 !!!"),
            ]
        }
    )

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs"),
    ):
        with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
            msgs = await client.receive("jobs", limit=10)

    # The poison message is skipped; the good one still comes back decoded.
    assert [m.id for m in msgs] == ["good"]
    assert msgs[0].body == b"hello"

    # The poison message is never registered for ack/nack (the caller never sees it).
    pending = client._SQSClient__pending  # type: ignore[attr-defined]
    assert "good" in pending
    assert "bad" not in pending
