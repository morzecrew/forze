"""``nack(requeue=False)`` must not wedge a FIFO message group.

A terminal nack leaves the message invisible so SQS's redrive policy can count the receive
and dead-letter it natively — correct on a *standard* queue. On a **FIFO** queue the same
treatment is a deadlock: the undeleted message sits at the head of its message group and
blocks every later message in it, redelivering at the head forever when no redrive policy
trims it — exactly the in-app ``max_deliveries`` shape, where the caller has already ruled
the message poison and nothing else will ever remove it.

So a FIFO poison nack takes the same path ``receive()`` takes for an undecodable message:
retain a raw copy on ``poison_queue_url`` when configured, then delete to free the group.
"""

import base64
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from forze_sqs.kernel.client import SQSClient


def _msg(mid: str) -> dict[str, Any]:
    """A perfectly decodable delivery — the poison verdict here is the caller's, not a decode failure."""

    return {
        "MessageId": mid,
        "ReceiptHandle": f"r-{mid}",
        "Body": base64.b64encode(b"hello").decode(),
        "MessageAttributes": {"forze_encoding": {"StringValue": "b64"}},
    }


class _Recorder:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def warning(self, msg: str, *args: object, **_kw: object) -> None:
        self.warnings.append(msg % args if args else msg)

    def error(self, msg: str, *args: object, **_kw: object) -> None:
        self.errors.append(msg % args if args else msg)

    def trace(self, *_a: object, **_kw: object) -> None: ...


async def _receive_then_nack(
    client: SQSClient,
    mock_boto: AsyncMock,
    *,
    queue: str,
    queue_url: str,
    requeue: bool,
) -> int:
    """Receive one message (registering it pending), then nack it."""

    mock_boto.receive_message = AsyncMock(return_value={"Messages": [_msg("m1")]})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value=queue_url),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        received = await client.receive(queue, limit=10)
        assert [m.id for m in received] == ["m1"]

        return await client.nack(queue, ["m1"], requeue=requeue)


# ....................... #


@pytest.mark.asyncio
async def test_fifo_terminal_nack_deletes_to_unblock_the_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The regression: previously this left the message undeleted, blocking its group
    # forever on a queue with no redrive policy.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.delete_message = AsyncMock(return_value={})

    nacked = await _receive_then_nack(
        client,
        mock_boto,
        queue="jobs.fifo",
        queue_url="https://sqs/jobs.fifo",
        requeue=False,
    )

    assert nacked == 1
    mock_boto.delete_message.assert_awaited_once_with(
        QueueUrl="https://sqs/jobs.fifo", ReceiptHandle="r-m1"
    )
    # Nothing retained (no poison queue configured), so the destruction names the knob.
    destroyed = [w for w in recorder.warnings if "poison_queue_url" in w]
    assert len(destroyed) == 1
    assert client._SQSClient__pending == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_fifo_terminal_nack_retains_a_raw_copy_before_deleting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    calls: list[str] = []
    mock_boto = AsyncMock()
    mock_boto.send_message = AsyncMock(side_effect=lambda **_kw: calls.append("send") or {})
    mock_boto.delete_message = AsyncMock(side_effect=lambda **_kw: calls.append("delete") or {})

    nacked = await _receive_then_nack(
        client,
        mock_boto,
        queue="jobs.fifo",
        queue_url="https://sqs/jobs.fifo",
        requeue=False,
    )

    assert nacked == 1
    assert calls == ["send", "delete"]  # retain first, then unblock the group

    send_kwargs = mock_boto.send_message.await_args.kwargs
    assert send_kwargs["QueueUrl"] == "https://sqs/poison"
    # The RAW (still-encoded) body is preserved byte-for-byte, not the decoded model.
    assert send_kwargs["MessageBody"] == base64.b64encode(b"hello").decode()

    attrs_ = send_kwargs["MessageAttributes"]
    assert attrs_["forze_poison_source_queue"]["StringValue"] == "jobs.fifo"
    assert attrs_["forze_poison_message_id"]["StringValue"] == "m1"
    assert attrs_["forze_encoding"]["StringValue"] == "b64"  # original attrs carried

    assert recorder.errors == []
    assert client._SQSClient__pending == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_failed_retention_does_not_delete_the_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A throttled or unavailable poison queue must not become permanent data loss: with
    # retention configured but not achieved, the original stays put. The group is blocked,
    # which is recoverable — the message reappears and the next attempt retries the copy.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    mock_boto = AsyncMock()
    mock_boto.send_message = AsyncMock(side_effect=RuntimeError("throttled"))
    mock_boto.delete_message = AsyncMock(return_value={})

    nacked = await _receive_then_nack(
        client,
        mock_boto,
        queue="jobs.fifo",
        queue_url="https://sqs/jobs.fifo",
        requeue=False,
    )

    assert nacked == 1
    mock_boto.delete_message.assert_not_awaited()  # on neither queue would be data loss
    assert any("throttled" in e for e in recorder.errors)


@pytest.mark.asyncio
async def test_retention_configured_after_receive_does_not_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The delivery was read while no retention queue was configured, so no raw copy was
    # kept for it; retention was turned on afterwards. There is nothing to send, and the
    # operator has asked for retention — so the original is kept rather than destroyed.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(return_value={"Messages": [_msg("m1")]})
    mock_boto.send_message = AsyncMock(return_value={})
    mock_boto.delete_message = AsyncMock(return_value={})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        assert [m.id for m in await client.receive("jobs.fifo", limit=10)] == ["m1"]

        # Retention switched on only now — the in-flight delivery has no retained body.
        client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

        assert await client.nack("jobs.fifo", ["m1"], requeue=False) == 1

    mock_boto.send_message.assert_not_awaited()  # nothing to send
    mock_boto.delete_message.assert_not_awaited()  # and so nothing destroyed
    assert any("no raw copy was retained" in e for e in recorder.errors)


@pytest.mark.asyncio
async def test_standard_queue_terminal_nack_still_leaves_it_for_redrive() -> None:
    # Unchanged on a standard queue: not deleting is what lets SQS's own redrive policy
    # count the receive and dead-letter it. Deleting here would bypass the native DLQ.
    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.delete_message = AsyncMock(return_value={})
    mock_boto.send_message = AsyncMock(return_value={})

    nacked = await _receive_then_nack(
        client,
        mock_boto,
        queue="jobs",
        queue_url="https://sqs/jobs",
        requeue=False,
    )

    assert nacked == 1
    mock_boto.delete_message.assert_not_awaited()
    mock_boto.send_message.assert_not_awaited()
    assert client._SQSClient__pending == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_fifo_requeue_nack_resets_visibility_and_never_deletes() -> None:
    # requeue=True is a redelivery request, not a poison verdict — it must not delete.
    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.delete_message = AsyncMock(return_value={})
    mock_boto.change_message_visibility_batch = AsyncMock(return_value={})

    nacked = await _receive_then_nack(
        client,
        mock_boto,
        queue="jobs.fifo",
        queue_url="https://sqs/jobs.fifo",
        requeue=True,
    )

    assert nacked == 1
    mock_boto.change_message_visibility_batch.assert_awaited_once()
    mock_boto.delete_message.assert_not_awaited()
    assert client._SQSClient__pending == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_fifo_delete_failure_is_best_effort_not_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A failed delete only leaves the group blocked until redrive/visibility — no worse
    # than before — so it must not raise out of nack().
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.delete_message = AsyncMock(side_effect=RuntimeError("throttled"))

    nacked = await _receive_then_nack(
        client,
        mock_boto,
        queue="jobs.fifo",
        queue_url="https://sqs/jobs.fifo",
        requeue=False,
    )

    assert nacked == 1  # reported nacked; the delete failure is logged, not raised
    assert any("delete failed" in w for w in recorder.warnings)
