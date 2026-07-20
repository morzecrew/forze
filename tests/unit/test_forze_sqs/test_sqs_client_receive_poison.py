"""SQSClient.receive() isolates a per-message decode failure: one non-base64 body must not
poison the whole batch — the good messages are still returned and registered for ack/nack, and
the poison message is left in-flight (SQS redelivery + redrive → DLQ handles it).

The FIFO exception (hard delete to unblock the message group) retains a raw copy on the
configured ``poison_queue_url`` before deleting; without one it stays a destructive delete,
logged at WARNING."""

import base64
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_sqs.kernel.client import SQSClient


def _b64_msg(mid: str, body: str) -> dict[str, Any]:
    return {
        "MessageId": mid,
        "ReceiptHandle": f"r-{mid}",
        "Body": body,
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
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs", limit=10)

    # The poison message is skipped; the good one still comes back decoded.
    assert [m.id for m in msgs] == ["good"]
    assert msgs[0].body == b"hello"

    # The poison message is never registered for ack/nack (the caller never sees it).
    pending = client._SQSClient__pending  # type: ignore[attr-defined]
    assert "good" in pending
    assert "bad" not in pending


@pytest.mark.asyncio
async def test_receive_deletes_fifo_poison_to_unblock_group() -> None:
    # On a FIFO queue a skipped-but-undeleted poison message deadlocks its whole message group.
    # With no poison queue configured, receive() deletes it (logged destruction) and returns
    # the good messages behind it.
    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={
            "Messages": [
                _b64_msg("bad", "!!! not base64 !!!"),
                _b64_msg("good", base64.b64encode(b"hello").decode()),
            ]
        }
    )
    mock_boto.delete_message = AsyncMock(return_value={})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs.fifo", limit=10)

    # Poison deleted (group unblocked); good message still returned and pending.
    mock_boto.delete_message.assert_awaited_once_with(
        QueueUrl="https://sqs/jobs.fifo", ReceiptHandle="r-bad"
    )
    assert [m.id for m in msgs] == ["good"]
    pending = client._SQSClient__pending  # type: ignore[attr-defined]
    assert "good" in pending and "bad" not in pending


@pytest.mark.asyncio
async def test_fifo_poison_without_poison_queue_warns_and_deletes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No poison_queue_url configured: current semantics pinned — the message is
    # deleted (group unblocked) and the destruction is a WARNING, not an ERROR,
    # naming the retention knob.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={"Messages": [_b64_msg("bad", "!!! not base64 !!!")]}
    )
    mock_boto.delete_message = AsyncMock(return_value={})
    mock_boto.send_message = AsyncMock()

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs.fifo", limit=10)

    assert msgs == []
    mock_boto.send_message.assert_not_awaited()
    mock_boto.delete_message.assert_awaited_once_with(
        QueueUrl="https://sqs/jobs.fifo", ReceiptHandle="r-bad"
    )
    assert recorder.errors == []
    destroyed = [w for w in recorder.warnings if "poison_queue_url" in w]
    assert len(destroyed) == 1
    assert "bad" in destroyed[0] and "jobs.fifo" in destroyed[0]


@pytest.mark.asyncio
async def test_fifo_poison_with_poison_queue_retains_before_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # poison_queue_url configured: the raw (still-encoded) body is sent to the
    # side queue with provenance attributes BEFORE the delete, and the log stays
    # a WARNING (nothing was destroyed).
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    calls: list[str] = []
    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={"Messages": [_b64_msg("bad", "!!! not base64 !!!")]}
    )

    async def _send(**_kw: Any) -> dict[str, Any]:
        calls.append("send")
        return {}

    async def _delete(**_kw: Any) -> dict[str, Any]:
        calls.append("delete")
        return {}

    mock_boto.send_message = AsyncMock(side_effect=_send)
    mock_boto.delete_message = AsyncMock(side_effect=_delete)

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs.fifo", limit=10)

    assert msgs == []
    assert calls == ["send", "delete"]  # retain first, then unblock the group

    send_kwargs = mock_boto.send_message.await_args.kwargs
    assert send_kwargs["QueueUrl"] == "https://sqs/poison"
    # Body forwarded verbatim (still-encoded, byte-identical) — never re-encoded.
    assert send_kwargs["MessageBody"] == "!!! not base64 !!!"
    # Standard side queue: no FIFO entry fields.
    assert "MessageGroupId" not in send_kwargs
    assert "MessageDeduplicationId" not in send_kwargs

    attrs_ = send_kwargs["MessageAttributes"]
    assert attrs_["forze_poison_source_queue"]["StringValue"] == "jobs.fifo"
    assert attrs_["forze_poison_message_id"]["StringValue"] == "bad"
    # Original attributes carried alongside the provenance ones.
    assert attrs_["forze_encoding"]["StringValue"] == "b64"

    mock_boto.delete_message.assert_awaited_once_with(
        QueueUrl="https://sqs/jobs.fifo", ReceiptHandle="r-bad"
    )
    assert recorder.errors == []
    retained = [w for w in recorder.warnings if "https://sqs/poison" in w]
    assert len(retained) == 1


@pytest.mark.asyncio
async def test_fifo_poison_copy_to_fifo_side_queue_sets_group_and_dedup() -> None:
    # A FIFO side queue needs MessageGroupId/MessageDeduplicationId: the original
    # message id serves as both — each poison is its own group (one poison never
    # blocks another) and a redelivered original never lands twice.
    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison.fifo"  # type: ignore[attr-defined]

    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={"Messages": [_b64_msg("bad", "!!! not base64 !!!")]}
    )
    mock_boto.send_message = AsyncMock(return_value={})
    mock_boto.delete_message = AsyncMock(return_value={})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        await client.receive("jobs.fifo", limit=10)

    send_kwargs = mock_boto.send_message.await_args.kwargs
    assert send_kwargs["MessageGroupId"] == "bad"
    assert send_kwargs["MessageDeduplicationId"] == "bad"
    mock_boto.delete_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_fifo_poison_side_send_failure_still_deletes_and_logs_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Unblocking the group is the primary duty: a failed side-send never blocks
    # the delete, but the destruction log is upgraded to ERROR.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={"Messages": [_b64_msg("bad", "!!! not base64 !!!")]}
    )
    mock_boto.send_message = AsyncMock(side_effect=RuntimeError("side queue down"))
    mock_boto.delete_message = AsyncMock(return_value={})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs.fifo", limit=10)

    assert msgs == []
    mock_boto.delete_message.assert_awaited_once_with(
        QueueUrl="https://sqs/jobs.fifo", ReceiptHandle="r-bad"
    )
    destroyed = [e for e in recorder.errors if "https://sqs/poison" in e]
    assert len(destroyed) == 1
    assert "bad" in destroyed[0] and "side queue down" in destroyed[0]


@pytest.mark.asyncio
async def test_receive_does_not_delete_poison_on_standard_queue() -> None:
    # Standard queue: skip-and-leave-for-redrive; never delete (redrive → DLQ preserves it).
    client = SQSClient()
    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={"Messages": [_b64_msg("bad", "!!! not base64 !!!")]}
    )
    mock_boto.delete_message = AsyncMock()

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs", limit=10)

    assert msgs == []
    mock_boto.delete_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_poison_copy_keeps_all_originals_under_slot_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An original already at the 10-attribute SQS cap keeps every original
    # attribute on the copy (they are the replay context); the provenance
    # attributes are dropped with a warning naming them — never silently.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    originals = {
        f"attr_{i}": {"StringValue": f"v{i}", "DataType": "String"} for i in range(10)
    }

    mock_boto = AsyncMock()
    mock_boto.send_message = AsyncMock(return_value={})

    with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        await client._SQSClient__send_poison_copy(  # type: ignore[attr-defined]
            source_queue="jobs.fifo",
            source_queue_url="https://sqs/jobs.fifo",
            message_id="m-1",
            body="raw",
            attributes=originals,
        )

    sent = mock_boto.send_message.await_args.kwargs["MessageAttributes"]
    assert sent == originals  # all 10 originals, byte-identical
    assert "forze_poison_message_id" not in sent
    assert "forze_poison_source_queue" not in sent
    assert any("no free attribute slots" in w for w in recorder.warnings)


@pytest.mark.asyncio
async def test_poison_copy_prefers_message_id_when_one_slot_remains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # With exactly one free slot, the original message id wins it — the
    # correlation key back to the source log line; the queue name is dropped
    # with a warning.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    originals = {
        f"attr_{i}": {"StringValue": f"v{i}", "DataType": "String"} for i in range(9)
    }

    mock_boto = AsyncMock()
    mock_boto.send_message = AsyncMock(return_value={})

    with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        await client._SQSClient__send_poison_copy(  # type: ignore[attr-defined]
            source_queue="jobs.fifo",
            source_queue_url="https://sqs/jobs.fifo",
            message_id="m-1",
            body="raw",
            attributes=originals,
        )

    sent = mock_boto.send_message.await_args.kwargs["MessageAttributes"]
    assert sent["forze_poison_message_id"]["StringValue"] == "m-1"
    assert "forze_poison_source_queue" not in sent
    assert all(sent[k] == originals[k] for k in originals)
    assert any("no free attribute slots" in w for w in recorder.warnings)


@pytest.mark.asyncio
async def test_poison_copy_without_configured_url_is_an_internal_error() -> None:
    # The caller gates on the knob before invoking; reaching the sender without
    # a URL is a framework bug, not a caller mistake.
    client = SQSClient()

    with pytest.raises(CoreException) as ei:
        await client._SQSClient__send_poison_copy(  # type: ignore[attr-defined]
            source_queue="jobs.fifo",
            source_queue_url="https://sqs/jobs.fifo",
            message_id="m-1",
            body="raw",
            attributes=None,
        )

    assert ei.value.kind is ExceptionKind.INTERNAL


@pytest.mark.asyncio
async def test_poison_copy_original_attribute_wins_a_provenance_name_collision() -> (
    None
):
    # An original attribute that already uses a provenance name is preserved
    # verbatim — the copy never overwrites original context.
    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    originals = {
        "forze_poison_message_id": {"StringValue": "spoofed", "DataType": "String"},
    }

    mock_boto = AsyncMock()
    mock_boto.send_message = AsyncMock(return_value={})

    with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        await client._SQSClient__send_poison_copy(  # type: ignore[attr-defined]
            source_queue="jobs.fifo",
            source_queue_url="https://sqs/jobs.fifo",
            message_id="m-1",
            body="raw",
            attributes=originals,
        )

    sent = mock_boto.send_message.await_args.kwargs["MessageAttributes"]
    assert sent["forze_poison_message_id"]["StringValue"] == "spoofed"
    assert sent["forze_poison_source_queue"]["StringValue"] == "jobs.fifo"


@pytest.mark.asyncio
async def test_poison_copy_with_no_original_attributes_sends_provenance_only() -> None:
    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/poison"  # type: ignore[attr-defined]

    mock_boto = AsyncMock()
    mock_boto.send_message = AsyncMock(return_value={})

    with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        await client._SQSClient__send_poison_copy(  # type: ignore[attr-defined]
            source_queue="jobs",
            source_queue_url="https://sqs/jobs",
            message_id="m-2",
            body="raw",
            attributes=None,
        )

    sent = mock_boto.send_message.await_args.kwargs["MessageAttributes"]
    assert set(sent) == {"forze_poison_message_id", "forze_poison_source_queue"}


@pytest.mark.asyncio
async def test_poison_copy_rejects_a_self_targeted_queue() -> None:
    # poison_queue_url pointing back at the source queue would loop the copy
    # (copy → undecodable → copy …); it is rejected before anything is sent.
    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/jobs.fifo"  # type: ignore[attr-defined]

    mock_boto = AsyncMock()

    with patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        with pytest.raises(CoreException) as ei:
            await client._SQSClient__send_poison_copy(  # type: ignore[attr-defined]
                source_queue="jobs.fifo",
                source_queue_url="https://sqs/jobs.fifo",
                message_id="m-1",
                body="raw",
                attributes=None,
            )

    assert ei.value.kind is ExceptionKind.INTERNAL
    mock_boto.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_self_targeted_poison_queue_still_deletes_and_logs_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Through the receive path: the self-target rejection follows the existing
    # failure ordering — the original is deleted anyway (unblocking the group is
    # the primary duty) and the loss is logged as an ERROR.
    recorder = _Recorder()
    monkeypatch.setattr("forze_sqs.kernel.client.client.logger", recorder)

    client = SQSClient()
    client._SQSClient__poison_queue_url = "https://sqs/jobs.fifo"  # type: ignore[attr-defined]

    mock_boto = AsyncMock()
    mock_boto.receive_message = AsyncMock(
        return_value={"Messages": [_b64_msg("bad", "!!! not base64 !!!")]}
    )
    mock_boto.send_message = AsyncMock(return_value={})
    mock_boto.delete_message = AsyncMock(return_value={})

    with patch.object(
        client,
        "_SQSClient__resolve_queue_url",
        AsyncMock(return_value="https://sqs/jobs.fifo"),
    ), patch.object(client, "_SQSClient__require_client", return_value=mock_boto):
        msgs = await client.receive("jobs.fifo", limit=10)

    assert msgs == []
    mock_boto.send_message.assert_not_awaited()  # nothing published to itself
    mock_boto.delete_message.assert_awaited_once()  # the group is still unblocked
    assert len(recorder.errors) == 1
