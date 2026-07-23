"""Unit tests for SQSClient receive/ack/nack id semantics (no I/O).

``id`` exposes the broker ``MessageId`` (stable, correlatable with enqueue);
the per-delivery ``ReceiptHandle`` lives on ``receipt_handle`` and is resolved
internally for ack/nack. ``nack(requeue=False)`` never deletes — the message
is left to reappear after its visibility timeout so the queue's redrive
policy can dead-letter it.
"""

from typing import Any

import pytest

pytest.importorskip("aioboto3")

from forze_sqs.kernel.client import SQSClient

# ----------------------- #

_QUEUE = "jobs"
_QUEUE_URL = "https://sqs.local/1/jobs"


class _FakeSqs:
    """Records ack/nack/receive calls and returns scripted responses."""

    def __init__(self, messages: list[dict[str, Any]] | None = None) -> None:
        self.messages = messages or []
        self.receive_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        self.single_delete_calls: list[dict[str, Any]] = []
        self.send_calls: list[dict[str, Any]] = []
        self.visibility_calls: list[dict[str, Any]] = []
        self.visibility_error: Exception | None = None
        self.visibility_failed_entries: list[dict[str, str]] = []
        self.send_error: Exception | None = None
        self.single_delete_error: Exception | None = None
        self.queue_attributes: dict[str, Any] = {}
        self.attribute_calls: list[dict[str, Any]] = []
        self.attribute_error: Exception | None = None

    async def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.receive_calls.append(kwargs)
        return {"Messages": list(self.messages)}

    async def delete_message_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.delete_calls.append(kwargs)
        entries = kwargs.get("Entries") or []
        return {
            "Successful": [{"Id": e["Id"]} for e in entries],
            "Failed": [],
        }

    async def delete_message(self, **kwargs: Any) -> dict[str, Any]:
        self.single_delete_calls.append(kwargs)

        if self.single_delete_error is not None:
            raise self.single_delete_error

        return {}

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.send_calls.append(kwargs)

        if self.send_error is not None:
            raise self.send_error

        return {"MessageId": f"copy-{len(self.send_calls)}"}

    async def get_queue_attributes(self, **kwargs: Any) -> dict[str, Any]:
        self.attribute_calls.append(kwargs)

        if self.attribute_error is not None:
            raise self.attribute_error

        return {"Attributes": dict(self.queue_attributes)}

    async def change_message_visibility_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.visibility_calls.append(kwargs)

        if self.visibility_error is not None:
            raise self.visibility_error

        entries = kwargs.get("Entries") or []
        return {
            "Successful": [{"Id": e["Id"]} for e in entries],
            "Failed": list(self.visibility_failed_entries),
        }


def _bind(client: SQSClient, fake: _FakeSqs) -> None:
    client._SQSClient__queue_url_cache[_QUEUE] = _QUEUE_URL  # type: ignore[attr-defined]
    client._SQSClient__ctx_client.set(fake)  # type: ignore[attr-defined]
    client._SQSClient__ctx_depth.set(1)  # type: ignore[attr-defined]


def _raw_message(message_id: str = "mid-1", receipt: str = "receipt-1") -> dict[str, Any]:
    return {"MessageId": message_id, "ReceiptHandle": receipt, "Body": "hello"}


# ----------------------- #


class TestReceiveIdSemantics:
    @pytest.mark.asyncio
    async def test_id_is_message_id_and_receipt_handle_is_separate(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        messages = await client.receive(_QUEUE, limit=1)

        assert len(messages) == 1
        assert messages[0].id == "mid-1"
        assert messages[0].receipt_handle == "receipt-1"

    @pytest.mark.asyncio
    async def test_missing_message_id_falls_back_to_receipt(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([{"ReceiptHandle": "receipt-only", "Body": "hello"}])
        _bind(client, fake)

        messages = await client.receive(_QUEUE, limit=1)

        assert messages[0].id == "receipt-only"
        assert messages[0].receipt_handle == "receipt-only"

    @pytest.mark.asyncio
    async def test_redelivery_overwrites_receipt_handle(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        await client.receive(_QUEUE, limit=1)
        fake.messages = [_raw_message("mid-1", "receipt-2")]
        await client.receive(_QUEUE, limit=1)

        await client.ack(_QUEUE, ["mid-1"])

        # ack must use the latest receipt handle, not the superseded one.
        entries = fake.delete_calls[0]["Entries"]
        assert entries[0]["ReceiptHandle"] == "receipt-2"


# ....................... #


class TestAck:
    @pytest.mark.asyncio
    async def test_ack_resolves_receipt_handle_from_message_id(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]
        acked = await client.ack(_QUEUE, [message.id])

        assert acked == 1
        assert len(fake.delete_calls) == 1
        entries = fake.delete_calls[0]["Entries"]
        assert entries == [{"Id": "m0", "ReceiptHandle": "receipt-1"}]

    @pytest.mark.asyncio
    async def test_ack_unknown_id_is_skipped_without_broker_call(self) -> None:
        client = SQSClient()
        fake = _FakeSqs()
        _bind(client, fake)

        assert await client.ack(_QUEUE, ["never-received"]) == 0
        assert fake.delete_calls == []

    @pytest.mark.asyncio
    async def test_ack_drops_pending_entry(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]
        assert await client.ack(_QUEUE, [message.id]) == 1

        # The delivery is settled: a second ack is a no-op.
        assert await client.ack(_QUEUE, [message.id]) == 0
        assert len(fake.delete_calls) == 1

    @pytest.mark.asyncio
    async def test_ack_skips_pending_entry_for_other_queue(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)
        client._SQSClient__queue_url_cache["other"] = "https://sqs.local/1/other"  # type: ignore[attr-defined]

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.ack("other", [message.id]) == 0
        assert fake.delete_calls == []


# ....................... #


class TestNack:
    @pytest.mark.asyncio
    async def test_requeue_true_resets_visibility_to_zero(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]
        nacked = await client.nack(_QUEUE, [message.id], requeue=True)

        assert nacked == 1
        assert fake.delete_calls == []
        assert len(fake.visibility_calls) == 1
        entries = fake.visibility_calls[0]["Entries"]
        assert entries == [
            {"Id": "m0", "ReceiptHandle": "receipt-1", "VisibilityTimeout": 0}
        ]

    @pytest.mark.asyncio
    async def test_requeue_false_issues_no_delete(self) -> None:
        """nack(requeue=False) must NOT delete: the message stays for the
        queue's redrive policy instead of being permanently lost."""
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]
        nacked = await client.nack(_QUEUE, [message.id], requeue=False)

        assert nacked == 1
        assert fake.delete_calls == []
        assert fake.visibility_calls == []

    @pytest.mark.asyncio
    async def test_requeue_true_visibility_error_is_best_effort(self) -> None:
        """A failed visibility reset is logged, not raised: the message still
        redelivers once its original visibility timeout lapses."""
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        fake.visibility_error = RuntimeError("boom")
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.nack(_QUEUE, [message.id], requeue=True) == 1

    @pytest.mark.asyncio
    async def test_requeue_true_failed_entries_are_best_effort(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        fake.visibility_failed_entries = [{"Id": "m0", "Code": "InternalError"}]
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.nack(_QUEUE, [message.id], requeue=True) == 1

    @pytest.mark.asyncio
    async def test_nack_unknown_id_is_skipped_without_broker_call(self) -> None:
        client = SQSClient()
        fake = _FakeSqs()
        _bind(client, fake)

        assert await client.nack(_QUEUE, ["never-received"], requeue=True) == 0
        assert await client.nack(_QUEUE, ["never-received"], requeue=False) == 0
        assert fake.visibility_calls == []
        assert fake.delete_calls == []

    @pytest.mark.asyncio
    async def test_nack_drops_pending_entry(self) -> None:
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]
        assert await client.nack(_QUEUE, [message.id], requeue=False) == 1

        # The local delivery is settled; a follow-up ack is a no-op.
        assert await client.ack(_QUEUE, [message.id]) == 0
        assert fake.delete_calls == []


# ....................... #


_FIFO_QUEUE = "jobs.fifo"
_FIFO_QUEUE_URL = "https://sqs.local/1/jobs.fifo"


def _fifo_bind(client: SQSClient, fake: _FakeSqs) -> None:
    client._SQSClient__queue_url_cache[_FIFO_QUEUE] = _FIFO_QUEUE_URL  # type: ignore[attr-defined]
    client._SQSClient__ctx_client.set(fake)  # type: ignore[attr-defined]
    client._SQSClient__ctx_depth.set(1)  # type: ignore[attr-defined]


def _fifo_raw(
    message_id: str = "mid-1",
    receipt: str = "receipt-1",
    *,
    receive_count: int,
    group: str = "g1",
) -> dict[str, Any]:
    return {
        "MessageId": message_id,
        "ReceiptHandle": receipt,
        "Body": "hello",
        "Attributes": {
            "ApproximateReceiveCount": str(receive_count),
            "MessageGroupId": group,
        },
    }


def _redrive_policy(max_receive_count: int) -> dict[str, Any]:
    import json

    return {
        "RedrivePolicy": json.dumps(
            {"deadLetterTargetArn": "arn:aws:sqs:x:1:dlq", "maxReceiveCount": max_receive_count}
        )
    }


class TestNackUncounted:
    """``nack(requeue=True, count=False)``: a redelivery that is not the message's fault.

    A standard queue replaces the message with a byte-identical copy (fresh broker
    ``MessageId``, receive count zero) and deletes the original, so an outage's requeue
    cycles can never creep toward the redrive policy's ``maxReceiveCount`` and
    dead-letter a healthy message. A FIFO queue keeps the order-preserving visibility
    reset until one more counted receive could cross the redrive threshold — where the
    broker was about to break group order anyway — and only then copies back, under the
    original ``MessageGroupId``.
    """

    @pytest.mark.asyncio
    async def test_standard_queue_requeues_a_copy_and_deletes_the_original(self) -> None:
        client = SQSClient()
        attributes = {"forze_type": {"StringValue": "T", "DataType": "String"}}
        fake = _FakeSqs(
            [
                {
                    "MessageId": "mid-1",
                    "ReceiptHandle": "receipt-1",
                    "Body": "hello",
                    "MessageAttributes": attributes,
                }
            ]
        )
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.nack(_QUEUE, [message.id], requeue=True, count=False) == 1

        # A byte-identical copy went back onto the same queue…
        assert len(fake.send_calls) == 1
        sent = fake.send_calls[0]
        assert sent["QueueUrl"] == _QUEUE_URL
        assert sent["MessageBody"] == "hello"
        assert sent["MessageAttributes"] == attributes
        # …the original was deleted, and no counted visibility reset happened.
        assert fake.single_delete_calls == [
            {"QueueUrl": _QUEUE_URL, "ReceiptHandle": "receipt-1"}
        ]
        assert fake.visibility_calls == []

    @pytest.mark.asyncio
    async def test_failed_copy_falls_back_to_counted_reset_without_deleting(self) -> None:
        # Copy first, delete only after the copy is on the queue: a failed send must
        # leave the original untouched (visibility reset — counted, but never lossy).
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        fake.send_error = RuntimeError("send throttled")
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.nack(_QUEUE, [message.id], requeue=True, count=False) == 1
        assert fake.single_delete_calls == []  # the original survives
        assert len(fake.visibility_calls) == 1  # degraded to the counted requeue

    @pytest.mark.asyncio
    async def test_failed_delete_after_copy_is_best_effort(self) -> None:
        # Send succeeded, delete failed: the original redelivers alongside its copy and
        # the consumer-side inbox dedup absorbs the duplicate — logged, never raised.
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        fake.single_delete_error = RuntimeError("delete failed")
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.nack(_QUEUE, [message.id], requeue=True, count=False) == 1
        assert len(fake.send_calls) == 1

    @pytest.mark.asyncio
    async def test_fifo_below_threshold_keeps_the_counted_reset(self) -> None:
        # Order-preservation is free while the receive count is well under the redrive
        # threshold — a drain refusal must not rotate its message group.
        client = SQSClient()
        fake = _FakeSqs([_fifo_raw(receive_count=2)])
        fake.queue_attributes = _redrive_policy(5)
        _fifo_bind(client, fake)

        message = (await client.receive(_FIFO_QUEUE, limit=1))[0]

        assert await client.nack(_FIFO_QUEUE, [message.id], requeue=True, count=False) == 1
        assert fake.send_calls == []
        assert fake.single_delete_calls == []
        assert len(fake.visibility_calls) == 1

    @pytest.mark.asyncio
    async def test_fifo_near_redrive_threshold_copies_back_under_its_group(self) -> None:
        # One more counted receive would make the head DLQ-eligible: the broker was
        # about to break group order anyway, so the copy trades nothing — the message
        # re-enters at the back of its own group with the count reset.
        client = SQSClient()
        fake = _FakeSqs([_fifo_raw(receive_count=4)])
        fake.queue_attributes = _redrive_policy(5)
        _fifo_bind(client, fake)

        message = (await client.receive(_FIFO_QUEUE, limit=1))[0]

        assert await client.nack(_FIFO_QUEUE, [message.id], requeue=True, count=False) == 1
        assert len(fake.send_calls) == 1
        sent = fake.send_calls[0]
        assert sent["QueueUrl"] == _FIFO_QUEUE_URL
        assert sent["MessageBody"] == "hello"
        assert sent["MessageGroupId"] == "g1"  # group affinity survives
        # Deterministic dedup id: a retried copy after a failed delete dedups
        # instead of duplicating.
        assert sent["MessageDeduplicationId"] == "mid-1-r4"
        assert fake.single_delete_calls == [
            {"QueueUrl": _FIFO_QUEUE_URL, "ReceiptHandle": "receipt-1"}
        ]
        assert fake.visibility_calls == []

    @pytest.mark.asyncio
    async def test_fifo_without_redrive_policy_always_resets(self) -> None:
        # No redrive policy → no DLQ to protect against: blocking the group head is
        # lossless and order-preserving, whatever the receive count.
        client = SQSClient()
        fake = _FakeSqs([_fifo_raw(receive_count=50)])
        _fifo_bind(client, fake)

        message = (await client.receive(_FIFO_QUEUE, limit=1))[0]

        assert await client.nack(_FIFO_QUEUE, [message.id], requeue=True, count=False) == 1
        assert fake.send_calls == []
        assert len(fake.visibility_calls) == 1

    @pytest.mark.asyncio
    async def test_fifo_redrive_lookup_failure_degrades_to_reset_without_caching(self) -> None:
        # A failed GetQueueAttributes must not disable the copy-back forever: the reset
        # covers this nack, and the next nack fetches again.
        client = SQSClient()
        fake = _FakeSqs([_fifo_raw(receive_count=50)])
        fake.attribute_error = RuntimeError("attributes unavailable")
        _fifo_bind(client, fake)

        message = (await client.receive(_FIFO_QUEUE, limit=1))[0]
        assert await client.nack(_FIFO_QUEUE, [message.id], requeue=True, count=False) == 1
        assert len(fake.visibility_calls) == 1
        assert fake.send_calls == []

        fake.messages = [_fifo_raw(receive_count=51, receipt="receipt-2")]
        message = (await client.receive(_FIFO_QUEUE, limit=1))[0]
        assert await client.nack(_FIFO_QUEUE, [message.id], requeue=True, count=False) == 1

        assert len(fake.attribute_calls) == 2  # failure was not cached

    @pytest.mark.asyncio
    async def test_counted_requeue_never_sends_a_copy(self) -> None:
        # The default path is untouched: count=True is the plain visibility reset.
        client = SQSClient()
        fake = _FakeSqs([_raw_message("mid-1", "receipt-1")])
        _bind(client, fake)

        message = (await client.receive(_QUEUE, limit=1))[0]

        assert await client.nack(_QUEUE, [message.id], requeue=True) == 1
        assert fake.send_calls == []
        assert fake.single_delete_calls == []
        assert len(fake.visibility_calls) == 1


# ....................... #


class TestHealthSelfContained:
    @pytest.mark.asyncio
    async def test_health_without_ambient_scope_opens_own_scope(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from contextlib import asynccontextmanager

        client = SQSClient()
        fake = _FakeSqs()
        entered: list[bool] = []

        async def _list_queues(**kwargs: Any) -> dict[str, Any]:
            return {"QueueUrls": []}

        fake.list_queues = _list_queues  # type: ignore[attr-defined]

        @asynccontextmanager
        async def _fake_scope(self: SQSClient):
            entered.append(True)
            yield fake

        monkeypatch.setattr(SQSClient, "client", _fake_scope)

        message, ok = await client.health()

        assert (message, ok) == ("ok", True)
        assert entered == [True]

    @pytest.mark.asyncio
    async def test_health_without_scope_or_session_reports_failure(self) -> None:
        """An uninitialized client reports failure instead of raising."""
        client = SQSClient()

        message, ok = await client.health()

        assert ok is False
        assert "not initialized" in message

    @pytest.mark.asyncio
    async def test_health_reuses_ambient_scope(self) -> None:
        client = SQSClient()
        fake = _FakeSqs()
        calls: list[dict[str, Any]] = []

        async def _list_queues(**kwargs: Any) -> dict[str, Any]:
            calls.append(kwargs)
            return {"QueueUrls": []}

        fake.list_queues = _list_queues  # type: ignore[attr-defined]
        _bind(client, fake)

        message, ok = await client.health()

        assert (message, ok) == ("ok", True)
        assert calls == [{"MaxResults": 1}]
