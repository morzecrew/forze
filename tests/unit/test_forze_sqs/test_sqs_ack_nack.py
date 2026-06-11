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
        self.visibility_calls: list[dict[str, Any]] = []
        self.visibility_error: Exception | None = None
        self.visibility_failed_entries: list[dict[str, str]] = []

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
