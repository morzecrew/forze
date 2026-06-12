"""Unit tests for SQS transport headers and delivery_count (no broker)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pydantic import BaseModel

pytest.importorskip("aioboto3")

from forze.base.serialization import PydanticModelCodec
from forze_sqs.adapters import SQSQueueAdapter, SQSQueueCodec
from forze_sqs.kernel.client import SQSClient, SQSQueueMessage

# ----------------------- #


class _Payload(BaseModel):
    value: str


# ----------------------- #
# Publish-side attribute building: caller headers verbatim, reserved win.


def test_build_message_attributes_headers_and_collision_rule() -> None:
    attrs = SQSClient._SQSClient__build_message_attributes(
        type="created",
        key="real-key",
        enqueued_at=None,
        headers={
            "trace": "t-1",
            "forze_key": "forged",  # collides with the reserved key attr
            "forze_encoding": "forged-too",
        },
    )

    assert attrs["trace"] == {"StringValue": "t-1", "DataType": "String"}
    # Reserved transport attributes always win over colliding caller headers.
    assert attrs["forze_key"]["StringValue"] == "real-key"
    assert attrs["forze_encoding"]["StringValue"] == "b64"
    assert attrs["forze_type"]["StringValue"] == "created"


def test_build_message_attributes_without_headers_unchanged() -> None:
    attrs = SQSClient._SQSClient__build_message_attributes(
        type=None,
        key=None,
        enqueued_at=None,
    )

    assert set(attrs) == {"forze_encoding"}


# ----------------------- #
# Receive-side extraction helpers.


class TestExtractHeaders:
    def test_none(self) -> None:
        assert SQSClient._SQSClient__extract_headers(None) is None

    def test_reserved_attrs_excluded(self) -> None:
        got = SQSClient._SQSClient__extract_headers(
            {
                "forze_type": {"StringValue": "created", "DataType": "String"},
                "forze_key": {"StringValue": "k", "DataType": "String"},
                "forze_encoding": {"StringValue": "b64", "DataType": "String"},
                "forze_enqueued_at": {"StringValue": "ts", "DataType": "String"},
                "trace": {"StringValue": "t-1", "DataType": "String"},
                "binary": {"BinaryValue": b"x", "DataType": "Binary"},
            }
        )

        assert got == {"trace": "t-1"}

    def test_empty_collapses_to_none(self) -> None:
        assert (
            SQSClient._SQSClient__extract_headers(
                {"forze_encoding": {"StringValue": "b64", "DataType": "String"}}
            )
            is None
        )


class TestExtractDeliveryCount:
    def test_parses_approximate_receive_count(self) -> None:
        got = SQSClient._SQSClient__extract_delivery_count(
            {"ApproximateReceiveCount": "3", "SentTimestamp": "1700000000000"}
        )

        assert got == 3

    def test_missing_or_invalid_is_none(self) -> None:
        assert SQSClient._SQSClient__extract_delivery_count(None) is None
        assert SQSClient._SQSClient__extract_delivery_count({}) is None
        assert (
            SQSClient._SQSClient__extract_delivery_count(
                {"ApproximateReceiveCount": "nope"}
            )
            is None
        )


# ----------------------- #
# Codec + adapter pass-through.


def test_codec_decodes_headers_and_delivery_count() -> None:
    codec = SQSQueueCodec(payload_codec=PydanticModelCodec(_Payload))
    encoded = codec.encode(_Payload(value="hello"))

    decoded = codec.decode(
        "jobs",
        SQSQueueMessage(
            queue="jobs",
            id="m-1",
            body=encoded,
            headers={"trace": "t-1"},
            delivery_count=4,
        ),
    )

    assert decoded.headers == {"trace": "t-1"}
    assert decoded.delivery_count == 4


@pytest.mark.asyncio
async def test_adapter_forwards_headers_to_client() -> None:
    client = Mock(spec=SQSClient)
    client.client = MagicMock(return_value=AsyncMock())
    client.enqueue = AsyncMock(return_value="m-1")
    client.enqueue_many = AsyncMock(return_value=["m-1"])
    adapter = SQSQueueAdapter(
        client=client,
        codec=SQSQueueCodec(payload_codec=PydanticModelCodec(_Payload)),
        namespace="ns",
    )

    await adapter.enqueue("jobs", _Payload(value="x"), headers={"trace": "t-1"})
    assert client.enqueue.await_args.kwargs["headers"] == {"trace": "t-1"}

    await adapter.enqueue_many("jobs", [_Payload(value="x")], headers={"a": "b"})
    assert client.enqueue_many.await_args.kwargs["headers"] == {"a": "b"}
