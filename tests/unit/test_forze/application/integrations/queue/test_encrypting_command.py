"""The encrypting queue decorator batches: one inner enqueue_many, per-message event ids."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.queue import QueueSpec
from forze.application.integrations.crypto import Keyring, is_encrypted_payload
from forze.application.integrations.queue import encrypting_queue_command
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockKeyManagement

# ----------------------- #


class _Job(BaseModel):
    n: int


@attrs.define(slots=True)
class _SpyQueueCommand:
    """Records every publish call so the test can assert the batch shape."""

    enqueue_calls: int = 0
    many_calls: list[dict[str, Any]] = attrs.field(factory=list)

    async def enqueue(self, queue: str, payload: Any, **kw: Any) -> str:
        self.enqueue_calls += 1
        return "single"

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[Any],
        *,
        message_headers: Sequence[Mapping[str, str]] | None = None,
        **kw: Any,
    ) -> list[str]:
        self.many_calls.append(
            {"payloads": list(payloads), "message_headers": message_headers}
        )
        return [f"id-{i}" for i in range(len(payloads))]


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _wrap(inner: _SpyQueueCommand):
    spec = QueueSpec(name="jobs", codec=PydanticModelCodec(_Job), encryption="end_to_end")  # type: ignore[arg-type]
    return encrypting_queue_command(
        inner, spec, cipher=_keyring(), tenant_provider=lambda: None
    )


# ....................... #


@pytest.mark.asyncio
async def test_batch_is_one_inner_call_with_per_message_event_ids() -> None:
    spy = _SpyQueueCommand()

    ids = await _wrap(spy).enqueue_many("jobs", [_Job(n=1), _Job(n=2), _Job(n=3)])

    # One backend round-trip, not three — and never the per-message enqueue path.
    assert spy.enqueue_calls == 0
    assert len(spy.many_calls) == 1

    call = spy.many_calls[0]
    assert all(is_encrypted_payload(p) for p in call["payloads"])  # sealed

    msg_headers = call["message_headers"]
    assert msg_headers is not None and len(msg_headers) == 3
    event_ids = {h[HEADER_EVENT_ID] for h in msg_headers}
    assert len(event_ids) == 3  # each message carries its own AAD anchor
    assert ids == ["id-0", "id-1", "id-2"]


@pytest.mark.asyncio
async def test_empty_batch_short_circuits() -> None:
    spy = _SpyQueueCommand()

    assert await _wrap(spy).enqueue_many("jobs", []) == []
    assert spy.many_calls == []
    assert spy.enqueue_calls == 0
