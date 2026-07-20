"""Inngest event payload encryption: sealed on send, opened on receive, handler sees
plaintext; the ``_forze`` context envelope stays readable."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.contracts.durable.function import DurableFunctionEventSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze.application.integrations.crypto import Keyring, is_encrypted_payload
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze_inngest.adapters import InngestEventCommandAdapter
from forze_inngest.adapters.context import merge_envelope, split_envelope
from forze_inngest.adapters.crypto import open_event_payload, seal_event_payload
from forze_mock import MockKeyManagement
from tests.support.execution_context import frozen_deps_from_deps

# ----------------------- #


class _Payload(BaseModel):
    n: int


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _spec(*, encrypt: bool) -> DurableFunctionEventSpec[_Payload]:
    return DurableFunctionEventSpec(
        name="thing.happened", codec=PydanticModelCodec(_Payload), encrypt=encrypt
    )


class _CapturingClient:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def send(self, event: Any) -> list[str]:
        self.events.append(event)
        return ["evt-1"]


# ....................... #


@pytest.mark.asyncio
async def test_send_seal_then_receive_open_round_trips() -> None:
    """The exact send (seal → merge envelope) and receive (split → open) flow."""

    keyring = _keyring()
    tenant = TenantIdentity(tenant_id=uuid4())

    sealed = await seal_event_payload(keyring, {"n": 7}, tenant=tenant)
    assert is_encrypted_payload(sealed)

    # The context envelope is merged alongside the sealed payload, plaintext.
    event_data = merge_envelope(sealed, tenant=tenant)
    assert "_forze" in event_data

    # Receive: the envelope splits off; the remaining payload is still the wrapper.
    envelope, payload = split_envelope(event_data)
    assert envelope.tenant == tenant
    assert is_encrypted_payload(payload)

    opened = await open_event_payload(keyring, payload, tenant=envelope.tenant)
    assert opened == {"n": 7}


@pytest.mark.asyncio
async def test_adapter_seals_payload_when_spec_encrypts() -> None:
    client = _CapturingClient()
    adapter = InngestEventCommandAdapter(
        client=client,  # type: ignore[arg-type]
        spec=_spec(encrypt=True),
        execution_ctx=None,
        include_execution_context=False,
        cipher=_keyring(),
    )

    await adapter.send(_Payload(n=7))

    [event] = client.events
    assert is_encrypted_payload(event.data)  # ciphertext on the wire
    assert "n" not in event.data


@pytest.mark.asyncio
async def test_adapter_fails_closed_when_encrypting_without_keyring() -> None:
    adapter = InngestEventCommandAdapter(
        client=_CapturingClient(),  # type: ignore[arg-type]
        spec=_spec(encrypt=True),
        execution_ctx=None,
        include_execution_context=False,
        cipher=None,
    )

    with pytest.raises(CoreException) as ei:
        await adapter.send(_Payload(n=1))

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.durable.encryption_wiring"


@pytest.mark.asyncio
async def test_tenant_sealed_event_round_trips_without_execution_context() -> None:
    """Regression: ``encrypt=True`` + ``include_execution_context=False`` under a real
    tenant must still carry the tenant so the receive side can rebuild the AAD and key.

    Before the fix the tenant-bearing ``_forze`` envelope was dropped, leaving the AAD
    (and key) reconstructed under ``tenant=None`` on the receive side — a wrong-key /
    ``aead_auth_failed`` decryption failure for every tenant-scoped encrypted event.
    """

    keyring = _keyring()
    tenant = TenantIdentity(tenant_id=uuid4())

    client = _CapturingClient()
    ctx = ExecutionContext(deps=frozen_deps_from_deps(Deps.plain({})))

    adapter = InngestEventCommandAdapter(
        client=client,  # type: ignore[arg-type]
        spec=_spec(encrypt=True),
        execution_ctx=ctx,
        include_execution_context=False,
        cipher=keyring,
    )

    with ctx.inv_ctx.bind_identity(tenant=tenant):
        await adapter.send(_Payload(n=42))

    [event] = client.events

    # The receive path: split the envelope, then open under the recovered tenant.
    envelope, payload = split_envelope(event.data)
    assert envelope.tenant == tenant  # tenant must travel with the ciphertext
    assert is_encrypted_payload(payload)  # business payload is ciphertext
    assert "n" not in payload

    opened = await open_event_payload(keyring, payload, tenant=envelope.tenant)
    assert opened == {"n": 42}


@pytest.mark.asyncio
async def test_plaintext_spec_sends_plaintext() -> None:
    client = _CapturingClient()
    adapter = InngestEventCommandAdapter(
        client=client,  # type: ignore[arg-type]
        spec=_spec(encrypt=False),
        execution_ctx=None,
        include_execution_context=False,
        cipher=_keyring(),
    )

    await adapter.send(_Payload(n=3))

    [event] = client.events
    assert event.data == {"n": 3}
