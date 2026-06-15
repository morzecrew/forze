"""Encrypting Temporal ``PayloadCodec`` — seals workflow/activity payloads at rest.

Temporal's :class:`PayloadCodec` is the native seam for payload encryption: the SDK runs
``encode`` before a payload leaves the client and ``decode`` after it arrives, so workflow
inputs/outputs, signals, queries, and activity args are sealed in the Temporal datastore
and on the wire while staying transparent to handlers.

The codec is **context-free** (the SDK hands it raw payloads with no workflow/tenant
context), so it uses the deployment's single/default key (``tenant=None``) — still BYOK
with the KEK held in the backend. The self-describing :class:`EncryptedEnvelope` lets
``decode`` resolve the key with no ambient context. The whole inner ``Payload`` is sealed,
so its original encoding metadata survives the round-trip; a payload we did not encrypt
(legacy / non-Forze producer) passes through untouched.
"""

import dataclasses
from collections.abc import Sequence

from temporalio.api.common.v1 import Payload
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.converter import DataConverter, PayloadCodec

from forze.application.contracts.crypto import BytesCipherPort

# ----------------------- #

_ENCODING = b"binary/forze-encrypted"
"""Marker on a sealed payload's ``encoding`` metadata so ``decode`` recognizes ours."""

_DURABLE_AAD = b"forze.durable"
"""Fixed associated data binding the ciphertext to the durable-execution domain."""


class EncryptingPayloadCodec(PayloadCodec):
    """Seals each Temporal ``Payload`` with the keyring; decodes our own back."""

    def __init__(self, cipher: BytesCipherPort) -> None:
        self._cipher = cipher

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []

        for payload in payloads:
            sealed = await self._cipher.encrypt(
                payload.SerializeToString(), tenant=None, aad=_DURABLE_AAD
            )
            out.append(Payload(metadata={"encoding": _ENCODING}, data=sealed))

        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []

        for payload in payloads:
            if payload.metadata.get("encoding") != _ENCODING:
                # Not ours (plaintext / legacy / non-Forze producer): pass through.
                out.append(payload)
                continue

            raw = await self._cipher.decrypt(payload.data, aad=_DURABLE_AAD)
            inner = Payload()
            inner.ParseFromString(raw)
            out.append(inner)

        return out


# ....................... #


def encrypting_data_converter(
    cipher: BytesCipherPort, *, base: DataConverter | None = None
) -> DataConverter:
    """A ``DataConverter`` that seals payloads via *cipher*, composed over *base*.

    *base* defaults to the pydantic data converter (matching the framework default);
    pass a custom converter to keep its payload/failure conversion while adding encryption.
    """

    return dataclasses.replace(
        base or pydantic_data_converter, payload_codec=EncryptingPayloadCodec(cipher)
    )
