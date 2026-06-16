"""Application-layer encrypting decorator for direct stream appends.

Wraps any :class:`StreamCommandPort` so an ``encryption="end_to_end"`` :class:`StreamSpec`
seals each appended payload into a one-key envelope (shared messaging-plane domain) before
it reaches the backend. The stream codec forwards the wrapper opaquely and a consumer
decrypts it from the ``event_id``/``tenant`` headers — the same path the outbox relay uses.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import cast, final

import attrs

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.stream import StreamCommandPort, StreamSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.integrations.crypto import (
    MESSAGE_PAYLOAD_DOMAIN,
    seal_message_payload,
)
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EncryptingStreamCommand[M](StreamCommandPort[M]):
    """Seals each payload before delegating to the wrapped stream command."""

    inner: StreamCommandPort[M]
    spec: StreamSpec[M]
    cipher: BytesCipherPort = attrs.field(repr=False)
    tenant_provider: TenantProviderPort

    # ....................... #

    async def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        tenant = self.tenant_provider()
        wrapper, sealed_headers = await seal_message_payload(
            self.cipher,
            self.spec.codec,
            payload,
            domain=MESSAGE_PAYLOAD_DOMAIN,
            tenant_id=None if tenant is None else tenant.tenant_id,
            headers=headers,
        )
        return await self.inner.append(
            stream,
            cast(M, wrapper),
            type=type,
            key=key,
            timestamp=timestamp,
            headers=sealed_headers,
        )


# ....................... #


def encrypting_stream_command[M](
    inner: StreamCommandPort[M],
    spec: StreamSpec[M],
    *,
    cipher: BytesCipherPort | None,
    tenant_provider: TenantProviderPort,
) -> StreamCommandPort[M]:
    """Wrap *inner* with payload encryption when *spec* declares it; else return it as-is.

    Fail-closed: a route that declares ``encryption`` but resolves no keyring raises rather
    than silently appending plaintext.
    """

    if not spec.encrypts:
        return inner

    if cipher is None:
        raise exc.configuration(
            f"Stream route {spec.name!r} declares encryption but no keyring is wired to "
            "seal its payloads. Register a CryptoDepsModule or set encryption='none'.",
            code="core.stream.encryption_wiring",
        )

    return EncryptingStreamCommand(
        inner=inner, spec=spec, cipher=cipher, tenant_provider=tenant_provider
    )
