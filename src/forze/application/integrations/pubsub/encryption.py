"""Application-layer encrypting decorator for direct pub-sub publishing.

Wraps any :class:`PubSubCommandPort` so an ``encryption="end_to_end"`` :class:`PubSubSpec`
seals each published payload into a one-key envelope (shared messaging-plane domain) before
it reaches the backend. The pub-sub codec forwards the wrapper opaquely and a subscriber
decrypts it from the ``event_id``/``tenant`` headers — the same path the outbox relay uses.

Pub-sub is at-most-once: a message published while no subscriber is listening is lost,
encrypted or not — that is unchanged here.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import cast, final

import attrs

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.pubsub import PubSubCommandPort, PubSubSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze.application.integrations.crypto import (
    MESSAGE_PAYLOAD_DOMAIN,
    seal_message_payload,
)
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EncryptingPubSubCommand[M](PubSubCommandPort[M]):
    """Seals each payload before delegating to the wrapped pub-sub command."""

    inner: PubSubCommandPort[M]
    spec: PubSubSpec[M]
    cipher: BytesCipherPort = attrs.field(repr=False)
    tenant_provider: TenantProviderPort

    # ....................... #

    async def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        tenant = self.tenant_provider()
        wrapper, sealed_headers = await seal_message_payload(
            self.cipher,
            self.spec.codec,
            payload,
            domain=MESSAGE_PAYLOAD_DOMAIN,
            tenant_id=None if tenant is None else tenant.tenant_id,
            headers=headers,
        )
        await self.inner.publish(
            topic,
            cast(M, wrapper),
            type=type,
            key=key,
            published_at=published_at,
            headers=sealed_headers,
        )


# ....................... #


def encrypting_pubsub_command[M](
    inner: PubSubCommandPort[M],
    spec: PubSubSpec[M],
    *,
    cipher: BytesCipherPort | None,
    tenant_provider: TenantProviderPort,
) -> PubSubCommandPort[M]:
    """Wrap *inner* with payload encryption when *spec* declares it; else return it as-is.

    Fail-closed: a route that declares ``encryption`` but resolves no keyring raises rather
    than silently publishing plaintext.
    """

    if not spec.encrypts:
        return inner

    if cipher is None:
        raise exc.configuration(
            f"Pub-sub route {spec.name!r} declares encryption but no keyring is wired to "
            "seal its payloads. Register a CryptoDepsModule or set encryption='none'.",
            code="core.pubsub.encryption_wiring",
        )

    return EncryptingPubSubCommand(
        inner=inner, spec=spec, cipher=cipher, tenant_provider=tenant_provider
    )
