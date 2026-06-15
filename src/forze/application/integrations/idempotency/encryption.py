"""At-rest encryption for the idempotency result cache.

The idempotency store replays an operation's cached **result** for a duplicate request — so a
Forze-owned store (Redis/Postgres) holds the full return value of the operation, potentially
sensitive business data, in plaintext. :class:`EncryptingIdempotencyPort` wraps the resolved
port to seal that result on commit and open it on replay, leaving the status/hash metadata
plaintext. The bytes are sealed directly as a packed envelope (the result is opaque bytes, not
a JSON payload), with the AAD bound to ``(tenant, op:key)``; records written before encryption
was enabled still replay (envelope sniff).
"""

from collections.abc import Callable
from typing import final

import attrs

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.idempotency import IdempotencyPort, IdempotencyRecord
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import payload_aad
from forze.base.crypto import is_envelope
from forze.base.exceptions import exc

# ----------------------- #

IDEMPOTENCY_PAYLOAD_DOMAIN = "idempotency"
"""AAD domain isolating idempotency-result ciphertext from other contexts."""

_WIRING_CODE = "core.idempotency.encryption_wiring"


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EncryptingIdempotencyPort:
    """Seal an idempotent operation's cached result at rest, transparent to the hook."""

    inner: IdempotencyPort
    cipher: BytesCipherPort
    tenant_provider: Callable[[], TenantIdentity | None]

    # ....................... #

    async def begin(
        self, op: str, key: str | None, payload_hash: str
    ) -> IdempotencyRecord | None:
        record = await self.inner.begin(op, key, payload_hash)

        if record is None or key is None:
            return record

        return IdempotencyRecord(result=await self._open(op, key, record.result))

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> None:
        if key is not None:
            record = IdempotencyRecord(result=await self._seal(op, key, record.result))

        await self.inner.commit(op, key, payload_hash, record)

    # ....................... #

    async def fail(self, op: str, key: str | None, payload_hash: str) -> None:
        await self.inner.fail(op, key, payload_hash)

    # ....................... #

    def _aad(self, op: str, key: str, *, tenant: TenantIdentity | None) -> bytes:
        tenant_id = tenant.tenant_id if tenant is not None else None
        # Length-prefix the op so the (op, key) boundary is unambiguous: a naive ``f"{op}:{key}"``
        # collides — ("a:b", "c") and ("a", "b:c") both render "a:b:c", letting a ciphertext open
        # under a different (op, key). ``{len(op)}:{op}:{key}`` cannot.
        record_id = f"{len(op)}:{op}:{key}"
        return payload_aad(IDEMPOTENCY_PAYLOAD_DOMAIN, tenant_id, record_id)

    async def _seal(self, op: str, key: str, result: bytes) -> bytes:
        # Resolve the tenant once and thread it to both key selection and the AAD, so the two
        # cannot diverge (consistent with the other sealing sites).
        tenant = self.tenant_provider()
        return await self.cipher.encrypt(
            result, tenant=tenant, aad=self._aad(op, key, tenant=tenant)
        )

    async def _open(self, op: str, key: str, result: bytes) -> bytes:
        if not is_envelope(result):
            return result  # legacy plaintext record — replay as-is

        tenant = self.tenant_provider()
        return await self.cipher.decrypt(result, aad=self._aad(op, key, tenant=tenant))


# ....................... #


def encrypting_idempotency_port(
    inner: IdempotencyPort,
    *,
    cipher: BytesCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
    spec_name: str,
) -> IdempotencyPort:
    """Wrap *inner* to seal cached results, fail-closed when no keyring is wired.

    Called only when the spec opts in (``encrypt_result=True``); a missing keyring is a
    misconfiguration (the spec asked to seal but nothing can), so this raises rather than
    silently caching plaintext.
    """

    if cipher is None:
        raise exc.configuration(
            f"IdempotencySpec {spec_name!r} sets encrypt_result=True but no keyring is "
            "wired. Register a CryptoDepsModule or clear encrypt_result.",
            code=_WIRING_CODE,
        )

    return EncryptingIdempotencyPort(
        inner=inner, cipher=cipher, tenant_provider=tenant_provider
    )
