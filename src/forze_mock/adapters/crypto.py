"""In-memory key manager stub for tests and local development.

.. danger::

   :class:`MockKeyManagement` is **test/dev only**. It derives key-encryption
   keys deterministically from public identifiers, so it provides no real
   confidentiality — never wire it in production. A production deployment uses a
   real key backend (a KMS / HSM / Vault transit engine).

The *cipher* is not mocked: the real :class:`~forze.base.crypto.AesGcmAead`
(or another :class:`~forze.base.crypto.Aead`) is wired alongside this, so only
the key backend is a test double. That keeps the local crypto path faithful
while needing zero infrastructure.
"""

from __future__ import annotations

import hashlib
import os
from typing import final

import attrs

from forze.application.contracts.crypto import DataKey, KeyRef
from forze.base.exceptions import exc

# ----------------------- #

_DEK_SIZE = 32  # 256-bit data keys (matches AES-256-GCM)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class MockKeyManagement:
    """Deterministic in-memory key manager. **Test/dev only** — never production.

    Key-encryption keys are derived from the public :class:`KeyRef` (so wrapping
    is reversible without stored state and survives serialization), which is
    exactly why it is insecure. Data keys themselves are random per call.
    """

    def _kek(self, key_ref: KeyRef) -> bytes:
        version = key_ref.version or "v1"
        material = f"mock-kek|{key_ref.key_id}|{version}".encode("utf-8")
        return hashlib.sha256(material).digest()

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        version = key_ref.version or "v1"
        plaintext = os.urandom(_DEK_SIZE)
        kek = self._kek(key_ref)
        wrapped = bytes(d ^ k for d, k in zip(plaintext, kek))
        return DataKey(
            plaintext=plaintext,
            wrapped=wrapped,
            key_id=key_ref.key_id,
            key_version=version,
        )

    # ....................... #

    async def unwrap_data_key(
        self,
        *,
        wrapped: bytes,
        key_ref: KeyRef,
    ) -> bytes:
        if len(wrapped) != _DEK_SIZE:
            raise exc.validation(
                "Wrapped data key has unexpected length",
                code="core.crypto.wrapped_key_invalid",
                details={"length": len(wrapped), "expected": _DEK_SIZE},
            )

        kek = self._kek(key_ref)
        return bytes(w ^ k for w, k in zip(wrapped, kek))
