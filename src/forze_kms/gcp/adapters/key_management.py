""":class:`~forze.application.contracts.crypto.KeyManagementPort` adapter for GCP KMS.

GCP KMS has no ``GenerateDataKey`` primitive, so ``generate_data_key`` mints the
data key **client-side** from the CSPRNG secret-entropy seam
(:func:`~forze.base.primitives.secure_random_bytes`) and wraps it with the KMS
``Encrypt`` API; ``unwrap_data_key`` maps to ``Decrypt``. The key-encryption key
(the CryptoKey) never leaves KMS.

:class:`~forze.application.contracts.crypto.KeyRef.key_id` is a CryptoKey
resource name (``projects/…/locations/…/keyRings/…/cryptoKeys/…``). KMS key
rotation is transparent — ``Decrypt`` selects the version from the ciphertext, so
a data key wrapped before a rotation still decrypts afterwards and
``DataKey.key_version`` is ``None``. Wire it with a keyring, e.g.::

    Keyring(
        kms=GcpKmsKeyManagement(client=kms_client),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="projects/p/locations/global/keyRings/r/cryptoKeys/k")),
    )
"""

from typing import final

import attrs

from forze.application.contracts.crypto import DataKey, KeyRef
from forze.base.exceptions import exc
from forze.base.primitives import secure_random_bytes

from ..kernel.client import GcpKmsClientPort

# ----------------------- #

_ALLOWED_DEK_BYTES = frozenset({16, 32})
"""Data-key lengths matching the keyring's AEAD (32 → AES-256, 16 → AES-128)."""


# ....................... #


@final
@attrs.define(slots=True)
class GcpKmsKeyManagement:
    """Envelope key management backed by GCP KMS (client-side data-key generation)."""

    client: GcpKmsClientPort
    """GCP KMS client wrapping ``Encrypt`` / ``Decrypt``."""

    dek_bytes: int = 32
    """Data-key length in bytes (32 → AES-256, the AEAD default; 16 → AES-128)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.dek_bytes not in _ALLOWED_DEK_BYTES:
            raise exc.configuration(
                f"GCP KMS data-key length must be one of "
                f"{sorted(_ALLOWED_DEK_BYTES)} bytes",
                code="core.crypto.dek_length_unsupported",
            )

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        # GCP KMS has no GenerateDataKey: mint the DEK from the CSPRNG secret seam
        # (never a replayable source), then wrap it under the CryptoKey.
        plaintext = secure_random_bytes(self.dek_bytes)
        wrapped = await self.client.encrypt(key_ref.key_id, plaintext)

        return DataKey(
            plaintext=plaintext,
            wrapped=wrapped,
            key_id=key_ref.key_id,
            key_version=None,  # KMS rotation is transparent; Decrypt self-selects
        )

    # ....................... #

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return await self.client.decrypt(key_ref.key_id, wrapped)
