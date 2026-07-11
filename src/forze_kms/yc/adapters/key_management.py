""":class:`~forze.application.contracts.crypto.KeyManagementPort` adapter for Yandex Cloud KMS.

Yandex Cloud KMS is *not* AWS-API-compatible (only Yandex Object Storage speaks the
S3 API), but it does have a native data-key API, so — like AWS KMS and unlike GCP —
the backend mints the data key: ``generate_data_key`` maps to
``SymmetricCrypto.GenerateDataKey`` and ``unwrap_data_key`` to
``SymmetricCrypto.Decrypt``. The key-encryption key never leaves KMS.

:class:`~forze.application.contracts.crypto.KeyRef.key_id` is a Yandex Cloud
symmetric key id. Key rotation is transparent — the wrapped blob names its own key
version, so a data key wrapped before a rotation still decrypts afterwards and
``DataKey.key_version`` is ``None``. Wire it with a keyring, e.g.::

    Keyring(
        kms=YcKmsKeyManagement(client=kms_client),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="abj...")),
    )
"""

from typing import final

import attrs

from forze.application.contracts.crypto import DataKey, KeyRef
from forze.base.exceptions import exc

from ..kernel.client import YcKmsClientPort

# ----------------------- #

_ALGORITHMS: dict[int, str] = {16: "AES_128", 32: "AES_256"}
"""DEK byte length → Yandex Cloud ``SymmetricAlgorithm`` name. AES-256 (32 bytes) is
the default and matches the keyring's ``AesGcmAead``."""


# ....................... #


@final
@attrs.define(slots=True)
class YcKmsKeyManagement:
    """Envelope key management backed by Yandex Cloud KMS."""

    client: YcKmsClientPort
    """Yandex Cloud KMS client wrapping ``GenerateDataKey`` / ``Decrypt``."""

    dek_bytes: int = 32
    """Data-key length in bytes (32 → AES-256, the AEAD default; 16 → AES-128)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.dek_bytes not in _ALGORITHMS:
            raise exc.configuration(
                f"YC KMS data-key length must be one of "
                f"{sorted(_ALGORITHMS)} bytes",
                code="core.crypto.dek_length_unsupported",
            )

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        plaintext, ciphertext = await self.client.generate_data_key(
            key_ref.key_id,
            algorithm=_ALGORITHMS[self.dek_bytes],
        )

        return DataKey(
            plaintext=plaintext,
            wrapped=ciphertext,
            key_id=key_ref.key_id,
            key_version=None,  # rotation is transparent; the blob self-describes
        )

    # ....................... #

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return await self.client.decrypt(key_ref.key_id, wrapped)
