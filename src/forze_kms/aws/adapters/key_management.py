""":class:`~forze.application.contracts.crypto.KeyManagementPort` adapter for AWS KMS.

Implements envelope key management on AWS KMS: the key-encryption key (the CMK)
never leaves KMS. ``generate_data_key`` maps to the KMS ``GenerateDataKey`` API
and ``unwrap_data_key`` to ``Decrypt``.

:class:`~forze.application.contracts.crypto.KeyRef.key_id` is the CMK identifier
— a key id, ARN, or ``alias/<name>``. KMS key rotation is transparent: the
wrapped blob is self-describing, so a data key wrapped before a rotation still
decrypts afterwards; ``DataKey.key_version`` is therefore ``None`` (KMS does not
surface a per-blob version). Wire it with a keyring, e.g.::

    Keyring(
        kms=AwsKmsKeyManagement(client=kms_client),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="alias/app-cmk")),
    )
"""

from typing import final

import attrs

from forze.application.contracts.crypto import DataKey, KeyRef
from forze.base.exceptions import exc

from ..kernel.client import AwsKmsClientPort

# ----------------------- #

_KEY_SPECS: dict[int, str] = {16: "AES_128", 32: "AES_256"}
"""DEK byte length → AWS ``KeySpec``. AES-256 (32 bytes) is the default and
matches the keyring's ``AesGcmAead``."""


# ....................... #


@final
@attrs.define(slots=True)
class AwsKmsKeyManagement:
    """Envelope key management backed by AWS KMS."""

    client: AwsKmsClientPort
    """AWS KMS client wrapping ``GenerateDataKey`` / ``Decrypt``."""

    dek_bytes: int = 32
    """Data-key length in bytes (32 → AES-256, the AEAD default; 16 → AES-128)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.dek_bytes not in _KEY_SPECS:
            raise exc.configuration(
                f"AWS KMS data-key length must be one of {sorted(_KEY_SPECS)} bytes",
                code="core.crypto.dek_length_unsupported",
            )

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        plaintext, ciphertext = await self.client.generate_data_key(
            key_ref.key_id,
            key_spec=_KEY_SPECS[self.dek_bytes],
        )

        return DataKey(
            plaintext=plaintext,
            wrapped=ciphertext,
            key_id=key_ref.key_id,
            key_version=None,  # KMS rotation is transparent; the blob self-describes
        )

    # ....................... #

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return await self.client.decrypt(wrapped, key_id=key_ref.key_id)
