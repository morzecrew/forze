"""Integration tests for Yandex Cloud KMS envelope key management (real service).

Skipped unless Yandex Cloud credentials are supplied — see this package's
``conftest`` for the environment variables. There is no Yandex Cloud KMS emulator.
"""

import pytest

pytest.importorskip("yandexcloud")

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze_kms.yc import YcKmsClient, YcKmsKeyManagement

# ----------------------- #


@pytest.fixture
def kms(yc_kms_client: YcKmsClient) -> YcKmsKeyManagement:
    return YcKmsKeyManagement(client=yc_kms_client)


# ----------------------- #


@pytest.mark.integration
@pytest.mark.yc_kms
async def test_generate_then_unwrap_round_trip(
    kms: YcKmsKeyManagement, yc_key_id: str
) -> None:
    data_key = await kms.generate_data_key(KeyRef(key_id=yc_key_id))

    assert len(data_key.plaintext) == 32  # AES-256 data key
    assert data_key.wrapped  # wrapped blob only the KMS key can decrypt
    # Yandex Cloud reports the wrapping version, so it is carried in the envelope.
    # Decrypt never needs it (it reads the version from the ciphertext), so rotation
    # stays transparent — the round-trip below proves the unwrap ignores it.
    assert data_key.key_version

    recovered = await kms.unwrap_data_key(
        wrapped=data_key.wrapped,
        key_ref=KeyRef(key_id=yc_key_id),
    )

    assert recovered == data_key.plaintext


# ....................... #


@pytest.mark.integration
@pytest.mark.yc_kms
async def test_full_keyring_round_trip_against_kms(
    kms: YcKmsKeyManagement, yc_key_id: str
) -> None:
    keyring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id=yc_key_id)),
    )

    blob = await keyring.encrypt(b"sensitive payload", tenant=None, aad=b"ctx")

    assert await keyring.decrypt(blob, aad=b"ctx") == b"sensitive payload"
    # A mismatched AAD must not authenticate.
    with pytest.raises(CoreException):
        await keyring.decrypt(blob, aad=b"other")
