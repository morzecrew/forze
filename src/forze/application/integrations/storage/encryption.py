"""Shared encryption-floor validation for object-storage deps modules (S3 / GCS).

Object stores can only do whole-object (``envelope``) encryption, so a route's
derived tier is ``envelope`` when ``encrypt`` is set and ``none`` otherwise. This
enforces a declared ``required_encryption`` floor per route, failing closed at
wiring time — the encryption analog of
:func:`~forze.application.integrations.storage.validate_storage_tenancy_wiring`.
"""

from typing import Mapping, Protocol

from forze.application.contracts.crypto import (
    EncryptionTier,
    validate_required_encryption,
)
from forze.base.primitives import StrKey, StrKeyMapping

# ----------------------- #


class _EncryptableRouteConfig(Protocol):
    """Structural storage config exposing the client-side encryption flag."""

    @property
    def encrypt(self) -> bool: ...


# ....................... #


def validate_storage_encryption_wiring(
    *,
    integration: str,
    storages: (
        StrKeyMapping[_EncryptableRouteConfig]
        | Mapping[StrKey, _EncryptableRouteConfig]
        | None
    ),
    required_encryption: EncryptionTier | None,
    validation_failed_code: str,
) -> None:
    """Fail closed when a storage route's encryption is weaker than the floor."""

    if required_encryption is None:
        return

    for name, config in (storages or {}).items():
        validate_required_encryption(
            integration=f"{integration} storage route {name!r}",
            derived="envelope" if config.encrypt else "none",
            required=required_encryption,
            code=validation_failed_code,
            max_supported="envelope",
        )
