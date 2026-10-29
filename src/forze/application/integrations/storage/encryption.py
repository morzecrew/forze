"""Shared encryption-floor validation for object-storage deps modules (S3 / GCS).

Object stores can only do whole-object (``envelope``) encryption, so a route's
derived tier is ``envelope`` when ``encrypt`` is set and ``none`` otherwise. This
enforces a declared ``required_encryption`` floor per route, failing closed at
wiring time — the encryption analog of
:func:`~forze.application.integrations.storage.validate_storage_tenancy_wiring`.
"""

from collections.abc import Mapping
from typing import Protocol

from forze.application.contracts.crypto import (
    EncryptionTier,
    validate_required_encryption,
)
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import exc
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
        StrKeyMapping[_EncryptableRouteConfig] | Mapping[StrKey, _EncryptableRouteConfig] | None
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


# ....................... #


def refuse_direct_transfer_when_encrypted(spec: StorageSpec, *, encrypt: bool) -> None:
    """Refuse a :attr:`~StorageSpec.direct_transfer` spec on a client-side-encrypting route.

    Called by each backend's port factory, where the spec meets the route's config, so the
    refusal comes when the port is resolved rather than when a URL is first asked for.

    :raises CoreException: ``configuration`` (``storage_direct_transfer_encrypted``).
    """

    if spec.direct_transfer and encrypt:
        raise exc.configuration(
            f"Storage route {str(spec.name)!r} encrypts client-side, so it cannot serve "
            "presigned URLs or upload sessions: their bytes reach the store without passing "
            "through the application, which cannot encrypt them. Route direct transfers to a "
            "route without encrypt=True (server-side encryption is compatible).",
            code="storage_direct_transfer_encrypted",
        )
