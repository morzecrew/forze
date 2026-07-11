"""Dependency module for the GCP KMS client and key-management adapter."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyManagementDepKey, KeyManagementPort
from forze.application.contracts.deps import Deps, DepKey, DepsModule

from ...adapters import GcpKmsKeyManagement
from ...kernel.client import GcpKmsClientPort
from .keys import GcpKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GcpKmsDepsModule(DepsModule):
    """Register the GCP KMS client and an envelope key-management adapter.

    Give it a :class:`~forze_kms.gcp.kernel.client.GcpKmsClient` (initialized via
    :func:`~forze_kms.gcp.execution.gcpkms_lifecycle_step`); the key-management
    adapter defaults to :class:`~forze_kms.gcp.adapters.GcpKmsKeyManagement` over
    that client and is registered under ``KeyManagementDepKey`` so a
    :class:`~forze.application.execution.CryptoDepsModule` can compose the keyring
    on top of it.
    """

    client: GcpKmsClientPort
    """Pre-constructed GCP KMS client."""

    key_management: KeyManagementPort | None = attrs.field(default=None)
    """Optional envelope key manager; defaults to
    :class:`~forze_kms.gcp.adapters.GcpKmsKeyManagement` over :attr:`client`."""

    # ....................... #

    def __call__(self) -> Deps:
        adapter = (
            self.key_management
            if self.key_management is not None
            else GcpKmsKeyManagement(client=self.client)
        )

        deps: dict[DepKey[Any], Any] = {
            GcpKmsClientDepKey: self.client,
            KeyManagementDepKey: adapter,
        }

        return Deps.plain(deps)
