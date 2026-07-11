"""Dependency module for the Yandex Cloud KMS client and key-management adapter."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyManagementDepKey, KeyManagementPort
from forze.application.contracts.deps import Deps, DepKey, DepsModule

from ...adapters import YcKmsKeyManagement
from ...kernel.client import YcKmsClientPort
from .keys import YcKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcKmsDepsModule(DepsModule):
    """Register the Yandex Cloud KMS client and an envelope key-management adapter.

    Give it a :class:`~forze_kms.yc.kernel.client.YcKmsClient` (initialized via
    :func:`~forze_kms.yc.execution.yckms_lifecycle_step`); the key-management adapter
    defaults to :class:`~forze_kms.yc.adapters.YcKmsKeyManagement` over that client and
    is registered under ``KeyManagementDepKey`` so a
    :class:`~forze.application.execution.CryptoDepsModule` can compose the keyring on
    top of it.
    """

    client: YcKmsClientPort
    """Pre-constructed Yandex Cloud KMS client."""

    key_management: KeyManagementPort | None = attrs.field(default=None)
    """Optional envelope key manager; defaults to
    :class:`~forze_kms.yc.adapters.YcKmsKeyManagement` over :attr:`client`."""

    # ....................... #

    def __call__(self) -> Deps:
        adapter = (
            self.key_management
            if self.key_management is not None
            else YcKmsKeyManagement(client=self.client)
        )

        deps: dict[DepKey[Any], Any] = {
            YcKmsClientDepKey: self.client,
            KeyManagementDepKey: adapter,
        }

        return Deps.plain(deps)
