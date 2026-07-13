"""Dependency module for the Yandex Cloud KMS client and key-management adapter."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyManagementDepKey, KeyManagementPort
from forze.application.contracts.deps import DepKey, Deps, DepsModule

from ...kernel.client import YcKmsClientPort
from .keys import YcKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcKmsDepsModule(DepsModule):
    """Register the Yandex Cloud KMS client (and, optionally, an envelope key manager).

    Give it a :class:`~forze_kms.yc.kernel.client.YcKmsClient` (initialized via
    :func:`~forze_kms.yc.execution.yckms_lifecycle_step`), and it registers the client
    under ``YcKmsClientDepKey`` so the lifecycle step can initialize it.

    Compose the keyring with :class:`~forze.application.execution.CryptoDepsModule`,
    passing :class:`~forze_kms.yc.adapters.YcKmsKeyManagement` as its ``kms`` — that
    module registers ``KeyManagementDepKey`` itself, so leave :attr:`key_management`
    unset unless you are wiring the port without a keyring.
    """

    client: YcKmsClientPort
    """Pre-constructed Yandex Cloud KMS client."""

    key_management: KeyManagementPort | None = attrs.field(default=None)
    """Optional envelope key manager, registered under ``KeyManagementDepKey`` only when
    set. Leave it unset when a ``CryptoDepsModule`` supplies the port (the usual wiring) —
    registering it in both places is a conflicting dependency."""

    # ....................... #

    def __call__(self) -> Deps:
        deps: dict[DepKey[Any], Any] = {YcKmsClientDepKey: self.client}

        if self.key_management is not None:
            deps[KeyManagementDepKey] = self.key_management

        return Deps.plain(deps)
