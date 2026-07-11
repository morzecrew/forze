"""Dependency module for the AWS KMS client and key-management adapter."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyManagementDepKey, KeyManagementPort
from forze.application.contracts.deps import Deps, DepKey, DepsModule

from ...adapters import AwsKmsKeyManagement
from ...kernel.client import AwsKmsClientPort
from .keys import AwsKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AwsKmsDepsModule(DepsModule):
    """Register the AWS KMS client and an envelope key-management adapter.

    Give it an :class:`~forze_kms.aws.kernel.client.AwsKmsClient` (initialized via
    :func:`~forze_kms.aws.execution.awskms_lifecycle_step`); the key-management
    adapter defaults to :class:`~forze_kms.aws.adapters.AwsKmsKeyManagement` over
    that client and is registered under ``KeyManagementDepKey`` so a
    :class:`~forze.application.execution.CryptoDepsModule` can compose the keyring
    on top of it.
    """

    client: AwsKmsClientPort
    """Pre-constructed AWS KMS client."""

    key_management: KeyManagementPort | None = attrs.field(default=None)
    """Optional envelope key manager; defaults to
    :class:`~forze_kms.aws.adapters.AwsKmsKeyManagement` over :attr:`client`."""

    # ....................... #

    def __call__(self) -> Deps:
        adapter = (
            self.key_management
            if self.key_management is not None
            else AwsKmsKeyManagement(client=self.client)
        )

        deps: dict[DepKey[Any], Any] = {
            AwsKmsClientDepKey: self.client,
            KeyManagementDepKey: adapter,
        }

        return Deps.plain(deps)
