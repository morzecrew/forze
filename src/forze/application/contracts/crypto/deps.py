"""Dependency keys for crypto ports."""

from forze.base.crypto import Aead

from ..base import EncryptionReach
from ..deps import DepKey
from .directory import KeyDirectoryPort
from .ports import DeterministicFieldCipherPort, KeyManagementPort, KeyringPort

# ----------------------- #

KeyManagementDepKey = DepKey[KeyManagementPort]("crypto.kms")
"""Key used to register a :class:`KeyManagementPort` (key backend / KMS)."""

# ....................... #

AeadDepKey = DepKey[Aead]("crypto.aead")
"""Key used to register the local :class:`~forze.base.crypto.Aead` cipher."""

# ....................... #

KeyDirectoryDepKey = DepKey[KeyDirectoryPort]("crypto.key_directory")
"""Key used to register a :class:`KeyDirectoryPort` (tenant → key resolution)."""

# ....................... #

KeyringDepKey = DepKey[KeyringPort]("crypto.keyring")
"""Key used to register the keyring (async value cipher + sync field cipher)."""

# ....................... #

DeterministicCipherDepKey = DepKey[DeterministicFieldCipherPort]("crypto.deterministic")
"""Key used to register the deterministic (searchable) field cipher."""

# ....................... #

RequiredReachDepKey = DepKey[EncryptionReach]("crypto.required_reach")
"""Key registering a deployment-wide minimum encryption *reach* (the ``required_reach``
floor). Present only when a deployment declares it (via ``CryptoDepsModule``); absent
means no floor. Read at messaging-encryption resolve points to fail closed against any
outbox/transport route whose declared reach is weaker."""
