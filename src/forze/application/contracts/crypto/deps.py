"""Dependency keys for crypto ports."""

from forze.base.crypto import Aead

from ..deps import DepKey
from .directory import KeyDirectoryPort
from .ports import BytesCipherPort, KeyManagementPort

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

KeyringDepKey = DepKey[BytesCipherPort]("crypto.keyring")
"""Key used to register the value-level :class:`BytesCipherPort` (keyring)."""
