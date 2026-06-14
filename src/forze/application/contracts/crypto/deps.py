"""Dependency keys for crypto ports."""

from forze.base.crypto import Aead

from ..deps import DepKey
from .ports import KeyManagementPort

# ----------------------- #

KeyManagementDepKey = DepKey[KeyManagementPort]("crypto.kms")
"""Key used to register a :class:`KeyManagementPort` (key backend / KMS)."""

# ....................... #

AeadDepKey = DepKey[Aead]("crypto.aead")
"""Key used to register the local :class:`~forze.base.crypto.Aead` cipher."""
