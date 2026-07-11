"""Dependency keys for Yandex Cloud KMS services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import YcKmsClientPort

# ----------------------- #

YcKmsClientDepKey: DepKey[YcKmsClientPort] = DepKey("yckms_client")
"""Key used to register a Yandex Cloud KMS client in the deps container."""
