"""Secrets lifecycle kits: poll watcher, file source, hot-reload binder, rotation
notifications, the durable rotator, and the lease manager.

The consuming half (fingerprint dedup, ``evict_tenant``, ``fingerprint_ttl``) ships
in the routed-client base; these kits produce and route the signals it waits for.
Posture throughout: **signals accelerate, the TTL floor guarantees.**
"""

from .binder import SecretsHotReloadBinder
from .file_source import DirectorySecretsChangeSource
from .lease_manager import SecretsLeaseManager
from .notify import (
    DEFAULT_SECRET_ROTATIONS_CHANNEL,
    PubSubSecretsChangeSource,
    publish_secret_rotated,
    secret_rotated_outbox_spec,
    secret_rotated_pubsub_spec,
)
from .rotator import (
    PENDING_SUFFIX,
    ROTATE_FUNCTION_NAME,
    RotationInput,
    SecretRotator,
    pending_ref_for,
)
from .watcher import DEFAULT_SECRETS_WATCH_INTERVAL, SecretsPollWatcher

# ----------------------- #

__all__ = [
    "DEFAULT_SECRET_ROTATIONS_CHANNEL",
    "DEFAULT_SECRETS_WATCH_INTERVAL",
    "PENDING_SUFFIX",
    "ROTATE_FUNCTION_NAME",
    "DirectorySecretsChangeSource",
    "PubSubSecretsChangeSource",
    "RotationInput",
    "SecretRotator",
    "SecretsHotReloadBinder",
    "SecretsLeaseManager",
    "SecretsPollWatcher",
    "pending_ref_for",
    "publish_secret_rotated",
    "secret_rotated_outbox_spec",
    "secret_rotated_pubsub_spec",
]
