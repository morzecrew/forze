"""Secrets lifecycle kits: poll watcher, file source, hot-reload binder, rotation
notifications, the durable rotator, the lease manager, and the outbound OAuth2 token
client that acquires a grant and then rotates it.

The consuming half (fingerprint dedup, ``evict_tenant``, ``fingerprint_ttl``) ships
in the routed-client base; these kits produce and route the signals it waits for.
Posture throughout: **signals accelerate, the TTL floor guarantees.**
"""

from .binder import SecretsHotReloadBinder
from .credential_sweep import (
    REFRESH_FUNCTION_NAME,
    SWEEP_FUNCTION_NAME,
    CredentialSweeper,
    SweepInput,
    SweepRefreshInput,
)
from .file_source import DirectorySecretsChangeSource
from .lease_manager import SecretsLeaseManager
from .notify import (
    DEFAULT_SECRET_ROTATIONS_CHANNEL,
    PubSubSecretsChangeSource,
    publish_secret_rotated,
    secret_rotated_outbox_spec,
    secret_rotated_pubsub_spec,
)
from .oauth_acquire import complete_authorization
from .oauth_client import (
    GRANTED_SCOPE_METADATA,
    REQUESTED_SCOPE_METADATA,
    TOKEN_OPERATION,
    OAuth2ErrorResponse,
    OAuth2ProviderConfig,
    OAuth2TokenClient,
    OAuth2TokenRequest,
    OAuth2TokenResponse,
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
    "DEFAULT_SECRETS_WATCH_INTERVAL",
    "GRANTED_SCOPE_METADATA",
    "OAuth2ErrorResponse",
    "OAuth2ProviderConfig",
    "OAuth2TokenClient",
    "OAuth2TokenRequest",
    "OAuth2TokenResponse",
    "REQUESTED_SCOPE_METADATA",
    "TOKEN_OPERATION",
    "complete_authorization",
    "DEFAULT_SECRET_ROTATIONS_CHANNEL",
    "PENDING_SUFFIX",
    "ROTATE_FUNCTION_NAME",
    "DirectorySecretsChangeSource",
    "PubSubSecretsChangeSource",
    "RotationInput",
    "REFRESH_FUNCTION_NAME",
    "SWEEP_FUNCTION_NAME",
    "CredentialSweeper",
    "SecretRotator",
    "SweepInput",
    "SweepRefreshInput",
    "SecretsHotReloadBinder",
    "SecretsLeaseManager",
    "SecretsPollWatcher",
    "pending_ref_for",
    "publish_secret_rotated",
    "secret_rotated_outbox_spec",
    "secret_rotated_pubsub_spec",
]
