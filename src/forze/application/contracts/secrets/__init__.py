"""Contracts for async secret resolution (KV-style wire format) and the secrets
lifecycle plane: versioned reads, control-plane writes, the change feed, rotation
targets, and leased dynamic credentials."""

from .admin import SecretsAdminPort
from .capabilities import (
    DEFAULT_SECRETS_CAPABILITIES,
    FULL_SECRETS_CAPABILITIES,
    UNSUPPORTED_SECRETS_FEATURE_CODE,
    SecretsCapabilities,
    secrets_capabilities_of,
    validate_dynamic_credentials_supported,
    validate_secret_writes_supported,
    validate_versioned_reads_supported,
)
from .changes import (
    SECRET_ROTATED_EVENT_TYPE,
    SecretChanged,
    SecretRotated,
    SecretsChangeSource,
)
from .deps import (
    RotatingCredentialsAdminDepKey,
    RotatingCredentialsDepKey,
    SecretsAdminDepKey,
    SecretsDepKey,
    SecretsLeaseDepKey,
)
from .lease import DynamicSecretsPort, LeasedSecret
from .ports import SecretsPort
from .resolution import (
    TenantSecretResolver,
    resolve_str_for_tenant,
    resolve_structured,
    secret_ref_for_tenant,
)
from .rotating import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    DueCredential,
    ExchangedCredential,
    RotatingCredential,
    RotatingCredentialsAdminPort,
    RotatingCredentialStorePort,
)
from .rotation import PendingCredential, RotationTargetPort
from .value_objects import SecretRef
from .versioning import (
    SecretValue,
    SecretVersion,
    VersionedSecretsPort,
    content_secret_version,
)

# ----------------------- #

__all__ = [
    "SecretsDepKey",
    "SecretsAdminDepKey",
    "SecretsLeaseDepKey",
    "SecretsPort",
    "SecretRef",
    "TenantSecretResolver",
    "resolve_structured",
    "secret_ref_for_tenant",
    "resolve_str_for_tenant",
    # capabilities
    "DEFAULT_SECRETS_CAPABILITIES",
    "FULL_SECRETS_CAPABILITIES",
    "UNSUPPORTED_SECRETS_FEATURE_CODE",
    "SecretsCapabilities",
    "secrets_capabilities_of",
    "validate_dynamic_credentials_supported",
    "validate_secret_writes_supported",
    "validate_versioned_reads_supported",
    # versioned reads
    "SecretValue",
    "SecretVersion",
    "VersionedSecretsPort",
    "content_secret_version",
    # control-plane writes
    "SecretsAdminPort",
    # change feed
    "SECRET_ROTATED_EVENT_TYPE",
    "SecretChanged",
    "SecretRotated",
    "SecretsChangeSource",
    # rotation
    "PendingCredential",
    "RotationTargetPort",
    # leases
    "DynamicSecretsPort",
    "LeasedSecret",
    # counterparty-rotated credentials
    "DueCredential",
    "RotatingCredentialsAdminDepKey",
    "RotatingCredentialsDepKey",
    "BURNT_CREDENTIAL_CODE",
    "CREDENTIAL_EXCHANGE_TIMEOUT_CODE",
    "CREDENTIAL_PERSIST_LOST_CODE",
    "INVALID_GRANT_CODE",
    "CredentialExchangerPort",
    "ExchangedCredential",
    "RotatingCredential",
    "RotatingCredentialsAdminPort",
    "RotatingCredentialStorePort",
]
