from enum import StrEnum

# ----------------------- #


class AuthnResourceName(StrEnum):
    """Authn resource name."""

    PASSWORD_ACCOUNTS = "authn_password_accounts"  # nosec B105
    API_KEY_ACCOUNTS = "authn_api_key_accounts"
    TOKEN_SESSIONS = "authn_token_sessions"  # nosec B105
    PRINCIPALS = "authn_principals"
    IDENTITY_MAPPINGS = "authn_identity_mappings"


# Document spec names used during authentication bootstrap. These reads must not rely on
# ``tenant_aware=True`` predicate injection before :class:`~forze.application.contracts.tenancy.TenantIdentity`
# is bound (configure Postgres/Mongo deps accordingly).
AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES = frozenset({
    AuthnResourceName.PASSWORD_ACCOUNTS,
    AuthnResourceName.API_KEY_ACCOUNTS,
    AuthnResourceName.TOKEN_SESSIONS,
    AuthnResourceName.PRINCIPALS,
    AuthnResourceName.IDENTITY_MAPPINGS,
})
