from enum import StrEnum

# ----------------------- #


class AuthnResourceName(StrEnum):
    """Authn resource name."""

    PASSWORD_ACCOUNTS = "authn_password_accounts"  # nosec B105
    API_KEY_ACCOUNTS = "authn_api_key_accounts"  # nosec B105
    TOKEN_SESSIONS = "authn_token_sessions"  # nosec B105
    PRINCIPALS = "authn_principals"
