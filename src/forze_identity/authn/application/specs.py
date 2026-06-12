from forze.application.contracts.document import DocumentSpec

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    CreatePasswordAccountCmd,
    PasswordAccount,
    ReadApiKeyAccount,
    ReadPasswordAccount,
    UpdateApiKeyAccountCmd,
    UpdatePasswordAccountCmd,
)
from ..domain.models.identity_mapping import (
    CreateIdentityMappingCmd,
    IdentityMapping,
    ReadIdentityMapping,
    UpdateIdentityMappingCmd,
)
from ..domain.models.invite import (
    CreatePasswordInviteCmd,
    PasswordInvite,
    ReadPasswordInvite,
    UpdatePasswordInviteCmd,
)
from ..domain.models.reset import (
    CreatePasswordResetCmd,
    PasswordReset,
    ReadPasswordReset,
    UpdatePasswordResetCmd,
)
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from .constants import AuthnResourceName

# ----------------------- #

# The read models below carry credential material (Argon2 password hashes, HMAC
# key/token digests), so the specs are marked ``sensitive`` — generated external
# surfaces (HTTP route generators, MCP tools/resources) refuse to project them.

password_account_spec = DocumentSpec(
    name=AuthnResourceName.PASSWORD_ACCOUNTS,
    read=ReadPasswordAccount,
    write={
        "domain": PasswordAccount,
        "create_cmd": CreatePasswordAccountCmd,
        "update_cmd": UpdatePasswordAccountCmd,
    },
    sensitive=True,
)

api_key_account_spec = DocumentSpec(
    name=AuthnResourceName.API_KEY_ACCOUNTS,
    read=ReadApiKeyAccount,
    write={
        "domain": ApiKeyAccount,
        "create_cmd": CreateApiKeyAccountCmd,
        "update_cmd": UpdateApiKeyAccountCmd,
    },
    sensitive=True,
)

# ....................... #

password_invite_spec = DocumentSpec(
    name=AuthnResourceName.PASSWORD_INVITES,
    read=ReadPasswordInvite,
    write={
        "domain": PasswordInvite,
        "create_cmd": CreatePasswordInviteCmd,
        "update_cmd": UpdatePasswordInviteCmd,
    },
    sensitive=True,
)

# ....................... #

password_reset_spec = DocumentSpec(
    name=AuthnResourceName.PASSWORD_RESETS,
    read=ReadPasswordReset,
    write={
        "domain": PasswordReset,
        "create_cmd": CreatePasswordResetCmd,
        "update_cmd": UpdatePasswordResetCmd,
    },
    sensitive=True,
)

# ....................... #

session_spec = DocumentSpec(
    name=AuthnResourceName.TOKEN_SESSIONS,
    read=ReadSession,
    write={
        "domain": Session,
        "create_cmd": CreateSessionCmd,
        "update_cmd": UpdateSessionCmd,
    },
    sensitive=True,
)

# ....................... #

identity_mapping_spec = DocumentSpec(
    name=AuthnResourceName.IDENTITY_MAPPINGS,
    read=ReadIdentityMapping,
    write={
        "domain": IdentityMapping,
        "create_cmd": CreateIdentityMappingCmd,
        "update_cmd": UpdateIdentityMappingCmd,
    },
)
