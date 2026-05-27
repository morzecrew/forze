from forze.application.contracts.document import DocumentSpec

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    CreatePasswordAccountCmd,
    PasswordAccount,
    ReadApiKeyAccount,
    ReadPasswordAccount,
    ReadPrincipal,
    UpdateApiKeyAccountCmd,
    UpdatePasswordAccountCmd,
)
from ..domain.models.identity_mapping import (
    CreateIdentityMappingCmd,
    IdentityMapping,
    ReadIdentityMapping,
    UpdateIdentityMappingCmd,
)
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from .constants import AuthnResourceName

# ----------------------- #

principal_spec = DocumentSpec(
    name=AuthnResourceName.PRINCIPALS,
    read=ReadPrincipal,
)

# ....................... #

password_account_spec = DocumentSpec(
    name=AuthnResourceName.PASSWORD_ACCOUNTS,
    read=ReadPasswordAccount,
    write={
        "domain": PasswordAccount,
        "create_cmd": CreatePasswordAccountCmd,
        "update_cmd": UpdatePasswordAccountCmd,
    },
)

api_key_account_spec = DocumentSpec(
    name=AuthnResourceName.API_KEY_ACCOUNTS,
    read=ReadApiKeyAccount,
    write={
        "domain": ApiKeyAccount,
        "create_cmd": CreateApiKeyAccountCmd,
        "update_cmd": UpdateApiKeyAccountCmd,
    },
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
