"""Document specs for the authn plane — and their canonical Postgres DDL.

The specs prescribe exact columns, enforced at boot by the Postgres schema
validation (type and nullability per field). The DDL below is the canonical
starting point — the same shape forze's own integration fixtures validate
against; adapt names/indexes to taste, not columns. The durable adapters set
the precedent of shipping DDL next to the spec; the identity plane deserves
the same, so nobody reverse-engineers migrations from the models again.

Password accounts (``password_account_spec``)::

    CREATE TABLE authn_password_accounts (
        id uuid PRIMARY KEY,
        rev integer NOT NULL,
        created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL,
        principal_id uuid NOT NULL,
        username text NOT NULL,
        email text,
        password_hash text NOT NULL,
        is_active boolean NOT NULL DEFAULT true
    );

API-key accounts (``api_key_account_spec``)::

    CREATE TABLE authn_api_key_accounts (
        id uuid PRIMARY KEY,
        rev integer NOT NULL,
        created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL,
        principal_id uuid NOT NULL,
        actor_principal_id uuid,
        prefix text,
        hint text,
        label text,
        key_hash text NOT NULL,
        expires_at timestamptz,
        is_active boolean NOT NULL DEFAULT true
    );

Sessions (``session_spec``)::

    CREATE TABLE authn_sessions (
        id uuid PRIMARY KEY,
        rev integer NOT NULL,
        created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL,
        principal_id uuid NOT NULL,
        tenant_id uuid,
        family_id uuid NOT NULL,
        refresh_digest text NOT NULL,
        expires_at timestamptz NOT NULL,
        revoked_at timestamptz,
        rotated_at timestamptz,
        replaced_by uuid
    );

Password resets (``password_reset_spec``)::

    CREATE TABLE authn_password_resets (
        id uuid PRIMARY KEY,
        rev integer NOT NULL,
        created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL,
        principal_id uuid NOT NULL,
        token_digest text NOT NULL,
        expires_at timestamptz NOT NULL,
        used_at timestamptz
    );

The remaining specs in this module follow the same mechanical mapping the
validator enforces: every model field is a column of the matching family
(``UUID`` → ``uuid``, ``str`` → ``text``/``varchar``, ``bool`` → ``boolean``,
``datetime`` → ``timestamptz``, ``int`` → ``int2/4/8``, nested models →
``jsonb``), ``T | None`` fields nullable, required fields ``NOT NULL``.
"""

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
