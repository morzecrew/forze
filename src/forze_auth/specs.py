import attrs

from forze.application.contracts.auth import AuthSpec
from forze.application.contracts.document import DocumentSpec

from .domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    CreatePasswordAccountCmd,
    PasswordAccount,
    ReadApiKeyAccount,
    ReadPasswordAccount,
    UpdateApiKeyAccountCmd,
    UpdatePasswordAccountCmd,
)
from .domain.models.iam import (
    CreateIamGroupCmd,
    CreateIamGroupRoleCmd,
    CreateIamPermissionCmd,
    CreateIamPrincipalCmd,
    CreateIamPrincipalGroupCmd,
    CreateIamPrincipalPermissionCmd,
    CreateIamPrincipalRoleCmd,
    CreateIamRoleCmd,
    CreateIamRolePermissionCmd,
    IamGroup,
    IamGroupRole,
    IamPermission,
    IamPrincipal,
    IamPrincipalGroup,
    IamPrincipalPermission,
    IamPrincipalRole,
    IamRole,
    IamRolePermission,
    ReadIamGroup,
    ReadIamGroupRole,
    ReadIamPermission,
    ReadIamPrincipal,
    ReadIamPrincipalGroup,
    ReadIamPrincipalPermission,
    ReadIamPrincipalRole,
    ReadIamRole,
    ReadIamRolePermission,
    UpdateIamGroupCmd,
    UpdateIamGroupRoleCmd,
    UpdateIamPermissionCmd,
    UpdateIamPrincipalCmd,
    UpdateIamPrincipalGroupCmd,
    UpdateIamPrincipalPermissionCmd,
    UpdateIamPrincipalRoleCmd,
    UpdateIamRoleCmd,
    UpdateIamRolePermissionCmd,
)
from .domain.models.session import (
    CreateRefreshGrantCmd,
    ReadRefreshGrant,
    RefreshGrant,
    UpdateRefreshGrantCmd,
)
from .kernel import (
    AccessTokenConfig,
    ApiKeyConfig,
    PasswordHasherConfig,
    RefreshTokenConfig,
)

# ----------------------- #


def password_account_spec(name: str = "auth_password_accounts") -> DocumentSpec[
    ReadPasswordAccount,
    PasswordAccount,
    CreatePasswordAccountCmd,
    UpdatePasswordAccountCmd,
]:
    """Return the default password-account document spec."""

    return DocumentSpec(
        name=name,
        read=ReadPasswordAccount,
        write={
            "domain": PasswordAccount,
            "create_cmd": CreatePasswordAccountCmd,
            "update_cmd": UpdatePasswordAccountCmd,
        },
    )


def api_key_account_spec(name: str = "auth_api_key_accounts") -> DocumentSpec[
    ReadApiKeyAccount,
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    UpdateApiKeyAccountCmd,
]:
    """Return the default API-key-account document spec."""

    return DocumentSpec(
        name=name,
        read=ReadApiKeyAccount,
        write={
            "domain": ApiKeyAccount,
            "create_cmd": CreateApiKeyAccountCmd,
            "update_cmd": UpdateApiKeyAccountCmd,
        },
    )


def refresh_grant_spec(name: str = "auth_refresh_grants") -> DocumentSpec[
    ReadRefreshGrant,
    RefreshGrant,
    CreateRefreshGrantCmd,
    UpdateRefreshGrantCmd,
]:
    """Return the default refresh-grant document spec."""

    return DocumentSpec(
        name=name,
        read=ReadRefreshGrant,
        write={
            "domain": RefreshGrant,
            "create_cmd": CreateRefreshGrantCmd,
            "update_cmd": UpdateRefreshGrantCmd,
        },
    )


def principal_spec(name: str = "auth_iam_principals") -> DocumentSpec[
    ReadIamPrincipal,
    IamPrincipal,
    CreateIamPrincipalCmd,
    UpdateIamPrincipalCmd,
]:
    """Return the default IAM-principal document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamPrincipal,
        write={
            "domain": IamPrincipal,
            "create_cmd": CreateIamPrincipalCmd,
            "update_cmd": UpdateIamPrincipalCmd,
        },
    )


def role_spec(name: str = "auth_iam_roles") -> DocumentSpec[
    ReadIamRole,
    IamRole,
    CreateIamRoleCmd,
    UpdateIamRoleCmd,
]:
    """Return the default IAM-role document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamRole,
        write={
            "domain": IamRole,
            "create_cmd": CreateIamRoleCmd,
            "update_cmd": UpdateIamRoleCmd,
        },
    )


def permission_spec(name: str = "auth_iam_permissions") -> DocumentSpec[
    ReadIamPermission,
    IamPermission,
    CreateIamPermissionCmd,
    UpdateIamPermissionCmd,
]:
    """Return the default IAM-permission document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamPermission,
        write={
            "domain": IamPermission,
            "create_cmd": CreateIamPermissionCmd,
            "update_cmd": UpdateIamPermissionCmd,
        },
    )


def group_spec(name: str = "auth_iam_groups") -> DocumentSpec[
    ReadIamGroup,
    IamGroup,
    CreateIamGroupCmd,
    UpdateIamGroupCmd,
]:
    """Return the default IAM-group document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamGroup,
        write={
            "domain": IamGroup,
            "create_cmd": CreateIamGroupCmd,
            "update_cmd": UpdateIamGroupCmd,
        },
    )


def principal_role_spec(name: str = "auth_iam_principal_roles") -> DocumentSpec[
    ReadIamPrincipalRole,
    IamPrincipalRole,
    CreateIamPrincipalRoleCmd,
    UpdateIamPrincipalRoleCmd,
]:
    """Return the default principal-role assignment document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamPrincipalRole,
        write={
            "domain": IamPrincipalRole,
            "create_cmd": CreateIamPrincipalRoleCmd,
            "update_cmd": UpdateIamPrincipalRoleCmd,
        },
    )


def principal_permission_spec(
    name: str = "auth_iam_principal_permissions",
) -> DocumentSpec[
    ReadIamPrincipalPermission,
    IamPrincipalPermission,
    CreateIamPrincipalPermissionCmd,
    UpdateIamPrincipalPermissionCmd,
]:
    """Return the default principal-permission assignment document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamPrincipalPermission,
        write={
            "domain": IamPrincipalPermission,
            "create_cmd": CreateIamPrincipalPermissionCmd,
            "update_cmd": UpdateIamPrincipalPermissionCmd,
        },
    )


def principal_group_spec(name: str = "auth_iam_principal_groups") -> DocumentSpec[
    ReadIamPrincipalGroup,
    IamPrincipalGroup,
    CreateIamPrincipalGroupCmd,
    UpdateIamPrincipalGroupCmd,
]:
    """Return the default principal-group assignment document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamPrincipalGroup,
        write={
            "domain": IamPrincipalGroup,
            "create_cmd": CreateIamPrincipalGroupCmd,
            "update_cmd": UpdateIamPrincipalGroupCmd,
        },
    )


def group_role_spec(name: str = "auth_iam_group_roles") -> DocumentSpec[
    ReadIamGroupRole,
    IamGroupRole,
    CreateIamGroupRoleCmd,
    UpdateIamGroupRoleCmd,
]:
    """Return the default group-role assignment document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamGroupRole,
        write={
            "domain": IamGroupRole,
            "create_cmd": CreateIamGroupRoleCmd,
            "update_cmd": UpdateIamGroupRoleCmd,
        },
    )


def role_permission_spec(name: str = "auth_iam_role_permissions") -> DocumentSpec[
    ReadIamRolePermission,
    IamRolePermission,
    CreateIamRolePermissionCmd,
    UpdateIamRolePermissionCmd,
]:
    """Return the default role-permission assignment document spec."""

    return DocumentSpec(
        name=name,
        read=ReadIamRolePermission,
        write={
            "domain": IamRolePermission,
            "create_cmd": CreateIamRolePermissionCmd,
            "update_cmd": UpdateIamRolePermissionCmd,
        },
    )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentAuthSpec(AuthSpec):
    """Document-backed auth provider specification."""

    access_secret_key: bytes = attrs.field(validator=attrs.validators.min_len(32))
    """Secret key used to sign and verify access tokens."""

    refresh_pepper: bytes = attrs.field(validator=attrs.validators.min_len(32))
    """Pepper used to digest refresh tokens before storage."""

    api_key_pepper: bytes = attrs.field(validator=attrs.validators.min_len(32))
    """Pepper used to digest API keys before storage."""

    password_accounts: DocumentSpec[
        ReadPasswordAccount,
        PasswordAccount,
        CreatePasswordAccountCmd,
        UpdatePasswordAccountCmd,
    ] = attrs.field(factory=password_account_spec)
    """Password-account document spec."""

    api_key_accounts: DocumentSpec[
        ReadApiKeyAccount,
        ApiKeyAccount,
        CreateApiKeyAccountCmd,
        UpdateApiKeyAccountCmd,
    ] = attrs.field(factory=api_key_account_spec)
    """API-key-account document spec."""

    refresh_grants: DocumentSpec[
        ReadRefreshGrant,
        RefreshGrant,
        CreateRefreshGrantCmd,
        UpdateRefreshGrantCmd,
    ] = attrs.field(factory=refresh_grant_spec)
    """Refresh-grant document spec."""

    principals: DocumentSpec[
        ReadIamPrincipal,
        IamPrincipal,
        CreateIamPrincipalCmd,
        UpdateIamPrincipalCmd,
    ] = attrs.field(factory=principal_spec)
    """IAM-principal document spec."""

    roles: DocumentSpec[
        ReadIamRole,
        IamRole,
        CreateIamRoleCmd,
        UpdateIamRoleCmd,
    ] = attrs.field(factory=role_spec)
    """IAM-role document spec."""

    permissions: DocumentSpec[
        ReadIamPermission,
        IamPermission,
        CreateIamPermissionCmd,
        UpdateIamPermissionCmd,
    ] = attrs.field(factory=permission_spec)
    """IAM-permission document spec."""

    groups: DocumentSpec[
        ReadIamGroup,
        IamGroup,
        CreateIamGroupCmd,
        UpdateIamGroupCmd,
    ] = attrs.field(factory=group_spec)
    """IAM-group document spec."""

    principal_roles: DocumentSpec[
        ReadIamPrincipalRole,
        IamPrincipalRole,
        CreateIamPrincipalRoleCmd,
        UpdateIamPrincipalRoleCmd,
    ] = attrs.field(factory=principal_role_spec)
    """Principal-role assignment document spec."""

    principal_permissions: DocumentSpec[
        ReadIamPrincipalPermission,
        IamPrincipalPermission,
        CreateIamPrincipalPermissionCmd,
        UpdateIamPrincipalPermissionCmd,
    ] = attrs.field(factory=principal_permission_spec)
    """Principal-permission assignment document spec."""

    principal_groups: DocumentSpec[
        ReadIamPrincipalGroup,
        IamPrincipalGroup,
        CreateIamPrincipalGroupCmd,
        UpdateIamPrincipalGroupCmd,
    ] = attrs.field(factory=principal_group_spec)
    """Principal-group assignment document spec."""

    group_roles: DocumentSpec[
        ReadIamGroupRole,
        IamGroupRole,
        CreateIamGroupRoleCmd,
        UpdateIamGroupRoleCmd,
    ] = attrs.field(factory=group_role_spec)
    """Group-role assignment document spec."""

    role_permissions: DocumentSpec[
        ReadIamRolePermission,
        IamRolePermission,
        CreateIamRolePermissionCmd,
        UpdateIamRolePermissionCmd,
    ] = attrs.field(factory=role_permission_spec)
    """Role-permission assignment document spec."""

    access_token: AccessTokenConfig = attrs.field(factory=AccessTokenConfig)
    """Access-token configuration."""

    refresh_token: RefreshTokenConfig = attrs.field(factory=RefreshTokenConfig)
    """Refresh-token configuration."""

    password_hasher: PasswordHasherConfig = attrs.field(factory=PasswordHasherConfig)
    """Password hashing configuration."""

    api_key: ApiKeyConfig = attrs.field(factory=ApiKeyConfig)
    """API-key configuration."""

    hydrate_token_identity: bool = attrs.field(default=True)
    """Whether token authentication reloads the IAM principal and grants."""

    hydrate_authorization: bool = attrs.field(default=True)
    """Whether authorization reloads grants instead of trusting identity permissions."""
