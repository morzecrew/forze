from enum import StrEnum

# ----------------------- #


class AuthzResourceName(StrEnum):
    """Authz document resource names."""

    POLICY_PRINCIPALS = "authz_policy_principals"
    PERMISSIONS = "authz_permissions"
    ROLES = "authz_roles"
    GROUPS = "authz_groups"
    ROLE_PERMISSION_BINDINGS = "authz_role_permission_bindings"
    PRINCIPAL_ROLE_BINDINGS = "authz_principal_role_bindings"
    PRINCIPAL_PERMISSION_BINDINGS = "authz_principal_permission_bindings"
    GROUP_PRINCIPAL_BINDINGS = "authz_group_principal_bindings"
    GROUP_ROLE_BINDINGS = "authz_group_role_bindings"
    GROUP_PERMISSION_BINDINGS = "authz_group_permission_bindings"
