from forze.application.contracts.document import DocumentSpec

from ..domain.models.bindings import (
    CreateGroupPermissionBindingCmd,
    CreateGroupPrincipalBindingCmd,
    CreateGroupRoleBindingCmd,
    CreatePrincipalPermissionBindingCmd,
    CreatePrincipalRoleBindingCmd,
    CreateRolePermissionBindingCmd,
    GroupPermissionBinding,
    GroupPrincipalBinding,
    GroupRoleBinding,
    PrincipalPermissionBinding,
    PrincipalRoleBinding,
    ReadGroupPermissionBinding,
    ReadGroupPrincipalBinding,
    ReadGroupRoleBinding,
    ReadPrincipalPermissionBinding,
    ReadPrincipalRoleBinding,
    ReadRolePermissionBinding,
    RolePermissionBinding,
)
from ..domain.models.group import CreateGroupCmd, Group, ReadGroup, UpdateGroupCmd
from ..domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
    PermissionDefinition,
    ReadPermissionDefinition,
    UpdatePermissionDefinitionCmd,
)
from ..domain.models.policy_principal import (
    CreatePolicyPrincipalCmd,
    PolicyPrincipal,
    ReadPolicyPrincipal,
    UpdatePolicyPrincipalCmd,
)
from ..domain.models.role_definition import (
    CreateRoleDefinitionCmd,
    ReadRoleDefinition,
    RoleDefinition,
    UpdateRoleDefinitionCmd,
)
from .constants import AuthzResourceName

# ----------------------- #

policy_principal_spec = DocumentSpec(
    name=AuthzResourceName.POLICY_PRINCIPALS,
    read=ReadPolicyPrincipal,
    write={
        "domain": PolicyPrincipal,
        "create_cmd": CreatePolicyPrincipalCmd,
        "update_cmd": UpdatePolicyPrincipalCmd,
    },
)

# ....................... #

permission_definition_spec = DocumentSpec(
    name=AuthzResourceName.PERMISSIONS,
    read=ReadPermissionDefinition,
    write={
        "domain": PermissionDefinition,
        "create_cmd": CreatePermissionDefinitionCmd,
        "update_cmd": UpdatePermissionDefinitionCmd,
    },
)

role_definition_spec = DocumentSpec(
    name=AuthzResourceName.ROLES,
    read=ReadRoleDefinition,
    write={
        "domain": RoleDefinition,
        "create_cmd": CreateRoleDefinitionCmd,
        "update_cmd": UpdateRoleDefinitionCmd,
    },
)

group_spec = DocumentSpec(
    name=AuthzResourceName.GROUPS,
    read=ReadGroup,
    write={
        "domain": Group,
        "create_cmd": CreateGroupCmd,
        "update_cmd": UpdateGroupCmd,
    },
)

# ....................... #
# Bindings (create/delete only)


role_permission_binding_spec = DocumentSpec(
    name=AuthzResourceName.ROLE_PERMISSION_BINDINGS,
    read=ReadRolePermissionBinding,
    write={
        "domain": RolePermissionBinding,
        "create_cmd": CreateRolePermissionBindingCmd,
    },
)

principal_role_binding_spec = DocumentSpec(
    name=AuthzResourceName.PRINCIPAL_ROLE_BINDINGS,
    read=ReadPrincipalRoleBinding,
    write={
        "domain": PrincipalRoleBinding,
        "create_cmd": CreatePrincipalRoleBindingCmd,
    },
)

principal_permission_binding_spec = DocumentSpec(
    name=AuthzResourceName.PRINCIPAL_PERMISSION_BINDINGS,
    read=ReadPrincipalPermissionBinding,
    write={
        "domain": PrincipalPermissionBinding,
        "create_cmd": CreatePrincipalPermissionBindingCmd,
    },
)

group_principal_binding_spec = DocumentSpec(
    name=AuthzResourceName.GROUP_PRINCIPAL_BINDINGS,
    read=ReadGroupPrincipalBinding,
    write={
        "domain": GroupPrincipalBinding,
        "create_cmd": CreateGroupPrincipalBindingCmd,
    },
)

group_role_binding_spec = DocumentSpec(
    name=AuthzResourceName.GROUP_ROLE_BINDINGS,
    read=ReadGroupRoleBinding,
    write={
        "domain": GroupRoleBinding,
        "create_cmd": CreateGroupRoleBindingCmd,
    },
)

group_permission_binding_spec = DocumentSpec(
    name=AuthzResourceName.GROUP_PERMISSION_BINDINGS,
    read=ReadGroupPermissionBinding,
    write={
        "domain": GroupPermissionBinding,
        "create_cmd": CreateGroupPermissionBindingCmd,
    },
)
