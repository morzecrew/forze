from ..base import BaseDepPort, DepKey
from .ports import AuthorizationPort, PrincipalRegistryPort, RoleAssignmentPort
from .specs import AuthzSpec

# ----------------------- #

PrincipalRegistryDepPort = BaseDepPort[AuthzSpec, PrincipalRegistryPort]
"""Principal registry dependency port."""

RoleAssignmentDepPort = BaseDepPort[AuthzSpec, RoleAssignmentPort]
"""Role assignment dependency port."""

AuthorizationDepPort = BaseDepPort[AuthzSpec, AuthorizationPort]
"""Authorization decision dependency port."""

# ....................... #

PrincipalRegistryDepKey = DepKey[PrincipalRegistryDepPort]("authz_principal_registry")
"""Key used to register the ``PrincipalRegistryPort`` builder implementation."""

RoleAssignmentDepKey = DepKey[RoleAssignmentDepPort]("authz_role_assignment")
"""Key used to register the ``RoleAssignmentPort`` builder implementation."""

AuthorizationDepKey = DepKey[AuthorizationDepPort]("authz_authorization")
"""Key used to register the ``AuthorizationPort`` builder implementation."""
