from ..base import ConfigurableDepPort, DepKey
from .ports import (
    AuthzPort,
    EffectiveGrantsPort,
    PrincipalRegistryPort,
    RoleAssignmentPort,
)
from .specs import AuthzSpec

# ----------------------- #

PrincipalRegistryDepPort = ConfigurableDepPort[AuthzSpec, PrincipalRegistryPort]
"""Principal registry dependency port."""

EffectiveGrantsDepPort = ConfigurableDepPort[AuthzSpec, EffectiveGrantsPort]
"""Effective grants dependency port."""

RoleAssignmentDepPort = ConfigurableDepPort[AuthzSpec, RoleAssignmentPort]
"""Role assignment dependency port."""

AuthzDepPort = ConfigurableDepPort[AuthzSpec, AuthzPort]
"""Authorization decision dependency port."""

# ....................... #

PrincipalRegistryDepKey = DepKey[PrincipalRegistryDepPort]("authz_principal_registry")
"""Key used to register the ``PrincipalRegistryPort`` builder implementation."""

EffectiveGrantsDepKey = DepKey[EffectiveGrantsDepPort]("authz_effective_grants")
"""Key used to register the ``EffectiveGrantsPort`` builder implementation."""

RoleAssignmentDepKey = DepKey[RoleAssignmentDepPort]("authz_role_assignment")
"""Key used to register the ``RoleAssignmentPort`` builder implementation."""

AuthzDepKey = DepKey[AuthzDepPort]("authz")
"""Key used to register the ``AuthorizationPort`` builder implementation."""
