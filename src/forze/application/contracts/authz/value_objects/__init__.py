"""Authz contract value objects grouped by concern."""

from .catalog import GroupRef, PermissionRef, PrincipalRef, RoleRef
from .decision import AuthzDecision, AuthzRequest, AuthzResource, AuthzScope, AuthzSubject
from .grants import EffectiveGrants
from .scoping import AuthzDocumentScope, AuthzDocumentScopeRequest, AuthzSensitiveAccessRequest

# ----------------------- #

__all__ = [
    "AuthzDecision",
    "AuthzDocumentScope",
    "AuthzDocumentScopeRequest",
    "AuthzRequest",
    "AuthzResource",
    "AuthzScope",
    "AuthzSensitiveAccessRequest",
    "AuthzSubject",
    "EffectiveGrants",
    "GroupRef",
    "PermissionRef",
    "PrincipalRef",
    "RoleRef",
]
