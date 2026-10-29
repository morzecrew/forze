"""Transport-agnostic boundary pieces for surfaces generated from the operation catalog.

Exposure, input validation, identity, API-key authentication and the bind-and-run of a
call, shared so that MCP and the operation CLI do not each carry a copy. It sits in the
execution layer because it needs :class:`~forze.application.execution.ExecutionContext`.
"""

from .arguments import accepted_input_names, validate_operation_input
from .authn import api_key_credentials, authenticate_api_key, resolve_principal_tenant
from .exposure import (
    check_query_policy,
    query_policy_guards,
    refuse_sensitive,
    select_operations,
)
from .identity import IngressIdentityResolver, StaticIdentityResolver
from .invoke import invoke_at_ingress, mint_invocation_metadata

# ----------------------- #

__all__ = [
    "IngressIdentityResolver",
    "StaticIdentityResolver",
    "accepted_input_names",
    "api_key_credentials",
    "authenticate_api_key",
    "check_query_policy",
    "invoke_at_ingress",
    "mint_invocation_metadata",
    "query_policy_guards",
    "refuse_sensitive",
    "resolve_principal_tenant",
    "select_operations",
    "validate_operation_input",
]
