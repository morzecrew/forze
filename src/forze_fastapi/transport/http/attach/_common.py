"""HTTP-specific helpers for attach_*_routes.

Attach modules iterate composition catalogs and delegate HTTP details to bindings.
Shared path/schema resolution lives in :mod:`forze_fastapi.transport.http.attach._loop`.
"""

from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from typing import Any

from forze.application.execution import ExecutionContext
from forze_fastapi.transport.http.auth import AuthnRequirement
from forze_fastapi.transport.http.options.route_opts import RouteOpts
from forze_fastapi.transport.http.policies import Policy, RequirePrincipal

# ----------------------- #


def route_opts_from_entry(entry: RouteOpts | bool | None) -> RouteOpts | None:
    if entry is None or entry is False:
        return None
    if entry is True:
        return {}
    return entry


def build_route_policies(
    *,
    base_policies: Sequence[Policy],
    authn: AuthnRequirement | None,
    ctx_dep: Callable[[], ExecutionContext],
    route_opts: RouteOpts | None,
    extra: Sequence[Policy] = (),
) -> list[Policy]:
    policies = list(base_policies)
    policies.extend(extra)

    resolved_authn = None
    if route_opts is not None:
        resolved_authn = route_opts.get("authn")
    if resolved_authn is None:
        resolved_authn = authn

    if resolved_authn is not None:
        policies.append(RequirePrincipal(requirement=resolved_authn, ctx_dep=ctx_dep))

    if route_opts is not None:
        policies.extend(route_opts.get("policies", ()))

    return policies


def default_idempotency_ttl(config: Mapping[str, Any] | None) -> timedelta:
    if config is None:
        return timedelta(seconds=30)
    return config.get("idempotency_ttl", timedelta(seconds=30))
