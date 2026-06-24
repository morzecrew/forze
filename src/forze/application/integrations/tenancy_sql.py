"""Backend-neutral SQL-tenancy helpers reused across integration adapters.

A shared home for the *tenant-as-bound-parameter* mechanism so any port that runs registered
SQL (analytics, procedures, parametrized read sources) can bind and validate the tenant the same
way, without importing from a sibling integration package.
"""

import re
from typing import Mapping

from pydantic import BaseModel

from forze.application.contracts.tenancy import TenantProviderPort
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

TENANT_PARAM = "tenant"
"""Bound query-parameter name carrying the current tenant id on tenant-aware routes.

The registered SQL references it per backend — ``{tenant:UUID}`` (ClickHouse),
``@tenant`` (BigQuery), ``$tenant`` (DuckDB), ``%(tenant)s`` (Postgres) — and the adapter
binds the bound tenant id (as a string) under this name.
"""

# ....................... #


def bind_tenant_param(
    params: BaseModel | JsonDict,
    *,
    tenant_aware: bool,
    tenant_provider: TenantProviderPort | None,
    key: str = TENANT_PARAM,
    subject: str = "analytics",
) -> BaseModel | JsonDict:
    """Merge the current tenant id into bound query params on a tenant-aware route.

    On a tenant-aware route this **fails closed** — raising if the route is wired tenant-aware
    without a tenant provider (configuration) or with no bound tenant (authentication) — rather
    than running an unscoped query. The id is bound as a string under *key* so the registered
    SQL can reference it (``{tenant:UUID}`` / ``@tenant`` / ``$tenant`` / ``%(tenant)s``). On a
    non-tenant-aware route *params* is returned unchanged.

    Advisory by construction: binding the parameter does not guarantee the SQL *uses* it to
    scope. The wiring-time placeholder guard built on :func:`unreferenced_param_keys` rejects a
    tenant-aware route whose SQL never references the parameter.

    :param subject: Integration noun used in the misconfiguration message and code (e.g.
        ``analytics``, ``procedures``), so an error names the right surface.
    """

    if not tenant_aware:
        return params

    if tenant_provider is None:
        raise exc.configuration(
            f"Tenant provider is required for a tenant-aware {subject} route.",
            code=f"{subject}_tenant_provider_missing",
        )

    tenant = tenant_provider()

    if tenant is None:
        raise exc.authentication("Tenant ID is required", code="tenant_required")

    data = params.model_dump() if isinstance(params, BaseModel) else dict(params)
    data[key] = str(tenant.tenant_id)

    return data


# ....................... #


# One token = a single-quoted string, a double-quoted identifier, a line comment, or a block
# comment. Listing strings/identifiers first means a ``--`` or ``/*`` *inside* a quoted literal is
# consumed as part of that literal (and kept), so only genuine comments are blanked.
_SQL_TOKEN = re.compile(
    r"'(?:[^']|'')*'"  # single-quoted string ('' escapes a quote)
    r'|"(?:[^"]|"")*"'  # double-quoted identifier ("" escapes a quote)
    r"|--[^\n]*"  # line comment
    r"|/\*.*?\*/",  # block comment
    re.DOTALL,
)


def _strip_sql_comments(sql: str) -> str:
    """Blank out ``--`` line and ``/* */`` comments, leaving string/identifier literals intact.

    A naive strip would also remove a ``--`` that sits inside a quoted literal, dropping any real
    reference later on that line and failing a valid route at wiring. Scanning literals as whole
    tokens keeps them, so only genuine comments are removed.
    """

    def _blank(match: "re.Match[str]") -> str:
        token = match.group(0)
        return " " if token.startswith(("--", "/*")) else token

    return _SQL_TOKEN.sub(_blank, sql)


def unreferenced_param_keys(
    queries: Mapping[str, str],
    *,
    pattern: str,
) -> list[str]:
    """Return the sorted query keys whose SQL never references *pattern* (outside comments).

    The reusable core of the wiring-time tenant-placeholder guard: it checks *reference*, not
    correctness (the parameter could be referenced outside a filter), so it is a floor, not a
    proof of isolation. SQL comments are stripped first, so a placeholder that appears only in a
    comment does not satisfy the guard. Each integration wraps this with its own error message.

    :param queries: ``key -> SQL`` for the route.
    :param pattern: Backend regex matching the tenant placeholder (e.g. ``r"%\\(tenant\\)s"``).
    """

    rx = re.compile(pattern)

    return sorted(
        key for key, sql in queries.items() if not rx.search(_strip_sql_comments(sql))
    )
