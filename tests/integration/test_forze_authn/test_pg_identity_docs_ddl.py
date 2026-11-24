"""Integration: the identity DDL the docs publish passes startup schema validation, quietly.

The provisioning recipe promises its tables are what validation checks. This runs that DDL,
read from the page so the two cannot drift, binds the specs it covers, and validates them
through the specs derived from the identity inventory.

The recipe's unique index on the password-account username is one the framework relies on
and does not declare as a guarantee, since not every document store can keep one. The
application cannot declare anything on a framework spec, so the index must not draw the
undeclared-index warning.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest

from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_identity import spec_contributions
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.authn.application.specs import password_account_spec
from forze_identity.authz.application.constants import AuthzResourceName
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_specs,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

_RECIPE = Path(__file__).parents[3] / "pages/docs/recipes/provision-identity-postgres.md"
_SQL = re.compile(r"```sql\n(.*?)```", re.DOTALL)
_TABLE = re.compile(r"\b((?:authn|authz)_\w+)\b")
_LOGGER = "forze_postgres.kernel.catalog.validation.validate_schema.logger"

# Table names are the application's, so the recipe's tables are mapped to the specs its
# headings name rather than assumed to match the route names.
_ROUTES = {
    "authn_password_accounts": AuthnResourceName.PASSWORD_ACCOUNTS,
    "authn_api_key_accounts": AuthnResourceName.API_KEY_ACCOUNTS,
    "authn_sessions": AuthnResourceName.TOKEN_SESSIONS,
    "authn_password_resets": AuthnResourceName.PASSWORD_RESETS,
    "authz_policy_principals": AuthzResourceName.POLICY_PRINCIPALS,
    "authz_principal_role_bindings": AuthzResourceName.PRINCIPAL_ROLE_BINDINGS,
}


def _config(schema: str, table: str) -> PostgresDocumentConfig:
    return PostgresDocumentConfig(
        read=(schema, table),
        write=(schema, table),
        bookkeeping_strategy="application",
    )


async def test_the_documented_identity_ddl_validates_without_warnings(
    pg_client: PostgresClient,
) -> None:
    # The blocks that create tables; a migration block is for a deployment that has them.
    ddl = "\n".join(
        block
        for block in _SQL.findall(_RECIPE.read_text(encoding="utf-8"))
        if "CREATE TABLE" in block
    )

    assert set(_TABLE.findall(ddl)) == set(_ROUTES), "the recipe's tables changed"

    schema = f"identity_docs_{uuid4().hex[:12]}"
    await pg_client.execute(f"CREATE SCHEMA {schema};")

    for statement in _TABLE.sub(rf"{schema}.\1", ddl).split(";"):
        if statement.strip():
            await pg_client.execute(statement)

    deps = PostgresDepsModule(
        client=pg_client,
        rw_documents={route: _config(schema, table) for table, route in _ROUTES.items()},
    )
    specs = postgres_document_schema_specs(spec_contributions().freeze(), deps)
    ctx = context_from_deps(
        Deps.plain({PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client)})
    )

    assert len(specs) == len(_ROUTES)

    with patch(_LOGGER) as log:
        await PostgresDocumentSchemaValidationHook(specs=specs)(ctx)

    assert log.warning.call_args_list == []

    # The contrast that keeps the assertion above meaningful: a spec built by hand, with
    # nothing saying who owns it, does report the index.
    by_hand = postgres_document_schema_spec_for_binding(
        AuthnResourceName.PASSWORD_ACCOUNTS,
        spec=password_account_spec,
        config=_config(schema, "authn_password_accounts"),
    )

    with patch(_LOGGER) as log:
        await PostgresDocumentSchemaValidationHook(specs=(by_hand,))(ctx)

    log.warning.assert_called_once()
    assert "UniqueTogether(fields=('username',))" in (
        log.warning.call_args.args[0] % log.warning.call_args.args[1:]
    )


async def test_the_documented_migration_upgrades_a_pair_constrained_binding_table(
    pg_client: PostgresClient,
) -> None:
    # The shape a deployment had before scope_tenant_id, hh's among them: one row per
    # principal and role. After the recipe's migration a role can be held globally and in a
    # tenant at once, and the new index still keeps one row per tenant.
    (migration,) = (
        block
        for block in _SQL.findall(_RECIPE.read_text(encoding="utf-8"))
        if "ADD COLUMN scope_tenant_id" in block
    )
    schema = f"identity_migration_{uuid4().hex[:12]}"
    table = f"{schema}.authz_principal_role_bindings"
    await pg_client.execute(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            role_id uuid NOT NULL,
            UNIQUE (principal_id, role_id)
        );
        """
    )

    await pg_client.execute(re.sub(r"\bauthz_principal_role_bindings\b", table, migration))

    principal_id, role_id = uuid4(), uuid4()
    insert = f"""
        INSERT INTO {table}
            (id, rev, created_at, last_update_at, principal_id, role_id, scope_tenant_id)
        VALUES (%s, 1, now(), now(), %s, %s, %s)
    """
    await pg_client.execute(insert, (uuid4(), principal_id, role_id, None))
    await pg_client.execute(insert, (uuid4(), principal_id, role_id, uuid4()))

    with pytest.raises(CoreException, match="Unique violation"):
        await pg_client.execute(insert, (uuid4(), principal_id, role_id, None))
