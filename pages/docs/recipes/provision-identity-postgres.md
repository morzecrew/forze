---
title: Provision identity tables on Postgres
icon: lucide/database
summary: The canonical DDL for the authn/authz document specs — the columns startup schema validation enforces
---

The identity plane stores its documents through the ordinary document plane, so
the specs are backend-agnostic: the same models run on Postgres, Mongo, or the
mock. What *is* backend-specific is provisioning. On Postgres the tables are
yours to create, and at startup the schema validation checks every mapped column
against the model (type family and nullability, exactly).

Forze prints the DDL for you. With the identity specs in your spec registry,
`forze schema sql --deps MODULE:ATTR --specs MODULE:ATTR` prints a table for every identity route
the deps module binds, and `forze schema diff --deps MODULE:ATTR --specs MODULE:ATTR` prints what a
live database lacks; see [`forze schema`](../reference/schema-cli.md). The SQL on
this page is that render, for the six tables most deployments bind, checked
against it by forze's tests.

The render picks one column type per model field. Startup validation and
`forze schema diff` also accept the other types of its family:

| Model field | Rendered column | Also accepted |
| --- | --- | --- |
| `UUID` | `uuid` | |
| `str`, a `str` enum, a `Literal` of strings, `EmailStr` | `text` | `varchar`, `char`, `citext` |
| `bool` | `boolean` | |
| `int` | `integer` (`bigint` for `Int64`) | `smallint`, `bigint` |
| `float` | `double precision` | |
| `datetime` | `timestamptz` | |
| `Decimal` | `numeric` | |
| a list of one of the above | that type's array | |
| nested model / `dict` / any other list | `jsonb` | |
| `T \| None` | the same type, nullable | |
| any other field | `NOT NULL` | |

Table names are yours: the spec binds by route configuration, not by name.
Indexes beyond the required ones are yours too; each table below notes the ones
its access patterns want.

## Authn

Password accounts (`password_account_spec`):

```sql
CREATE TABLE app.authn_password_accounts (
    id             uuid        NOT NULL,
    rev            integer     NOT NULL,
    created_at     timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    is_active      boolean     NOT NULL DEFAULT true,
    email          text,
    principal_id   uuid        NOT NULL,
    username       text        NOT NULL,
    password_hash  text        NOT NULL,
    PRIMARY KEY (id)
);
-- required: authn_password_accounts relies on it over (username)
CREATE UNIQUE INDEX ON app.authn_password_accounts (username);
```

Keep the unique index on `username`: registration checks for an existing login before it
inserts, and only the index stops two concurrent registrations of one username. The spec
declares it in `required_indexes`, not as a storage guarantee, because not every document
store can keep one, so startup validation neither requires it nor reports it.

API-key accounts (`api_key_account_spec`):

```sql
CREATE TABLE app.authn_api_key_accounts (
    id                 uuid        NOT NULL,
    rev                integer     NOT NULL,
    created_at         timestamptz NOT NULL,
    last_update_at     timestamptz NOT NULL,
    is_active          boolean     NOT NULL DEFAULT true,
    principal_id       uuid        NOT NULL,
    actor_principal_id uuid,
    prefix             text,
    hint               text,
    label              text,
    expires_at         timestamptz,
    key_hash           text        NOT NULL,
    PRIMARY KEY (id)
);
-- required: authn_api_key_accounts relies on it over (key_hash)
CREATE INDEX ON app.authn_api_key_accounts (key_hash);
-- required: authn_api_key_accounts relies on it over (principal_id)
CREATE INDEX ON app.authn_api_key_accounts (principal_id);
```

Sessions (`session_spec`):

```sql
CREATE TABLE app.authn_token_sessions (
    id             uuid        NOT NULL,
    rev            integer     NOT NULL,
    created_at     timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    revoked_at     timestamptz,
    rotated_at     timestamptz,
    replaced_by    uuid,
    principal_id   uuid        NOT NULL,
    tenant_id      uuid,
    family_id      uuid        NOT NULL,
    refresh_digest text        NOT NULL,
    expires_at     timestamptz NOT NULL,
    PRIMARY KEY (id)
);
-- required: authn_token_sessions relies on it over (refresh_digest)
CREATE INDEX ON app.authn_token_sessions (refresh_digest);
-- required: authn_token_sessions relies on it over (principal_id)
CREATE INDEX ON app.authn_token_sessions (principal_id);
```

```sql title="For lookups by family"
CREATE INDEX ON app.authn_token_sessions (family_id);
```

Password resets (`password_reset_spec`):

```sql
CREATE TABLE app.authn_password_resets (
    id             uuid        NOT NULL,
    rev            integer     NOT NULL,
    created_at     timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    used_at        timestamptz,
    principal_id   uuid        NOT NULL,
    token_digest   text        NOT NULL,
    expires_at     timestamptz NOT NULL,
    PRIMARY KEY (id)
);
```

## Authz

Policy principals (`policy_principal_spec`) — also the one table a token-only
deployment needs for the default eligibility gate
(`AuthnDepsModule(eligibility="allow_all")` is the declared opt-out):

```sql
CREATE TABLE app.authz_policy_principals (
    id             uuid        NOT NULL,
    rev            integer     NOT NULL,
    created_at     timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    is_active      boolean     NOT NULL DEFAULT true,
    kind           text        NOT NULL,
    PRIMARY KEY (id)
);
```

Principal-role bindings (`principal_role_binding_spec`) carry the tenant a binding
holds in, `scope_tenant_id`; `NULL` means every tenant
([roles in one tenant](../identity-tenancy-enc/identity.md#roles-in-one-tenant)). The
unique index is required: role assignment checks for an existing row and then inserts,
and the index is what stops two concurrent assigns leaving two rows, one of which a revoke
would miss. `NULLS NOT DISTINCT` counts `NULL` (every tenant) as one value.

```sql
CREATE TABLE app.authz_principal_role_bindings (
    id              uuid        NOT NULL,
    rev             integer     NOT NULL,
    created_at      timestamptz NOT NULL,
    last_update_at  timestamptz NOT NULL,
    principal_id    uuid        NOT NULL,
    role_id         uuid        NOT NULL,
    scope_tenant_id uuid,
    PRIMARY KEY (id)
);
-- required: authz_principal_role_bindings relies on it over (principal_id, role_id, scope_tenant_id)
CREATE UNIQUE INDEX ON app.authz_principal_role_bindings
    (principal_id, role_id, scope_tenant_id) NULLS NOT DISTINCT;
```

`NULLS NOT DISTINCT` needs Postgres 15 or later. An existing table gains the column
and the index in one migration, after refusing to run over duplicate rows, which the
check-then-insert assign could leave behind. Drop a `UNIQUE (principal_id, role_id)`
constraint in the same migration, or a role held in every tenant can never also be
bound in one:

```sql title="Migration: scope_tenant_id"
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM app.authz_principal_role_bindings
             GROUP BY principal_id, role_id HAVING count(*) > 1) THEN
    RAISE EXCEPTION 'duplicate principal-role bindings: keep one row per pair before migrating';
  END IF;
END $$;
ALTER TABLE app.authz_principal_role_bindings ADD COLUMN scope_tenant_id uuid;
-- Postgres's default name for UNIQUE (principal_id, role_id), drop one named otherwise by its name
ALTER TABLE app.authz_principal_role_bindings
    DROP CONSTRAINT IF EXISTS authz_principal_role_bindings_principal_id_role_id_key;
CREATE UNIQUE INDEX ON app.authz_principal_role_bindings
    (principal_id, role_id, scope_tenant_id) NULLS NOT DISTINCT;
```

A table with a `tenant_id` column, one role catalog shared by tenant-aware ports, holds one
row per tenant for the same pair: group by `tenant_id, principal_id, role_id` in the guard and
lead the index with `tenant_id`, as `forze schema diff` prints it. A store kept per tenant, a
routed client's database or a relation per tenant, whose ports are not tenant-aware under
`tenancy_mode="global"`, backfills its tenant in the same migration, in each tenant's store.
Its old rows otherwise read as held in every tenant, which a revoke inside the tenant refuses
and a detach leaves in place. Tenant-aware ports and `require_invocation_tenant` need no
backfill:

```sql title="Migration: a store kept per tenant"
UPDATE app.authz_principal_role_bindings
    SET scope_tenant_id = '<the tenant this store holds>'
    WHERE scope_tenant_id IS NULL;
```

SQL that reads or writes this table directly must follow the same rule once tenant rows
exist. A reader keeps a row when `scope_tenant_id IS NULL OR scope_tenant_id = <bound
tenant>`, and only `IS NULL` with no tenant bound. A writer sets `scope_tenant_id` on
purpose, since leaving it out binds the role in every tenant.

The remaining identity specs (identity mappings, invites, authz bindings and
grants, tenancy) are printed by `forze schema sql --deps … --specs …` the same way.

## Binding the specs to a schema

Once the tables exist, the deps module needs a `rw_documents` mapping for them.
The relation each name binds to is yours — this recipe happens to name tables
after the specs, so the mapping is a comprehension — but the *names* come from
`identity_document_names`, validated against the identity inventory, so a
renamed or misspelled spec fails a test naming the spec rather than a deploy
naming a missing table:

```python
from forze_identity import identity_document_names
from forze_postgres import PostgresDocumentConfig

IDENTITY_PG = {
    name: PostgresDocumentConfig(
        read=("app", name),
        write=("app", name),
        # the DDL in this recipe ships no optimistic-locking trigger,
        # so bookkeeping stays application-side
        bookkeeping_strategy="application",
    )
    for name in identity_document_names()
}
```

Binding a subset? Choose from the named feature groups
(`GRANT_RESOLUTION_SPECS`, `AUTHZ_DECISION_SPECS`, `DELEGATION_SPECS`,
`PASSWORD_LIFECYCLE_SPECS`, `TENANT_RESOLUTION_SPECS`) via
`identity_document_names((*AUTHZ_DECISION_SPECS, *DELEGATION_SPECS))` rather
than hand-listing names: a principal can reach a role directly or through a
group, so binding only part of the grant-resolution set resolves fewer grants
than the database holds, silently.

## Trust but verify

These tables are the render forze's own Postgres integration tests create and run
schema validation against, so they cannot drift from the models silently. After an
upgrade, `forze schema diff --deps MODULE:ATTR --specs MODULE:ATTR` prints what your tables lack; if you
change a model, boot against a copy of production first: the validation error names
the exact field, the expected column family, and what it found.
