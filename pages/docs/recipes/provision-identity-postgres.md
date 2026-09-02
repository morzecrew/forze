---
title: Provision identity tables on Postgres
icon: lucide/database
summary: The canonical DDL for the authn/authz document specs — the columns startup schema validation enforces
---

The identity plane stores its documents through the ordinary document plane, so
the specs are backend-agnostic: the same models run on Postgres, Mongo, or the
mock. What *is* backend-specific is provisioning — on Postgres you write the
tables yourself, and at startup the schema validation checks every mapped column
against the model (type family and nullability, exactly). This recipe is the DDL
that passes that check, so migrations are copied, not reverse-engineered from
the models.

The mapping rule behind it is mechanical, and it is what the validator enforces
on every backend that has a schema:

| Model field | Column |
| --- | --- |
| `UUID` | `uuid` |
| `str` | `text` (or `varchar`/`char`/`citext`) |
| `bool` | `boolean` |
| `int` | `int2` / `int4` / `int8` |
| `datetime` | `timestamptz` (or `timestamp`) |
| `Decimal` | `numeric` |
| nested model / `dict` / `list` | `jsonb` |
| `T \| None` | the same family, nullable |
| required field | `NOT NULL` |

Table names are yours — the spec binds by route configuration, not by name.
Indexes beyond the primary key are yours too; the ones shown are the minimum
the access patterns want.

## Authn

Password accounts (`password_account_spec`):

```sql
CREATE TABLE authn_password_accounts (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    principal_id uuid NOT NULL,
    username text NOT NULL,
    email text,
    password_hash text NOT NULL,
    is_active boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX ON authn_password_accounts (username);
```

API-key accounts (`api_key_account_spec`):

```sql
CREATE TABLE authn_api_key_accounts (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    principal_id uuid NOT NULL,
    actor_principal_id uuid,
    prefix text,
    hint text,
    label text,
    key_hash text NOT NULL,
    expires_at timestamptz,
    is_active boolean NOT NULL DEFAULT true
);
CREATE INDEX ON authn_api_key_accounts (principal_id);
```

Sessions (`session_spec`):

```sql
CREATE TABLE authn_sessions (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    principal_id uuid NOT NULL,
    tenant_id uuid,
    family_id uuid NOT NULL,
    refresh_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    rotated_at timestamptz,
    replaced_by uuid
);
CREATE INDEX ON authn_sessions (principal_id);
CREATE INDEX ON authn_sessions (family_id);
```

Password resets (`password_reset_spec`):

```sql
CREATE TABLE authn_password_resets (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    principal_id uuid NOT NULL,
    token_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    used_at timestamptz
);
```

## Authz

Policy principals (`policy_principal_spec`) — also the one table a token-only
deployment needs for the default eligibility gate
(`AuthnDepsModule(eligibility="allow_all")` is the declared opt-out):

```sql
CREATE TABLE authz_policy_principals (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    kind text NOT NULL,
    is_active boolean NOT NULL
);
```

The remaining identity specs (identity mappings, invites, authz bindings and
grants) follow the same mapping rule — apply the table above to their model
fields, and startup validation will tell you, field by field, if a column
disagrees.

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
        read=("identity", name),
        write=("identity", name),
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

These shapes are the ones forze's own Postgres integration tests provision and
run schema validation against, so they cannot drift from the models silently. If
you change a model (or upgrade across a release that does), boot against a copy
of production first: the validation error names the exact field, the expected
column family, and what it found.
