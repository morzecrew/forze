---
title: Store table schema
icon: lucide/table
summary: The application-owned idempotency, counter, principal epoch and rotating-credential tables — columns, keys and indexes, for Postgres
---

These Postgres store tables are application-owned: you apply their DDL with your migration
tool. The outbox, its delivery log, the HLC checkpoint and the inbox are on
[Outbox and inbox table schema](outbox-schema.md), and the durable tables on
[Durable execution schema](durable-schema.md).

Forze declares each table once, in code. `forze schema sql` prints the statements below for
the relations your `PostgresDepsModule` names, and `forze schema diff` prints what an
existing database is missing. [Upgrading forze's tables](../running-in-prod/upgrading-forze-tables.md)
walks through both. The SQL on this page is that render, for tables in the `app` schema, and
a test keeps it so. None of these configs has a switch that changes its table, and routes
that name one table share it.

## Idempotency claims

For `PostgresIdempotencyConfig(relation=("app", "idempotency"))`:

```sql
CREATE TABLE app.idempotency (
    op           text        NOT NULL,
    idem_key     text        NOT NULL,
    payload_hash text        NOT NULL,
    status       text        NOT NULL,
    result       bytea,
    expires_at   timestamptz NOT NULL,
    owner        uuid,
    PRIMARY KEY (op, idem_key)
);
```

- `status` is `pending` or `done`, and `result` is `NULL` while pending.
- A tenant-aware store puts the tenant into `idem_key`, so the table needs no tenant column.
- `owner` fences a claim to the call that took it. A table without it keeps working
  unfenced, and the store logs that once per process. Add it, and the fence turns on
  without a restart; see [Idempotency](../writing-operation/idempotency.md).

## Counters

For `PostgresCounterConfig(relation=("app", "counters"))`:

```sql
CREATE TABLE app.counters (
    tenant_id  text        NOT NULL,
    suffix     text        NOT NULL,
    value      bigint      NOT NULL,
    expires_at timestamptz,
    PRIMARY KEY (tenant_id, suffix)
);
```

- `tenant_id` is `''` when no tenant is bound. `suffix` folds the route into the key, so
  several counter routes share one table without merging.
- Only a counter bumped with a `ttl` reads `expires_at`, and `NULL` never expires. A table
  without it serves every other call, and refuses a `ttl` with `counter_ttl_unsupported`.
- A table-wide scheduled purge wants its own index; see
  [Postgres](../integrations/postgres.md). The store's own `purge_expired` needs none.

## Principal epochs

For `PostgresPrincipalEpochConfig(relation=("app", "principal_epochs"))`, which verifying
sessions from cached revocation states needs beside the sessions:

```sql
CREATE TABLE app.principal_epochs (
    principal_id uuid   NOT NULL,
    epoch        bigint NOT NULL,
    PRIMARY KEY (principal_id)
);
```

Never delete a row while a token might carry its epoch: a missing row reads as `0`.

## Rotating credentials

For `PostgresRotatingCredentialsConfig(relation=("app", "rotating_credentials"), ...)`:

```sql
CREATE TABLE app.rotating_credentials (
    tenant_id    text        NOT NULL,
    ref          text        NOT NULL,
    payload      jsonb       NOT NULL,
    expires_at   timestamptz,
    version      bigint      NOT NULL,
    burnt_reason text,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, ref)
);

-- the admin listing scans a tenant's grants by updated_at
CREATE INDEX ON app.rotating_credentials (tenant_id, updated_at);
```

`tenant_id` is part of the key, so a table keyed on `ref` alone could never hand one
tenant another's grant, and an unbound tenant stores as `''`. The store's own reads are
point lookups on the key; the index serves the control plane's idleness scan. The wiring is
in [Rotating OAuth credentials](../recipes/rotating-oauth-credentials.md).

## Next steps

- [Schema CLI](schema-cli.md): what `forze schema diff` prints for each catalog state, and
  its exit codes.
- [Upgrading forze's tables](../running-in-prod/upgrading-forze-tables.md): the read-only
  role, the migration file and the CI check.
