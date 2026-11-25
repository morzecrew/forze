---
title: Upgrading forze's tables
icon: lucide/database-zap
summary: Get the SQL for the Postgres tables forze owns from forze schema, review it, and catch a missing migration in CI
---

Some Postgres tables belong to forze rather than to your domain: its store tables, for
durable execution, the outbox and its delivery log, the HLC checkpoint, the inbox,
idempotency claims, counters, principal epochs and rotating credentials. Your migration
tool creates and changes them, as it does every other table. `forze schema` writes the SQL so you stop transcribing it from the
docs on each upgrade. **Forze prints the SQL and never runs it**: not at startup, not from
the CLI. You review the file and apply it with Flyway or your own runner.

Two commands do the work:

- `forze schema sql` prints the `CREATE` statements a fresh install needs. It reads no
  database.
- `forze schema diff` reads a live database and prints what it is missing for the installed
  forze version. It matches indexes by shape, not by name, so it works whatever your
  earlier migrations named them.

Both take `--deps`, the `PostgresDepsModule` your application wires, as `module:attribute`.
The relation names come from there. The exact contract is in the
[schema CLI reference](../reference/schema-cli.md).

## Create a read-only role for the diff

The diff connects as a role you create for it, with read access to the catalog and nothing
else:

```sql
CREATE ROLE forze_schema_reader LOGIN PASSWORD '…';
GRANT USAGE ON SCHEMA app TO forze_schema_reader;
-- Optional: lets the diff see which table another role's index build is on.
GRANT pg_read_all_stats TO forze_schema_reader;
```

Repeat the `USAGE` grant for every schema your relations live in. The catalog is readable
by every role by default, so the role needs no table privilege.

The role is what keeps the diff from writing. The diff also opens its transaction
`READ ONLY`, but a session can switch that back, so it only guards against a mistake in
forze itself. Do not point the diff at your migration role.

`pg_read_all_stats` also lets the role read other sessions' activity, including their query
text. Without it, the diff cannot tell which table a running index build is on. While any
build runs in the database, it then leaves every invalid index alone and prints a warning
for each.

## Start a new database

Print the statements into your first forze migration:

```console
$ forze schema sql --deps app.wire:postgres > migrations/V1__forze_tables.sql
```

The file holds each table, then its indexes, each with a one-line comment saying why it
exists. The same SQL, for example relation names, is on the
[durable execution](../reference/durable-schema.md), [outbox and inbox](../reference/outbox-schema.md)
and [store](../reference/store-schema.md) schema pages.

## Upgrade forze

1. Bump forze in your lock file.
2. Run the diff against a database migrated to your current head, such as staging, and
   write its output into your next migration:

    ```console
    $ export FORZE_SCHEMA_DSN='postgresql://forze_schema_reader:…@db/app'
    $ forze schema diff --deps app.wire:postgres > migrations/V42__forze_upgrade.sql
    ```

    The DSN is read from `FORZE_SCHEMA_DSN` only. A flag would leave it in your shell
    history and in `ps`, so `--dsn` is refused.

3. Review the file. For the 0.18 index change on a database at 0.17, it reads:

    ```sql
    -- Shape only: data rewrites are announced as **Migration:** SQL in CHANGELOG.md.
    -- CONCURRENTLY cannot run in a transaction (Flyway: executeInTransaction=false).
    -- Stop at the first error (psql: -v ON_ERROR_STOP=1): a drop below a failed create removes the index it replaces.

    -- forze 0.18.0 · app.durable_run (PostgresDurableRunStore)

    -- recommended since 0.18.0: the recovery scan (claim_abandoned) filters on status, and on name for some functions
    CREATE INDEX CONCURRENTLY ON app.durable_run (status, name, created_at);

    -- retired in 0.18.0: replaced by (status, name, created_at)
    DROP INDEX CONCURRENTLY app.idx_durable_run_status;
    ```

    The drop names the index your database holds, here one a migration named by hand.
    New indexes come first and drops last. To keep retired indexes for now, pass
    `--keep-retired`: the diff then says nothing about any index with a retired shape's
    keys.

4. Read every `REFUSED` and `WARNING` comment. The diff never prints a statement that
   rewrites a table or might drop something of yours. It refuses instead, and says why:

    - a `NOT NULL` column with no literal default, which needs a data step;
    - a column whose type changed family (`text` and `citext` are one family, and so are
      the integer widths), or narrowed within it, as `varchar(36)` or `smallint` would;
    - a column made `NOT NULL` that forze writes `NULL` to;
    - a primary key on other columns than the adapter's `ON CONFLICT` needs;
    - an index with a retired shape's keys that might be your own, because it is unique,
      backs a constraint, or has another predicate.

    Columns and indexes forze does not declare are never touched.

5. Apply the file outside a transaction, `CREATE INDEX CONCURRENTLY` cannot run in one, and
   with a tool that stops at the first error. A retired drop comes after the create that
   replaces it, so a runner that carried on past a failed create would drop the index the
   create was to replace. Flyway stops by default; add a script config file beside the
   migration:

    ```properties
    # migrations/V42__forze_upgrade.sql.conf
    executeInTransaction=false
    ```

    With `psql`, pass `-v ON_ERROR_STOP=1`. The next diff reads a failed create's invalid
    index and prints its drop and recreate.

6. Deploy the new forze version after the migration has run.

!!! note "Shape only"
    The diff compares columns and indexes. A change to how forze stores values, such as a
    key format, changes no shape, so the diff cannot see it. Such a change ships a
    **Migration:** statement in its CHANGELOG entry, written so that running it twice does
    nothing. Read the entries between your two versions as well.

## Catch a missing migration in CI

Run the diff with `--check` against the database your CI migrates. It prints nothing and
exits by what it found:

| Exit | Meaning |
|---|---|
| `0` | Nothing to apply: the migrations cover this forze version. |
| `10` | Statements are missing: write the migration. |
| `11` | A refusal or warning needs a person, or a relation was skipped. |

```console
$ forze schema diff --deps app.wire:postgres --check
```

A forze bump with no migration then fails the job with `10`. The other codes (`2`, `75`,
`78`) mean the check did not run, so the job fails with those too.

## Refuse to start on a table that cannot serve

A deploy that ran before its migration should stop at startup, not at its first write. Pass
the owned relations to the lifecycle module:

```python
from forze_postgres import PostgresLifecycleModule, owned_relations

PostgresLifecycleModule(client=pg, dsn="postgresql://…", owned_schema=owned_relations(pg_deps))
```

Startup then runs the diff's comparison with the application's own connection, and executes
nothing. It refuses to start, with `postgres_owned_schema_invalid` naming each table, when
an adapter's reads or writes would fail: a table, required column or required index is
missing or still being built, a column has another type family or refuses the `NULL` forze
writes, or a key or unique index breaks the adapter's `ON CONFLICT`. It logs a warning for anything else, such as
a recommended index the migration has not built yet. The
[Postgres integration](../integrations/postgres.md) page lists every case.

## Limits

- **Store tables only.** The identity, audit and other document tables still take their DDL
  from their pages.
- **Per-tenant tables need their tenants.** A relation given as a tenant-scoped resolver names
  no single table, so both commands skip it with a warning on stderr, and `diff` exits `11`.
  Pass `--tenant <id>` once per tenant to bind it for each.
- **Postgres only.**
