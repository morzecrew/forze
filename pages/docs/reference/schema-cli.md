---
title: Schema CLI
icon: lucide/terminal
summary: The forze schema commands, what the diff prints for each catalog state, and its exit codes
---

The how-to is [Upgrading forze's tables](../running-in-prod/upgrading-forze-tables.md); this
page is the contract CI relies on. Forze prints SQL and never runs it.

## Commands

```text
forze schema sql   --deps MODULE:ATTR
forze schema diff  --deps MODULE:ATTR [--check]
```

`--deps` names a `PostgresDepsModule`, or a sequence holding exactly one. Both commands
cover the owned tables it configures: `durable_run`, `durable_step` and `durable_schedule`.
A relation given as a tenant-scoped resolver is skipped, with a warning on stderr, and
`diff` then exits `11`: a relation it never compared cannot be reported in sync.

`sql` reads no database. It prints each table, then its required indexes, then its
recommended ones, under a header naming the forze version, the table and its adapter. A
tenant-aware table also gets the `tenant_id`-leading variant of each scan index.

`diff` reads its DSN from `FORZE_SCHEMA_DSN` and refuses `--dsn`. It reads every relation
in one `READ ONLY`, `REPEATABLE READ` transaction, so its output describes one moment. The
role the DSN names must have `USAGE` on the schemas and no write privilege: that role, not
the transaction, keeps the diff from writing.

`--check` prints nothing and only sets the exit code.

## What the diff prints

| Found in the catalog | Printed |
|---|---|
| Table missing | `CREATE TABLE`, then every declared index, without `CONCURRENTLY` |
| A view, a partitioned table or another non-plain table under the name | A refusal; a partitioned table takes no `CREATE INDEX CONCURRENTLY` |
| Primary key on other columns than declared | A refusal naming both key lists |
| Primary key on the declared columns in another order | A note with an index on the declared leading column commented, unless an index already leads with it: writes work, a scan on that column does not use the key |
| Column missing, nullable or with a literal default | `ALTER TABLE … ADD COLUMN` |
| Column missing, `NOT NULL` with no literal default | A refusal, the `ADD COLUMN` commented |
| Column type of another family | A refusal naming both types; never `ALTER … TYPE` |
| Declared `NOT NULL`, found nullable | A refusal, the `SET NOT NULL` commented |
| Declared nullable, found `NOT NULL` | A refusal, the `DROP NOT NULL` commented: the adapter writes `NULL` there |
| Declared index absent | `CREATE [UNIQUE] INDEX CONCURRENTLY ON …`, with its comment |
| Declared index invalid, no build running | `DROP INDEX CONCURRENTLY <found name>`, then the create |
| Declared index invalid while a build may be running | A warning, and nothing for that index |
| A retired index that is a retired match | `DROP INDEX CONCURRENTLY <found name>`, after every create |
| A retired match that is invalid while a build may be running | A warning, and no drop |
| An index with a retired shape's keys that is not a retired match | A refusal naming the index and the condition it failed |
| A retired column present | A note with the `DROP COLUMN` commented |
| A column or index neither declared nor retired | Nothing |

Type families: `text`, `varchar`, `char` and `citext` are one; `smallint`, `integer` and
`bigint` are one; every other type is its own. Within a family, a narrower column than
declared is refused, since a value the adapter writes may not fit: a smaller integer, or a
text type with a length limit (`varchar(36)`, `char(8)`, and `name`, which holds 63 bytes). A
wider one is the application's call.

An index **matches** a declared one when it is a b-tree over the same keys in the same order
and direction, with the same uniqueness and predicate, and default operator classes and
collations. `INCLUDE` columns do not count against a match. A unique constraint's index
matches a declared unique index, unless the constraint is `DEFERRABLE`.

`ON CONFLICT` infers every unique index on its target's columns, a full one for a partial
target included, and refuses a deferrable one even beside a `NOT DEFERRABLE` index. So the
diff refuses a deferrable unique index on the columns of any target the adapter writes to:
each declared unique index's and the primary key's. It also refuses a `NULLS NOT DISTINCT`
unique index on a nullable key, on which two rows without a key collide, partial or not,
unless its predicate keeps those rows out with `<key> IS NOT NULL`. Each refusal carries the
`DROP CONSTRAINT` or `DROP INDEX` commented, unless a foreign key depends on the index, when
it says to drop that key first. The diff still prints forze's own index when it is missing.

Keys and predicates are compared after normalizing: casts on literals and whitespace dropped,
keywords and bare names case-insensitive, `AND` and `OR` compared as a tree. A cast on a
column is kept, since it can change the value. In a predicate, a cast to plain `text` on a
`text` or `varchar` column is dropped, with the parentheses around the column: there
Postgres stores `status = 'pending'` as `((status)::text = 'pending'::text)`. On any other
column the cast is kept, since it changes how values compare. A `citext` column compared
through `::text` stops ignoring case, and `ON CONFLICT` cannot infer such an index, so the
diff prints forze's.

A **retired match** has a retired shape's keys and is not unique, backs no constraint, has
exactly the retired predicate, and has no `INCLUDE` columns, non-default operator classes or
non-default collations. Anything else with those keys is refused, because it may be the
application's own.

"A build may be running" means `pg_stat_progress_create_index` shows a build on the table, or
one on a table the role cannot see. A role without `pg_read_all_stats` sees no table for
another role's build.

## Output rules

- Every identifier printed is quoted as Postgres's `quote_ident()` quotes it: bare when it is
  lower-case letters, digits and underscores and not a reserved word, double-quoted otherwise.
- Every comment is one line. Line breaks and control characters in a catalog name become
  spaces.
- Indexes are created unnamed, as the reference pages print them.
- A non-empty diff opens with two lines: the output covers shape only, and `CONCURRENTLY`
  cannot run in a transaction.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Nothing to apply; a note may still print |
| `10` | Statements to apply |
| `11` | A refusal, a warning or a skipped relation needs a person; wins over `10` |
| `1` | Internal error |
| `2` | Usage error, such as `--dsn` passed |
| `75` | The database could not be reached or read; safe to retry |
| `78` | Configuration: `--deps` does not load or name one `PostgresDepsModule`, `FORZE_SCHEMA_DSN` is unset or does not parse, the server refuses the login, the role lacks access, or the `postgres` extra is missing |

A DSN that does not parse is reported without its text, which may hold a password.

The codes say what the diff found, whether or not `--check` printed it. A note, such as a
retired column still present, changes no exit code. A refused login is told from an
unreachable server by the server's message, so a server that localizes its messages
exits `75` for both.
