# RFC 0080 — Shipped schema migrations for forze-owned tables

- **Status:** 📝 Draft. Proposes an amendment to the beta rule that breaking changes ship
  without migration code (row 2), which the maintainer accepted on 2026-11-04 (§10 Q1).
- **Scope:** Moves the DDL for the Postgres tables forze owns out of docstrings and markdown
  into one declaration per table, kept in code. Adds a `forze schema` CLI group:
  - `sql` prints the `CREATE` statements a fresh install needs;
  - `diff` reads a live database and prints the forward statements that bring it to the
    installed forze version.

  Each consumer then applies that SQL with its own migration tool. P1 covers only the three
  durable tables (run, step, schedule), where the 0.18 index change hurt three consumers.
  P2 extends the declarations to the other eight Postgres store tables: the outbox, its
  delivery log and the HLC checkpoint; the inbox; idempotency claims; counters; principal
  epochs; and rotating credentials. P2 also adds a startup check of the store tables (the
  verify half; today only document specs are checked) and covers the framework-owned
  *document* specs: 19 identity specs, audit, progress jobs, the realtime mailbox, stored
  files and history. Forze still
  **never runs DDL itself**: not at startup, not from the CLI, and not when it provisions a
  tenant. The RFC does **not**:
  - cover the application's own document tables;
  - cover Mongo or any other backend;
  - add down-migrations;
  - add a migration runner or history table;
  - detect data rewrites (P3, demand-gated).

  It adds no port and changes no contract. New code lives in `forze_postgres` (the
  declarations, the diff) and `forze_cli` (the group).
- **Related:**
  - The DDL that exists today, in docstrings:
    - [`src/forze_postgres/adapters/durable/run_store.py:123-189`](../src/forze_postgres/adapters/durable/run_store.py),
      [`function_step.py:62`](../src/forze_postgres/adapters/durable/function_step.py),
      [`schedule_store.py:59-79`](../src/forze_postgres/adapters/durable/schedule_store.py)
    - [`src/forze_postgres/adapters/outbox.py:73-77`](../src/forze_postgres/adapters/outbox.py),
      [`inbox.py:31`](../src/forze_postgres/adapters/inbox.py),
      [`idempotency.py:88-111`](../src/forze_postgres/adapters/idempotency.py),
      [`counter.py:244`](../src/forze_postgres/adapters/counter.py),
      [`hlc_checkpoint.py:36`](../src/forze_postgres/adapters/hlc_checkpoint.py),
      [`principal_epoch.py:33`](../src/forze_postgres/adapters/principal_epoch.py),
      [`rotating_credentials.py:26-37`](../src/forze_postgres/adapters/rotating_credentials.py)
  - The DDL that exists today, on reference pages:
    [`pages/docs/reference/durable-schema.md`](../pages/docs/reference/durable-schema.md),
    [`outbox-schema.md`](../pages/docs/reference/outbox-schema.md)
  - The relation names: the store configs on `PostgresDepsModule`,
    [`src/forze_postgres/execution/deps/module.py:268-332`](../src/forze_postgres/execution/deps/module.py)
  - The verify half that exists today:
    - [`src/forze_postgres/execution/lifecycle/document_schema.py:183-186,289`](../src/forze_postgres/execution/lifecycle/document_schema.py)
    - [`src/forze_postgres/kernel/catalog/validation/validate_schema.py:1534,1723`](../src/forze_postgres/kernel/catalog/validation/validate_schema.py)
    - the catalog reader [`src/forze_postgres/kernel/catalog/introspect/introspector.py`](../src/forze_postgres/kernel/catalog/introspect/introspector.py)
  - The guard that keeps the three copies of the durable DDL in step:
    [`tests/unit/test_durable_schema_docs.py`](../tests/unit/test_durable_schema_docs.py),
    over [`tests/support/durable_schema.py`](../tests/support/durable_schema.py)
  - The CLI root: [`src/forze_cli/app.py:61-63`](../src/forze_cli/app.py)
  - Other RFCs:
    - [RFC 0066](0066-storage-guarantees.md) §5.4: declared, validated, never created.
      This RFC keeps "never created".
    - [RFC 0077](0077-operation-cli.md) §4: it deferred "schema attestation and migrations"
      to their own CLI group.
    - [RFC 0029](0029-managed-warehouse-relations.md): attestation before creation, for the
      analytics plane.
  - [`CONTRIBUTING.md`](../CONTRIBUTING.md) → Changelog: the rule that keeps
    **Migration:** SQL in entries.
- **Origin:** A survey of five consumer repositories, made read-only on 2026-11-03 (§2):
  - bi-backend, erp-backend, eis-backend and ai-stylist (Morze);
  - hh-backend-v2 (HoldemHub).

  Every one of them hand-writes DDL for forze's tables, and four of them write a new
  migration file on each forze upgrade that changes one.

---

## 1. Summary

Each table forze owns gets a declaration in `forze_postgres`. It lists:

- the columns, with a concrete type, nullability and default;
- the primary key;
- the indexes, identified by their shape and graded required or recommended;
- the index and column shapes forze has retired, each with a comment saying when and why.

A declaration is a function of the adapter's config: a tenant-tagged table leads its scan
indexes with `tenant_id`, and in P2 turning on `hlc_ordering` or consume storage changes the
outbox's. The declaration is the only copy: docstrings point at it, and a test
checks each reference page's SQL against it.

`forze schema sql --deps app.wire:postgres` renders the `CREATE` statements for every owned
table that deps module binds, qualified with the configured relation names.

`forze schema diff` reads the catalog, connected as a read-only database role, and prints
the forward delta: tables to create, columns to add, indexes to build `CONCURRENTLY`
(matched by shape, not by name), and retired indexes to drop *under the name the database
actually gave them*. Anything it cannot do safely, or cannot tell apart from the app's own
index, comes out as a commented refusal. Every identifier it prints is quoted. The consumer writes that
output into its next Flyway file. With `--check`, CI fails a forze bump that has no
migration.

In P2, a startup step runs the same comparison and refuses to start when a required table,
column or index is missing.

## 2. Motivation

**Every consumer transcribes forze's DDL, on every upgrade that touches it.**

| Consumer | forze | Runner | forze-owned DDL it wrote |
|---|---|---|---|
| bi-backend | 0.21.0 | Flyway | baseline (`authn_*`, `tenancy_*`, `durable_*`); `0021` counters; `0027__forze_0_17.sql`; `0031__forze_0_18.sql` |
| erp-backend | 0.20.0 | Flyway | baseline (idempotency, audit, `durable_*`, 9 `authz_*`, history); `0006` audit `actor_ids` (0.10); `0035` coalesce (0.17); `0036` realtime mailbox; `0038` claim index (0.18) |
| eis-backend | 0.20.0 | Flyway | `0032__identity.sql` (18 identity tables); a history template and its triggers |
| hh-backend-v2 | 0.15.1 | Flyway | `0012`–`0014` identity (19 tables); `0023`–`0024` outbox; `0085` principal epochs; one design RFC per forze bump |
| ai-stylist | 0.17.0 | own `migrate.py` | `0004__durable_jobs.sql` (transcribed from the 0.15.1 source); `0013` key rewrite |

None of the five uses `forze_mongo`. Four run Flyway, and ai-stylist runs its own
106-line runner. That runner has a checksummed `schema_migrations` table, a session advisory
lock and one transaction per file.

**One forze change, three hand-written variants.** CHANGELOG 0.18.0 replaced the Postgres
recovery indexes `(status, created_at)` and `(tenant_id, status, created_at)` with
`(status, name, created_at)` and its tenant variant (`CHANGELOG.md:128`). The three
consumers each wrote it differently:

- **bi** (`0031__forze_0_18.sql:6-7`) drops `durable_run_status_created_at_idx` and its
  tenant twin. These are the names Postgres generated for the unnamed `CREATE INDEX` the
  docs print.
- **erp** (`0038__durable_run_claim_index.sql:9`) drops its own `idx_durable_run_status`.
  It has no tenant variant.
- **ai-stylist** named the same index `ix_durable_run_scan` (`0004__durable_jobs.sql:21`),
  so it will need a third variant when it upgrades.

A migration that forze shipped as fixed text would fail on two of these three databases,
because the drop names an index that is not there.

**Transcription loses meaning.**

- **bi-backend** `0027__forze_0_17.sql:3-4` says "every enqueue writes the column, so it
  must exist". The adapter says the opposite: "Only this path names the column, so a table
  without it serves every other" (`run_store.py:287`).
- **hh-backend-v2** `0024__outbox_relay_columns.sql` exists because `0023` "was
  transcribed from `PostgresOutboxStore`'s **INSERT**". The claim half reads four more
  columns, and "the first relay run failed with `column "processing_at" does not exist`".
- **The 0.17 key rewrite** shipped with no statement. The entry says only that "rows
  written before keep the old form" (`CHANGELOG.md:406`). bi guards its `UPDATE` with
  `NOT LIKE ':%'` (`0027:13,17`). ai-stylist does not (`0013:4`), so running its statement
  twice would double the prefix.

**Learning that a migration is due is manual.** The consumers find out in four ways:

- bi and erp write header comments citing the forze version.
- hh-backend-v2 writes a design RFC for each bump (`rfcs/0027`, `0029`, `0031`), and keeps a
  hand-bumped readiness floor, `REQUIRED_MIGRATION = 85` (`app/src/wire/health.py:65`).
- eis-backend relies on startup validation. That covers its identity tables, but it never
  checks a store table.
- All of them read the CHANGELOG.

**The maintainer already writes this DDL each release.** It goes into a docstring, a page
and sometimes a CHANGELOG line, all by hand. Five consumers then each transcribe it again.
Writing the DDL once, in a form a program can diff against a live database, removes the
second step and keeps the cost of the first.

## 3. Current state

Verified against the source tree on 2026-11-03, after #816:

- **Forze creates no owned table.**
  - `integrations/postgres.md:169-171` says forze "never creates application tables", and
    RFC 0066 §5.4 makes "declared, validated, never created" a rule.
  - `PostgresSchemaTenantProvisioner` runs `CREATE SCHEMA IF NOT EXISTS`
    (`tenant_provisioner.py:206`), roles and grants. It leaves tables to the consumer.
  - The only runtime creation anywhere is the Mongo provisioner's marker collection,
    `_forze_tenants`.
- **The eleven store tables are declared nowhere except as text.**
  - Their configs sit on `PostgresDepsModule` (`module.py:268-332`): `outboxes`,
    `inboxes`, `idempotencies`, `counters`, `hlc_checkpoint`, `principal_epochs`,
    `durable_step`, `durable_run`, `durable_schedule` and `rotating_credentials`. The
    outbox's delivery log is `PostgresOutboxConfig.delivery_log`.
  - Each relation is a `RelationSpec`: either a static `(schema, name)` or a tenant-scoped
    resolver (`contracts/resolution/specs.py:11`).
  - Their DDL exists only as docstring text (the Related links) and on pages:
    - `durable-schema.md` and `outbox-schema.md`;
    - `integrations/authn.md:117` for principal epochs;
    - `recipes/rotating-oauth-credentials.md:25`;
    - fragments: the counters' `expires_at` `ALTER` (`integrations/postgres.md:187`) and
      purge index (`:222`), and the idempotency `owner` `ALTER`
      (`writing-operation/idempotency.md:83`).
  - The inbox has no DDL on any page.
- **The copies drift.**
  - **Durable:** three copies of the DDL (the docstrings, the page and
    `tests/support/durable_schema.py`) are kept in step by
    `tests/unit/test_durable_schema_docs.py`.
  - **Outbox:** no test guards it, and its copies already disagree. The docstring's base
    claim index is `(outbox_route, status, created_at)` (`outbox.py:73`). The page's is
    `(outbox_route, status, available_at, created_at)` (`outbox-schema.md:39-40`).
  - **The durable page's own "Migrations" section** (`durable-schema.md:132-160`) covers
    purge, coalesce and 0.6.0 cancellation. It does not cover the 0.18 index replacement.
- **The CHANGELOG is the delta channel, and its form varies.**
  - `CONTRIBUTING.md` asks entries to "always preserve … any **Migration:** SQL".
  - Prose with no statement: `CHANGELOG.md:106,128,160,250,1240,1957,2055`.
  - "see docs": `:1064`, which marks a **Required migration** without the SQL.
  - A stored-format change with no statement at all: `:406`.
- **The verify half covers document specs only, and only when opted in.**
  - `postgres_document_schema_validation_lifecycle_step` (`document_schema.py:289`) is
    registered only when `PostgresLifecycleModule.schema_specs` is non-empty
    (`lifecycle/module.py:75,117-123`).
  - It checks every bound `rw_documents`/`ro_documents` route, gathered by the loop at
    `document_schema.py:183-186` and walked at `:199`:
    columns, type family, nullability, tenant column, guarantee indexes, `EXCLUDE`,
    foreign keys and triggers (`validate_schema.py:1534`).
  - Framework-owned specs run with `warn_undeclared=False` (`document_schema.py:212`).
  - No store table is ever checked. A missing store column surfaces in one of two ways:
    - at first use (hh's `processing_at`);
    - or through a lazy probe. Idempotency probes for its `owner` column
      (`idempotency.py:159`), and counters probe for `expires_at` (`counter.py:156`).
- **The catalog reader has most of what a diff needs.**
  - `PostgresIntrospector` reads relations, column types, primary keys, foreign keys,
    exclusion constraints, and valid unique indexes with their predicates
    (`introspector.py:405,537,561`).
  - It reads a single index by name (`:1221`).
  - It has no listing of every index on a relation, which recommended non-unique indexes
    need.
  - Type-family comparison exists (`validate_schema_types.py:125`).
- **Framework-owned document specs.**
  - **Identity:** 19 specs (`forze_identity/inventory.py`). DDL is published for 5 of them
    (`recipes/provision-identity-postgres.md:39,61,81,102,121`). The other 14 "follow the
    same mapping rule" (`:132-135`).
  - **Audit:** `forze_kits/integrations/audit/record.py:95`, with DDL in
    `identity-tenancy-enc/audit.md`.
  - **Progress jobs:** DDL in `running-in-prod/operation-progress.md`.
  - **Realtime mailbox and cursors, stored files and history:** no DDL published.
- **The CLI has no schema group.** `forze_cli/app.py:61-63` registers `ops`, `dst` and
  `mock`. RFC 0077 §4 sent "schema attestation and migrations" to "their own groups if they
  ship".
- **No table carries a version.** The only version constants in the tree belong to archive,
  protocol and crypto formats.

## 4. Goals / Non-goals

**Goals**

- A consumer upgrading forze runs one command against a database and gets the SQL for every
  forze-owned table, whatever version that database was last migrated to, and whatever its
  indexes are named.
- One copy of each owned table's DDL. The adapters' integration suites run against it, so a
  declaration missing a column an adapter reads fails CI.
- CI can tell that a forze bump is missing its migration.
- The SQL fits the tools consumers already run: Flyway, or a plain-SQL runner.

**Non-goals**

- **Not applying DDL.** Forze prints SQL, and the consumer's tool applies it. Applying it
  from forze would put a second history beside Flyway's and take locks nobody scheduled
  (§5.6).
- **Not the application's own tables.** Forze does not own their column types. hh-backend-v2
  changed a forze column to `citext` (`0053`), and that is the app's call.
- **Not down-migrations.** None of the consumers writes them, and Flyway Community has
  none.
- **Not a version history of forze's schema.** The diff compares against the live catalog.
  It never asks "which forze version is this database at", a question nothing records.

## 5. Design

### 5.1 The declaration

One per owned table, beside its adapter, built from the adapter's config. The P1 shape:

```python
@attrs.define(frozen=True, kw_only=True)
class OwnedColumn:
    name: str
    type: str                   # concrete DDL type: "text", "timestamptz", "uuid", "jsonb", "bigint"
    nullable: bool = True
    default: str | None = None  # a constant SQL expression, never a volatile one
    required: bool = True       # False: only a feature reads it (coalesce_key, owner, expires_at)
    comment: str = ""           # one line, printed above the statement


@attrs.define(frozen=True, kw_only=True)
class OwnedIndex:
    keys: tuple[str, ...]       # columns or expressions, in order
    unique: bool = False
    where: str | None = None
    required: bool = False      # correctness (a unique claim) vs speed (a scan)
    comment: str                # "recommended since 0.18.0: the recovery scan filters …"


@attrs.define(frozen=True, kw_only=True)
class Retired:
    shape: OwnedIndex | OwnedColumn
    comment: str                # "retired in 0.18.0, replaced by (status, name, created_at)"


@attrs.define(frozen=True, kw_only=True)
class OwnedRelationShape:
    owner: str                  # "PostgresDurableRunStore"; printed in the header
    columns: tuple[OwnedColumn, ...]
    primary_key: tuple[str, ...]
    indexes: tuple[OwnedIndex, ...]
    retired: tuple[Retired, ...] = ()
```

In P1 the release a shape arrived or retired in is free text in `comment`. P2 replaces it
with structured fields: `since` on columns and indexes, and `retired_in` on `Retired`. Only
the P2 declaration-level rule below needs them.

`owned_relations(deps: PostgresDepsModule) -> tuple[BoundOwnedRelation, ...]` walks the store
configs on the deps module (`module.py:268-332`). It pairs each configured relation with the
shape for that config. In P1 it reads `durable_run`, `durable_step` and `durable_schedule`,
where the switch is a tenant-tagged table leading its scan indexes with `tenant_id`. P2 adds
the other eight configs and their switches:

- `hlc_ordering=True` adds `hlc` and its claim index;
- `storage="consume"` drops `published_at`, as `outbox-schema.md:194` documents. It drops
  `processing_at` only when stale reclaim is also off (`reclaim_stale_after=None`): the relay's
  `reclaim_stale_processing` reads and clears that column in either storage mode, and the
  default config has reclaim on.

A tenant-scoped relation (a resolver, not a tuple) has no single table. P1 skips it with a
warning, as document validation skips dynamic routes (`document_schema.py:217-226`). P2 binds
it for each tenant given with a repeated `--tenant`.

**Safe additions.**

1. The diff prints `ADD COLUMN` only for a column that is nullable or has a constant default.
   That is a catalog-only `ALTER` on Postgres 11 and later, with no table rewrite. Any other
   missing column comes out as a commented refusal. P1 enforces this when it prints.
2. No default is volatile. A unit test checks every declaration.
3. P2 adds the declaration-level rule, checked by a unit test over `since`: a column added
   after its table's first release is nullable or has a constant default.

A change that cannot meet these rules is a breaking change of a different kind. It needs a
data step (§5.5), and its CHANGELOG entry says so.

### 5.2 `forze schema sql`: a fresh install

```text
forze schema sql --deps app.wire:postgres_module          # P2 adds [--tenant ID ...]
```

`--deps` names a `PostgresDepsModule`, or a sequence that contains one. The command prints, for
each bound relation, `CREATE TABLE` followed by its required and recommended indexes, grouped
under a header naming the owner and the forze version. It needs no database. Its output is
the consumer's first forze migration, and the page's SQL blocks are tested against it (§6).

### 5.3 `forze schema diff`: an upgrade

```text
FORZE_SCHEMA_DSN=postgresql://… forze schema diff --deps app.wire:postgres_module [--check]
# P2 adds [--tenant ID ...] [--keep-retired]
```

The command reads the catalog and compares each bound relation with its shape:

| Found in the catalog | Printed |
|---|---|
| Table missing | `CREATE TABLE`, then every declared index |
| Primary key missing, or on other columns than declared | A commented refusal naming both key column lists; `--check` fails. A table without the declared key cannot serve the adapter's `ON CONFLICT (<key>)` |
| Column missing, nullable or with a constant default | `ALTER TABLE … ADD COLUMN` |
| Column missing, `NOT NULL` with no constant default | A commented refusal (§5.1) |
| Column type family differs | A commented refusal naming both types. Never `ALTER … TYPE` |
| Declared `NOT NULL`, found nullable | A commented refusal with a commented `SET NOT NULL` |
| Declared index absent, matched by shape | `CREATE [UNIQUE] INDEX CONCURRENTLY ON …` with its comment |
| Index invalid (`indisvalid = false`), no build in progress | `DROP INDEX CONCURRENTLY <found name>`, then the create |
| Index invalid while a `CREATE INDEX` on the table is in progress (`pg_stat_progress_create_index`) | A warning, and nothing for that index: another session may still be building it |
| An index that is a retired match (below) | `DROP INDEX CONCURRENTLY <found name>`, after every create |
| An index with a retired shape's keys that is not a retired match | A commented refusal naming the index and the condition it failed |
| A retired column present | A commented `-- ALTER TABLE … DROP COLUMN …`: it holds data |
| A column or index neither declared nor retired | Nothing: it belongs to the app |

**A retired match needs all four conditions.** Otherwise the diff prints the refusal and
drops nothing:

1. the shape is a `Retired` index in forze's declaration;
2. the index is not unique;
3. no constraint is backed by it (no `pg_constraint.conindid` points at it);
4. its predicate is exactly the retired one: absent when the retired shape has none, and
   equal under the row 10 comparison when it has one.

These conditions keep row 6's promise for any index the catalog can tell apart from
forze's. An app index with identical keys, uniqueness and predicate cannot be told apart,
and §9 accepts that case.

**Read-only is the role, not the transaction.** The diff opens a `READ ONLY` transaction,
but the session could switch it back with `SET TRANSACTION READ WRITE`, so that is a guard
against forze's own mistakes and not a boundary. The guarantee is the database role: the
how-to and the CLI help require `FORZE_SCHEMA_DSN` to name a role with `USAGE` on the
schemas and catalog read access, and no write privilege. The diff only prints. It never
executes what it prints (row 1).

**The output is SQL the consumer applies, so nothing from the catalog reaches it raw.**

- Every identifier the diff prints is quoted with `quote_ident` semantics: `psycopg.sql.Identifier`,
  or the server's `quote_ident()`. That covers schema, table, index and column names, and
  type names read back from the catalog. A plain lower-case name stays bare, as in the
  example below. A name containing a newline, `;`, a quote or upper case comes out
  double-quoted with inner quotes doubled.
- Comment text, whether from the declaration or naming a catalog object in a refusal, has
  CR and LF stripped before `-- ` is prepended. A name cannot end the comment and start a
  statement.

For bi-backend on 0.17, the 0.18 upgrade prints:

```sql
-- forze 0.18.0 · bi.durable_run (PostgresDurableRunStore)
-- Shape only: data rewrites are announced as **Migration:** SQL in CHANGELOG.md.
-- CONCURRENTLY cannot run in a transaction (Flyway: executeInTransaction=false).

-- recommended since 0.18.0: the recovery scan filters the functions a worker claims
CREATE INDEX CONCURRENTLY ON bi.durable_run (status, name, created_at);
CREATE INDEX CONCURRENTLY ON bi.durable_run (tenant_id, status, name, created_at);

-- retired in 0.18.0, replaced by (status, name, created_at)
DROP INDEX CONCURRENTLY bi.durable_run_status_created_at_idx;
DROP INDEX CONCURRENTLY bi.durable_run_tenant_id_status_created_at_idx;
```

Against erp-backend the same command drops `erp.idx_durable_run_status`, and against
ai-stylist it drops `ix_durable_run_scan`. Each drop uses the name the catalog holds, which
is the case fixed migration text cannot handle (§2). All three are plain non-unique
indexes with no predicate and no constraint behind them, so each one is a retired match.

Two more behaviours:

- **Exit codes.** The exit code says whether the database is in sync, a delta was printed,
  or a refusal needs a person. With `--check` the command prints nothing and only exits, so
  a CI job against the migrated test database fails a forze bump that has no migration.
- **Names.** Indexes are created unnamed, as the pages print them. Their identity is their
  shape, so the name Postgres picks never matters to a later diff.

### 5.4 The verify half: a startup step (P2)

`postgres_owned_schema_validation_lifecycle_step` runs the §5.3 comparison at startup. It
refuses when:

- a table is missing;
- a `required` column is missing;
- a type family differs;
- a `required` index is missing.

It warns when a recommended index or a non-required column is missing, or when a retired
index is still present. It is opt-in, like the document step, and registered from
`PostgresLifecycleModule`.

The lazy probes (`idempotency.py:159`, `counter.py:156`) stay: a table without the optional
column keeps serving every call that does not need it, and the step only warns.

In the same phase, `sql` and `diff` cover framework-owned document specs. Their columns are
rendered from the model by the mapping rule `provision-identity-postgres.md` already
publishes, their guarantee indexes by the existing suggestion builder
(`validate_schema.py:1036-1063`), and their history relation from the `DocumentHistory`
model (`src/forze/domain/models/document.py:612`). Their verify half already exists.

### 5.5 Data steps (P3, demand-gated)

A stored-format change, such as 0.17's `:{key}` rewrite, changes no shape, so the diff cannot
see it. P3 would add `DataStep(since, relation, detect_sql, apply_sql)`. The diff would run
`detect_sql`, which is read-only, and print `apply_sql` only when rows need it. That makes
the step independent of versions, the same way the shape diff is.

Until P3 ships, a stored-format change on an owned table ships an idempotent rewrite
statement in its CHANGELOG **Migration:** entry. The statement must be guarded so that a
second run is a no-op, as bi's `NOT LIKE ':%'` is.

### 5.6 Alternatives considered

- **Fixed migration scripts per release** (`forze_postgres/migrations/0.18.0/*.sql`, plus a
  `--from/--to` emitter).
  - For: needs no database, gives reviewable files, and is the familiar shape.
  - Against: it assumes the database matches forze's canonical shape at `--from`, and none
    of the surveyed ones does. Three names exist for one index. Versions range from 0.15.1
    to 0.21.0, which needs a chain. And nothing records which forze version a database is
    "at": Flyway records the consumer's file, not forze's release. The diff reads that state
    where the scripts would assume it.
- **Forze applies DDL at startup** (RFC 0018's `ensure_table`, or RFC 0029's `evolve`).
  - For: no consumer work at all.
  - Against:
    - every replica takes production locks at deploy, and the replicas race each other;
    - the consumer's migration history no longer matches the database, so its next Flyway
      file fails on "already exists";
    - it contradicts RFC 0066 §5.4 and `postgres.md:169-171`.
  - Named as an escape hatch in §8.
- **Forze ships a migration runner with its own history table**, as ai-stylist built.
  - For: complete.
  - Against: four of five consumers would run a second migration system beside Flyway.
    App migrations alter forze's tables, so the ordering between the two runners becomes a
    cross-tool problem. hh changes a forze column to `citext` (`0053`) and tunes the
    outbox's autovacuum (`0084`).
- **Alembic autogenerate.**
  - For: autogenerate exists.
  - Against: none of the five uses Alembic. ai-stylist moved off it (`ALEMBIC_HEAD`
    adoption in `migrate.py`). It would also add SQLAlchemy models that forze does not
    have.
- **Publish the expected schema as JSON for the consumer's own diff tool** (migra, Atlas).
  - For: the smallest change forze could make.
  - Against: none of the consumers runs a diff tool, and turning the JSON into DDL is the
    part that saves work. The declaration *is* that export, so exposing it as JSON later is
    a cheap addition.
- **The status quo with better pages** (one upgrade page per release).
  - For: no code.
  - Against: this is what exists now. Three copies, one of which has drifted, and every
    consumer still transcribes by hand.

## 6. Tests

- **Unit.**
  - Each declaration renders.
  - No declared default is volatile. In P2, a column added late (by `since`) is nullable
    or has a default.
  - Each row of the §5.3 table, over a recorded catalog.
  - `owned_relations` honours each config switch: tenant tagging in P1; `hlc_ordering`,
    consume storage and `delivery_log` in P2.
- **Quoting.**
  - A catalog fixture whose schema, table, index, column and type names contain a newline,
    `;`, `--`, a double quote and upper case renders each one quoted.
  - The printed file parses as exactly the statements the diff meant to print: an injected
    `DROP TABLE` never appears outside a quoted identifier.
  - Comment text carrying CR or LF comes out as one comment line.
- **One copy.**
  - `tests/support/durable_schema.py` is replaced by the rendered SQL, and every
    integration test that creates a durable table takes its DDL from `forze schema sql`.
    P2 does the same for the other store tables.
  - The adapters' existing suites then run against the declared schema. In P2, a
    declaration missing a column the outbox claim path reads fails here, which would have
    caught hh's `processing_at` before release.
  - `test_durable_schema_docs.py` becomes "the reference page's SQL equals the render" for
    the durable tables in P1. P2 extends it to every owned table, and the outbox drift then
    fails it until it is fixed.
- **Upgrade (Postgres via testcontainers).**
  - Each release snapshots the rendered `sql` output into
    `tests/support/schema_snapshots/<version>.sql`.
  - A database created from the previous snapshot, with `diff` applied, then shows an
    empty `diff`. From P2 it also passes the startup step.
  - A retired index created under a hand-picked name (`idx_durable_run_status`) is dropped
    under that name.
  - Indexes with the retired keys that fail a §5.3 condition each produce a refusal and no
    `DROP`: one unique, one backing a constraint, one with a different predicate.
  - An invalid index left by a failed `CONCURRENTLY` is dropped and recreated.
  - While a second session holds a `CREATE INDEX CONCURRENTLY` open on the table, its
    still-invalid index produces a warning and no statement.
- **Read-only role.**
  - `diff` runs against a role with `USAGE` and catalog access and no write privilege, and
    produces the same output as it does under a superuser.
  - Under that role, a write attempted even after `SET TRANSACTION READ WRITE` fails.
- **CLI.**
  - The exit codes and `--check`.
  - The DSN is read from the environment and refused as a flag.
  - A tenant-scoped relation is skipped with a warning (P1). With `--tenant`, it is bound
    for that tenant (P2).
- **Not tested:** Flyway itself. The output is plain SQL, and the CONCURRENTLY rule is
  stated in its header.

## 7. Docs

- **A new how-to, "Upgrading forze's tables"** (`running-in-prod/`):
  - create a read-only role for the diff (a required step, with the `GRANT`s);
  - bump forze;
  - run `forze schema diff` into the next migration file and review it;
  - `executeInTransaction=false` for CONCURRENTLY;
  - the `--check` CI job.

  It must say plainly that forze prints SQL and never runs it, that the role, not the
  transaction, is what keeps the diff from writing, and that the diff covers shape only
  until P3.
- **`durable-schema.md`** (P1) and **`outbox-schema.md`** (P2) keep their prose. Their SQL
  blocks become test-checked renders, and "you create and migrate them" becomes "you apply
  them; `forze schema` writes them". The hand-kept "Migrations" sections go: the diff
  replaces them.
- **A CLI reference entry for `forze schema`**, beside `reference/ops-cli.md`. The command's
  `--help` states the read-only role requirement too.
- **`CONTRIBUTING.md` → Changelog:**
  - a change to an owned table edits its declaration in the same PR;
  - the entry names the table and says "run `forze schema diff`";
  - a stored-format change carries a guarded rewrite statement (§5.5).

## 8. Out of scope

- **Applying DDL**, whether through `forze schema apply` or an opt-in `evolve` mode at
  startup. The consumer's tool owns the history. Revisit this if a consumer without a
  migration tool appears.
- **Running the rendered `CREATE` inside `PostgresSchemaTenantProvisioner`** for a new tenant
  schema. That is forze executing DDL, even if at an explicit call. It is the natural follow-up
  for schema-per-tenant deployments of store tables; build it on the first one.
- **The application's own document tables.** The P2 renderer would make this a small step,
  but the column types are the app's choice. Demand-gated.
- **Mongo.** The declaration's index vocabulary (keys, unique, partial predicate) is what
  `createIndex` needs, so a renderer can be added later without changing it. Build it when a
  consumer wires `forze_mongo` stores. None of the five surveyed does.
- **Removing the lazy column probes.** That only becomes possible once the P2 startup step is
  in place and adopted. It is a separate change.

## 9. Risks

- **A shape match that misses.** Postgres stores predicates normalized
  (`status = 'pending'` reads back as `(status = 'pending'::text)`). A naive comparison
  would report a present index as missing and print a duplicate. Mitigations: compare
  through the catalog's own rendering, reuse the guarantee matcher that already compares
  predicates (`introspector.py:561`), and test every declared predicate against real
  Postgres (row 10).
- **A retired index the app relies on.** The four §5.3 conditions refuse every index the
  catalog can tell apart from forze's. An app index with exactly the retired shape cannot be
  told apart, and the diff drops it. This is accepted: a drop loses no data, the index can be
  rebuilt, drops come last and carry a label, and the consumer reviews the file before
  applying it. In P2, `--keep-retired` leaves drops out.
- **SQL injection through catalog names.** A name with a newline or `;` would inject
  statements into a file the consumer applies with migration privileges. Every identifier
  is quoted and comments are stripped of CR/LF (§5.3, row 16), and a test feeds hostile
  names through the renderer (§6).
- **A concurrent index build read as a failed one.** `indisvalid = false` is also the state
  of an index another session is still building `CONCURRENTLY`. Dropping it would break
  that build. The diff checks `pg_stat_progress_create_index` and only warns while a build
  runs (row 17). A build that starts after the diff has read the catalog is outside what a
  printed file can know. The consumer applies the file later, under its own tool.
- **A read-only transaction mistaken for a boundary.** The session can switch a `READ ONLY`
  transaction back to read-write. The read-only role is the boundary, and the how-to and
  `--help` require it (row 7).
- **One wrong declaration reaches every consumer.** Today each consumer makes its own
  mistakes. With one copy, one mistake ships to everyone. Mitigation: the adapters' suites
  run against the render (§6), so the failure has to get past forze's CI first.
- **Read as "forze migrates my database".** It does not. The output header, the how-to and
  the exit codes all say that the SQL is the consumer's to apply.
- **A noisy first diff.** An existing deployment sees its accumulated drift on the first
  run, such as recommended indexes it never built (erp has no tenant variant). The drift is
  real, so this is accepted.
- **Shape-only coverage read as complete.** Until P3, a stored-format change is visible only
  in the CHANGELOG. Every diff header says so.

## 10. Unresolved questions

1. **The beta rule (row 2).** Does the maintainer accept the amendment? Without it, today's
   rule stands. The declaration still removes the drift, and the tests that create tables
   from it force any column an adapter reads through it. But a release may change an index
   or a stored format without touching the declaration, and the diff is then only as
   current as the maintainer's discipline. P1 does not depend on the answer. Row 2 stays
   `OPEN` until the maintainer decides.
2. **Exit codes.** The numbers for in-sync, delta and refusal, aligned with RFC 0077's
   contract. Implementation decides (row 12).
3. **Predicate and expression matching.** Through the catalog's rendering of a scratch
   index, or through normalized text. Implementation decides by the integration test
   (row 10).
4. **Tenant-scoped relations (P2).** Repeated `--tenant`, or a tenant source the command
   reads. P1 skips such relations with a warning. Implementation decides in P2, with
   `--tenant` as the minimum (row 11).
5. **Naming the P2 switch** on `PostgresLifecycleModule`. Implementation decides (row 13).

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Forze never executes DDL against an owned table: not at startup, not from the CLI, and not during tenant provisioning. It prints SQL, and the consumer's migration tool applies it. Applying from forze would split the migration history and take unscheduled production locks. Reversing this reopens RFC 0066 §5.4. |
| 2 | `LOCKED` | Accepted by the maintainer, 2026-11-04 (§10 Q1): the beta rule is amended, not dropped. Forze still ships no runtime migration code: no dual reads, no transition shims, no DDL it runs. What changes: every change to an owned table's shape lands as an edit to its declaration in the same PR, and a stored-format change ships a guarded rewrite statement in its CHANGELOG entry. The declaration edit replaces the docstring edit made today. |
| 3 | `LOCKED` | The delta is computed against the live catalog, never as fixed text between two forze versions. Surveyed databases name one index three ways and span 0.15.1 to 0.21.0, and nothing records which forze version a database is at. |
| 4 | `ASSUMED` | Indexes are identified by shape (keys in order, uniqueness, predicate) and created unnamed. A retired index is dropped under the name the catalog reports, and only on a retired match: a forze-declared retired shape, not unique, backing no constraint, with exactly the retired predicate. Any other index with the retired keys gets a refusal comment. |
| 5 | `ASSUMED` | One declaration per owned table, built from the adapter's config, is the only copy. Docstrings point at it, reference-page SQL is tested against its render, and integration tests create tables from it. |
| 6 | `ASSUMED` | The diff prints only safe statements: `CREATE TABLE`, `ADD COLUMN` (nullable or with a constant default, checked as it prints), and `CREATE`/`DROP INDEX CONCURRENTLY`. A type change, a tightened nullability, a column drop, an unsafe added column or an ambiguous retired index comes out as a commented refusal. Undeclared columns and indexes are never touched. |
| 7 | `ASSUMED` | The diff's guarantee against writing is a read-only database role, which the how-to and `--help` require. Its `READ ONLY` transaction guards only against forze's own mistakes, since the session can switch it back. It takes its DSN only from `FORZE_SCHEMA_DSN` (a flag would leak it to shell history and `ps`), and loads relation names from a `--deps module:attr` naming a `PostgresDepsModule`. It does not reuse `OpsApp`, which needs a registry and an identity this command does not use. |
| 8 | `ASSUMED` | P1 covers only the three durable tables, where the observed 0.18 pain was. It ships `sql` and `diff` over table, column, index and retired shapes, with free-text comments in place of `since`. P2 adds the other eight store tables (outbox, delivery log, HLC checkpoint, inbox, idempotency, counters, principal epochs, rotating credentials), structured `since`/`retired_in`, `--keep-retired`, repeated `--tenant`, the startup step, and framework-owned document specs, whose verify half already exists. |
| 9 | `ASSUMED` | Postgres only. Mongo is demand-gated: none of the five surveyed consumers wires `forze_mongo`, and its schema-free delta reduces to indexes. |
| 10 | `OPEN` | How a declared predicate or expression index is matched against the catalog: a rendered scratch index or normalized text. Settle it by the integration test over every declared predicate, and log the choice. |
| 11 | `OPEN` | How a tenant-scoped `RelationSpec` is resolved in P2: a repeated `--tenant`, or a tenant source. `--tenant` is the minimum. P1 skips such relations with a warning. Decide and log. |
| 12 | `OPEN` | The exit-code numbers for in-sync, delta and refusal. Align them with RFC 0077's contract, then decide and log. |
| 13 | `OPEN` | The name and shape of the P2 opt-in on `PostgresLifecycleModule`. Decide and log. |
| 14 | `ASSUMED` | The P2 startup step refuses on a missing table, a missing required column, a type mismatch or a missing required index. It warns on everything else and leaves the lazy probes in place. |
| 15 | `ASSUMED` | Data rewrites are not detected until P3. Until then they ship as guarded **Migration:** SQL in the CHANGELOG, and every diff header says the output covers shape only. |
| 16 | `LOCKED` | Nothing read from the catalog reaches the printed SQL raw. Every identifier (schema, table, index, column, type) is quoted with `quote_ident` semantics, and comment text has CR and LF stripped. The consumer applies the file with migration privileges, so a departure here is an injection. |
| 17 | `ASSUMED` | An invalid index is dropped and recreated only when no `CREATE INDEX` is in progress on its table (`pg_stat_progress_create_index`). Otherwise the diff warns and prints nothing for it, because `indisvalid = false` is also the state of another session's running `CONCURRENTLY` build. |

## 12. Phasing

- **P1:** the smallest slice that removes the observed pain, the 0.18 index case.
  - §5.1 declarations for `durable_run`, `durable_step` and `durable_schedule`, with
    free-text comments in place of `since`.
  - §5.2 `sql` and §5.3 `diff`, with `--check`, over table, column, index and retired
    shapes, including the retired-match conditions, the quoting rule and the active-build
    check.
  - The introspector's listing of every index on a relation.
  - The durable tests created from the render, and the durable page test.
  - The how-to (read-only role included) and the CONTRIBUTING rule.
  - One PR.
- **P2:** gated on P1.
  - Declarations for the other eight store tables (outbox, delivery log, HLC checkpoint,
    inbox, idempotency, counters, principal epochs, rotating credentials), with their
    config switches. The generalized page test then fixes the outbox drift.
  - Structured `since`/`retired_in` and the declaration-level rule over them.
  - `--keep-retired`, and repeated `--tenant` (row 11).
  - The §5.4 startup step.
  - `sql`/`diff` over framework-owned document specs.
- **P3:** demand-gated. §5.5 data steps, a Mongo renderer, and provisioner-run DDL for
  schema-per-tenant store tables.
