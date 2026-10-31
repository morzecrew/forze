---
title: Document
icon: lucide/file-text
summary: The document contract — its spec, the query/command ports, and every method
---

The document contract is Forze's CRUD aggregate store: a `DocumentSpec` binds a read
model (and optional write models) to a logical name, and the context resolves a **query**
port (reads) and a **command** port (writes) for it. The conceptual model is in
[Reading data](../../data-events/reading-data.md) and the
[writing-operation](../../writing-operation/wiring.md) chapter; this is the exhaustive
surface.

```python
q = ctx.document.query(spec)    # reads
c = ctx.document.command(spec)  # writes
```

All methods are `async`. Naming is systematic: `get*`/`find*` return the read
model `R`; `project*` returns a `JsonDict` of selected fields; `select*` validates
into a caller-supplied `return_type`. The suffix sets the result container —
none → `CountlessPage` (no total), `_page` → `Page` (with `.count`), `_cursor` →
`CursorPage` (keyset), `_stream` → an async generator of batches. The `filters`,
`sorts`, and `aggregates` arguments follow the [query DSL](../query-syntax.md).

## Spec

`DocumentSpec[R, D, C, U]` — binds the read model and optional write types to a name,
plus per-aggregate policy:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | logical name; the default namespace and route key |
| `read` | `type[R]` | required | the read model returned from queries |
| `write` | `DocumentWriteTypes \| None` | `None` | `{domain, create_cmd, update_cmd?}`; **omit for a read-only document** (no command port) |
| `history_enabled` | `bool` | `False` | keep an audit trail of every revision |
| `hard_delete` | `bool` | `True` | `False`: no document operation erases a row — no `kill` operation is generated and the command port refuses `kill`/`kill_many`. A provisioner's `drop_on_deprovision=True` and raw statements are outside the port and can still delete |
| `materialized` | `frozenset[str]` | `∅` | `@computed_field` names persisted as columns, so they're filterable/sortable |
| `read_conformity` | `"strict" \| "lenient"` | `"strict"` | `"lenient"` auto-derives `lenient_read_fields` from the read model (every statically-defaulted, non-identity, non-`materialized` field); explicit fields are added on top |
| `lenient_read_fields` | `frozenset[str]` | `∅` | read-model fields with **no** backing column: dropped from the projection, hydrated from their default, removed from the filter/sort/aggregate allow-sets, and tolerated by relational startup schema checks (see below) |
| `write_omit_fields` | `frozenset[str]` | `∅` | domain fields with **no** column: **silently stripped** from every write and hydrated from the domain default on read-back (the write-side of `lenient_read_fields`; explicit-only, requires `write`) |
| `default_sort` | `QuerySortExpression \| None` | `None` | sort applied when a caller omits `sorts` (required if the read model has no `id`) |
| `query_policy` | `QueryFieldPolicy \| None` | `None` | allow-sets restricting which fields a governed caller may filter / sort / aggregate |
| `filter_limits` | `QueryFilterLimits \| None` | `None` | bounds on the filters the document's queries accept, on every backend and on generated routes; `None` keeps the defaults (see [Limits](../query-syntax.md#limits)) |
| `query_params` | `type[BaseModel] \| None` | `None` | typed [query-parameter](../../data-events/query-parameters.md) contract, bound via `with_parameters` |
| `encryption` | `FieldEncryption \| None` | `None` | field-level [encryption](../../identity-tenancy-enc/encryption.md) policy (share the same object with the table's `SearchSpec`) |
| `cache` | `CacheSpec \| None` | `None` | read-through [cache](../../data-events/caching.md) for `get` |
| `sensitive` | `bool` | `False` | read model carries secrets; generated HTTP/MCP surfaces refuse to project it |
| `codecs` | `DocumentCodecs \| None` | `None` | codec overrides (auto-derived from the model types by default) |
| `guarantees` | `Sequence[StorageGuarantee]` | `()` | properties the store must enforce at write time (see below) |

`write` is a `DocumentWriteTypes` TypedDict — `domain` (the `Document` subclass),
`create_cmd`, and an optional `update_cmd`.

### Query policy

`QueryFieldPolicy` takes three allow-sets of read-model field names: `filterable`,
`sortable` and `aggregatable`. Each one is `None` by default, which allows every read
field. The generated document list operations enforce the policy, and so does every
HTTP, MCP or agent-tool surface built on them. The query port does not, so internal code
is not restricted. A search spec has no query policy. A refused query raises
`field_not_filterable`, `field_not_sortable` or `field_not_aggregatable`.

On a document aggregate (`agg_list`), groups and measures are checked against
`aggregatable`, and each metric's `filter` against `filterable`. `$having` and `sorts`
name output aliases, so each alias is checked as the field it comes from, whatever it is
called:

- `$having` needs that field filterable, and a sort needs it sortable.
- A measure counts as its field: a `$having` on the `$sum` of `salary` needs `salary`
  filterable.
- A `$count` comes from no field, so it can always be filtered and sorted on.

### Lenient read fields

By default every read-model field must map to a stored column, and a relational
backend fails at startup if one is missing — drift is caught at boot, not on the
first query. `lenient_read_fields` opts named fields out of that rule, on the read
side only:

- the field is **dropped from the read projection** and **hydrated from its model
  default** on every read (so it carries the same default for every row);
- it is **removed from the filter / sort / aggregate allow-sets** — a column that
  is not there cannot be queried;
- the Postgres startup schema check **tolerates the missing column** instead of
  failing (Mongo and Firestore are schemaless, so they tolerate it naturally).

Honored on Postgres, Mongo, and Firestore. Each name must be a non-computed
read-model field that carries a default (is non-required) and is not an
identity/audit field (`id`, `rev`, `created_at`, `last_update_at`) or a
`materialized` field. It is read-side only: if the same field is also stored on the
write/domain model over that relation, Postgres write-schema validation still
requires its column.

Use it for a field that exists in code ahead of (or independently of) the physical
column — e.g. during an expand/contract migration — or a read-model display field
the write model does not persist.

Instead of listing fields, set `read_conformity="lenient"` to **auto-derive** the set:
every statically-defaulted, non-identity read field that is not `materialized` becomes
lenient (fields with a `default_factory` are excluded — declare those explicitly to
accept a fresh value per row). Explicit `lenient_read_fields` are always added on top.
`resolved_lenient_read_fields` is the effective set every backend reads.

`write_omit_fields` is the **write-side** counterpart: a domain field with no column is
silently stripped from every insert/update and hydrates from the domain default on
read-back (Postgres, Mongo, and Firestore). Because the value is dropped (not persisted),
it is **explicit-only** — never auto-derived — requires a `write` spec, and each name must
be a defaulted, non-identity domain field. Use it for a domain field that is computed or
stored elsewhere, not on this table.

### Storage guarantees

A guarantee is a property of the data that the **store** refuses to violate — not something
the framework checks after the fact. Declare it and a backend that cannot keep it refuses at
wiring:

```python
DocumentSpec(
    name="fact",
    read=Fact,
    write={"domain": FactDoc, "create_cmd": CreateFact},
    guarantees=(
        UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}}),
    ),
)
```

Two sentences carry the whole doctrine:

- **A guarantee is declared, validated and never created.** Nothing here issues DDL. An adapter
  that created an index would take a lock on a production table nobody asked it to take, and
  would hide a missing migration until the next deployment.
- **Your migration is the thing that satisfies it.** Startup checks that the mechanism is there
  and prints the statement that would create it.

| Member | Property | Postgres | Mongo | In-memory |
|--------|----------|----------|-------|-----------|
| `UniqueTogether(fields=…)` | at most one row per field tuple | unique index | unique index | ✅ |
| `UniqueTogether(fields=…, where=…)` | …among the rows the filter selects | partial unique index | `partialFilterExpression` | ✅ |
| `UniqueTogether(fields=…, skip_null=True)` | …exempting tuples holding a null | unique index (nulls distinct) | `partialFilterExpression` with `$type` | ✅ |
| `NonOverlapping(key=…, period=…)` | no two rows for one key hold overlapping [periods](../../core-concepts/domain-layer.md#a-period-and-which-end-is-in-force) | `EXCLUDE USING gist` | — | ✅ |
| `NonOverlapping(key=…, period=…, where=…)` | …among the rows the filter selects | `EXCLUDE … WHERE (…)` | — | ✅ |
| `SerializedBy(key=…)` | two writes for one key never run at once | `pg_advisory_xact_lock` | — | ✅ |
| `Reference(field=…, spec=…, on_delete=…)` | a hard delete of the referenced row cascades, is refused, or clears the field | `FOREIGN KEY … ON DELETE …` | — | ✅ |
| `Reference(fields=…, references=…, spec=…, on_delete=…)` | …through a composite key | `FOREIGN KEY (…, …) REFERENCES … (…, …)` | — | ✅ |

`SerializedBy` is the one member about write *ordering* rather than about stored rows. It says
two writes for one key never run at once — the rule that otherwise lives in whichever writer
remembered to take a lock, and holds until somebody adds the next writer.

On Postgres every write takes a transaction-scoped advisory lock per owner before it writes, held
until the transaction ends — so there is nothing to migrate. A write outside a transaction runs in
one of its own, so its lock still covers it. A write that names a row by id reads the row first to
learn its owner, which is one extra query, paid only by a spec that declares this. Two
transactions that each hold the owner the other wants are a deadlock Postgres detects: one is
refused as `concurrency`, which is retryable, and the in-memory store refuses the same cycle the
same way. Mongo has no advisory-lock mechanism and refuses the declaration.

A datetime key field locks on the instant, not on its offset: a create written in `+03:00` and an
update of the row Postgres reads back in the session's time zone take one lock. A naive datetime
is read as UTC, as Postgres stores it in a `timestamptz` column whatever the session's
`TimeZone`. An instant whose UTC spelling falls outside the years 1 to 9999 is refused as
`lock_key_instant_out_of_range`.

Those locks add up in a long transaction. A transaction holds one for each distinct owner it writes
under until it ends, and they all live in Postgres's shared lock table. Every session shares that
table, which holds `max_locks_per_transaction` × (`max_connections` + `max_prepared_transactions`)
locks. A request touches a few owners; a bulk load or a seed that writes under thousands in one
transaction can fill the table and fail with `out of shared memory`. Commit such a load in batches,
so a transaction holds only its own batch's owners.

It **serializes, it does not validate**. A write takes the lock just before it writes, which
covers the write and nothing before it. A handler that reads, decides and then writes takes the
lock first with `hold`, inside its transaction, so it decides on a read no other writer for
that owner can change:

```python
async with ctx.tx_ctx.scope("orders"):
    lines = ctx.document.command(line_spec)
    await lines.hold({"order_id": order_id})  # the lock every write for the order takes
    ...  # read the order's lines, decide, then write: held until commit
```

`hold` writes nothing. Each argument names the fields of one declared key. A value is validated
against the field's type and constraints, so an id passed as a string locks the same owner. A
second handler for the order waits at its own `hold` until the first commits, and the first
handler's writes take the lock again without waiting.

`hold` refuses as `configuration`:

- a spec that declares no `SerializedBy`, or an argument naming other fields;
- a datetime key field: a write keys it as the database returns it, in the session's time zone;
- a key field the domain validates with a field validator of its own, or any domain with a
  model validator: a write keys the value after them, and `hold` does not run them;
- a key field with a serializer of its own (`PlainSerializer`, `WrapSerializer`,
  `@field_serializer`), or any domain with a model serializer: a write can key the stored
  form they produce.

A call outside a transaction is refused (`document_hold_outside_transaction`), since its lock
would end with its statement. Mongo and Firestore keep no write lock and refuse the call.

One call takes its owners in sorted order. Two calls, or a hold and then a write for another
owner, can take owners in another order than a concurrent transaction does. Postgres refuses
that deadlock on one side as a retryable `concurrency`, and the in-memory store refuses the same
cycle. Pass every owner a handler needs to one `hold` call.

`hold` locks a document owner. To make an operation take turns per input instead, whatever
documents it writes, see
[Serializing an operation](../../writing-operation/transactions.md#serializing-an-operation).

The declaration itself refuses no write: where the rule can also be a hard stop, that is a
second guarantee beside this one.

`UniqueTogether` and `NonOverlapping` also say **when** they hold: `holds="always"`, the default,
means after every write; `holds="commit"` means once the transaction commits, so a transaction may
pass through a violation it resolves before committing — inserting at the top of a positioned list
while the rows it displaces still hold their positions:

```python
from forze.application.contracts.guarantees import UniqueTogether

UniqueTogether(fields=("order_id", "position"), holds="commit")
```

On Postgres that is a constraint declared `DEFERRABLE INITIALLY DEFERRED`, and startup checks the
deferral both ways: a deferred constraint behind a guarantee that holds always is refused — the
in-memory store would refuse writes the database accepts — and so is an immediate one behind a
guarantee that holds at commit. `DEFERRABLE INITIALLY IMMEDIATE` counts as immediate. A
uniqueness filtered by `where` cannot hold at commit on Postgres, because deferral belongs to
constraints and a unique constraint cannot be partial, so wiring refuses it there. `skip_null` can:
a plain unique constraint already treats a tuple holding a null as distinct, so
`ADD UNIQUE (…) DEFERRABLE INITIALLY DEFERRED` keeps it. A filtered `NonOverlapping` can too,
since an `EXCLUDE` constraint takes both. Mongo checks every write and
refuses `"commit"`. The in-memory store checks a `"commit"` guarantee when the transaction
commits — and at the write itself outside a transaction, where, as in Postgres autocommit, there
is no later moment.

Mongo refuses `NonOverlapping`, and will keep refusing it: non-overlap is a comparison *between*
two rows rather than a property of one row's fields, so unlike filtered uniqueness there is no
partial index that expresses it.

The period's start field must be non-null. A store reads a null lower bound as "in force since
always" and refuses everything overlapping it, and the in-memory store has no way to say the
same — so the declaration is refused rather than enforced two different ways. A null in the
*key* conflicts with nothing, including another null, because the comparison is equality.

On Mongo the null exemption is a `partialFilterExpression` naming each field's stored BSON type,
not a `sparse` index: sparse skips a document only when *every* indexed field is missing and still
indexes an explicit null, so the tuples the exemption was meant to let through would collide under
it. Startup refuses a sparse index offered for a `skip_null` guarantee.

A `Reference` is declared on the referencing spec, as a foreign key is declared on the
referencing table. It says that `field` holds the id of a row of the document spec `spec`, and
what a hard delete of that row does to the rows naming it. The field must be a `UUID` (or
`UUID | None`) on the read and domain models, or it would never equal that id:

```python
from forze.application.contracts.guarantees import Reference

Reference(field="stage_id", spec="stages", on_delete="cascade")
```

A composite foreign key names its columns on both sides, paired in order. `field="stage_id"` is
the shorthand for `fields=("stage_id",)`, and a single field references `id` unless `references`
says otherwise:

```python
# FOREIGN KEY (stage_id, source_id) REFERENCES stages (id, source_id) ON DELETE CASCADE
Reference(
    fields=("stage_id", "source_id"),
    spec="stages",
    references=("id", "source_id"),
    on_delete="cascade",
)
```

A row names a parent when every field holds the paired parent value, an aware datetime compared
as its instant; a row holding a null in any of them names nothing, as under the key's
`MATCH SIMPLE`. A field paired with `id` must be a
`UUID`. A field paired with another parent field must have that field's type, `None` aside.
Referenced fields without `id` must be unique on the parent, through an unfiltered
`UniqueTogether` over exactly them that holds always, declared or implied by its domain model;
on a tenant-scoped parent that uniqueness is per tenant, as the key beside `tenant_id` is. A
spec catalogue checks both once it holds both specs.

`"cascade"` deletes them too, and their own referencing rows in turn. `"restrict"` refuses the
delete with `conflict` while any remain. `"set_null"` clears every field of the reference, or
only those `set_null_fields` names, as `SET NULL (stage_id)` clears the stage and keeps the
source; each cleared field must accept `None`. The cleared row keeps the child's guarantees, so
one it would break refuses the delete with the write's `conflict`. Clearing a field that another
`Reference` keys on refuses it too while a row names the old value, as Postgres refuses that
update under the default `ON UPDATE NO ACTION`. No other `ON UPDATE` action is modelled: a key
declaring `ON UPDATE CASCADE` or `SET NULL` passes the change on where the in-memory store
refuses, and startup does not check it. Only `kill` and `kill_many` apply a reference: a soft
delete is an update and leaves the rows alone. On Postgres the foreign key does the work. The
in-memory store does the same inside the transaction's view, so a rollback restores the parent
and its children together.
It plans the whole cascade first, so a `restrict` anywhere in it refuses the delete with nothing
removed. It plans from the transaction's view, so under snapshot isolation a child another
transaction committed meanwhile survives, where Postgres's referential check would still find
it. A `restrict` refuses even from a row the same cascade deletes, where Postgres refuses or
allows that depending on which key was created first. Below an unscoped parent it searches
every tenant's partition of a tenant-scoped spec, as a foreign key reaches every tenant's rows.
Below a tenant-scoped parent it searches only that tenant's partition; a single-column Postgres
key would also reach another tenant's rows naming the parent, which happens only when rows
reference across tenants. It finds a spec's rows once that spec's adapter has been built
against the state, which a seed, a write or a resolved port does. Neither store evicts the rows a
cascade removes from a document cache. A spec catalogue refuses a reference to a spec it does not
hold. Mongo and Firestore cannot apply one and refuse it at wiring.

Three things happen to a declared guarantee, and a failure at any of them is loud:

1. **Reconciliation**, when the port is built. A backend that cannot keep the guarantee raises
   `precondition` with code `storage_guarantee_unsupported`, naming which part is missing —
   "uniqueness over a field tuple" reads differently from "uniqueness restricted to a subset of
   rows", and they need different fixes. `check_wiring` reports it like any other resolution
   failure, so CI sees it before production does.
For a non-overlap guarantee startup checks more than the columns: the constraint's key must be
*exactly* the declared key (an extra column weakens it — two rows would have to match on that
one too before they conflict), each key element must be compared with `=`, the range must be
built over the period's two fields in that order, and it must carry the declared bounds and the
`&&` operator. A constraint with the right columns and `=` on the range refuses two *identical*
periods and takes every other overlapping pair.

2. **Validation**, at startup. The live catalog is checked for the index, and the refusal names
   the DDL. An index counts when it covers the guarantee's fields — as a set, since uniqueness
   over a tuple does not depend on the order an index lists it in — and when it is live and
   valid. How closely its *restriction* is compared differs by backend, and the difference is in
   the data rather than the effort: Mongo stores a `partialFilterExpression` as a document, so
   the two filters are reduced to their equality constraints and must select the same documents,
   and anything that does not reduce that way is refused rather than guessed at. Postgres hands
   back a deparsed SQL predicate, so only the columns it restricts on are checked — an index on
   the right column and the wrong value passes there. Neither is a proof that the mechanism
   matches; on Postgres it is a floor under a forgotten migration. For a `Reference`, Postgres
   wants a foreign key on the referenced spec's relation pairing exactly the declared fields with
   the referenced ones (`id` for a single field), in any column order, alone or beside
   `tenant_id`, with the declared `ON DELETE`. A referenced spec bound in another Postgres
   module is refused, since a foreign key lives in one database. `"restrict"` takes
   `ON DELETE RESTRICT` or a plain `NO ACTION` key, unless it is deferred to commit. `"set_null"`
   needs the columns it clears nullable, and a key clearing exactly those: a plain
   `ON DELETE SET NULL` only when they are all the key's columns, and otherwise — always beside
   `tenant_id`, which a delete never clears — `ON DELETE SET NULL (…)` naming them
   (Postgres 15). Postgres also warns about each unique index, `EXCLUDE` constraint and
   `ON DELETE CASCADE` or `SET NULL` foreign key on the write table that no guarantee declares,
   other than the primary key and an index over all its columns, and suggests the declaration —
   `fields=…, references=…` for a composite foreign key, `set_null_fields=…` for a key clearing
   some of its columns. A key whose `SET NULL` clears `tenant_id` gets the migration to a key
   that leaves it, then the declaration, since no declaration keeps a row moved out of its tenant.
   A `conflict_target` over other columns is reported too, since it refuses duplicates. Without it
   the in-memory store accepts writes the database refuses. An index counts as declared only
   when it is the one that keeps a guarantee, so a partial index beside an unfiltered
   declaration is still reported. A spec the framework owns, such as a `forze_identity` one,
   is not reported at all, even for an index you added yourself, because the application
   cannot declare anything on it.
3. **Enforcement**, at write time, by the store. A violating write raises `conflict`, from every
   backend and from the in-memory store alike — including across two concurrent transactions,
   which the in-memory store rechecks at commit because a unique index is not snapshot-scoped.

!!! note "Postgres counts nulls as distinct; a guarantee does not"

    With `skip_null` off, two rows sharing a tuple that holds a null conflict. An ordinary
    Postgres unique index permits them, so a guarantee over a nullable column asks for
    `UNIQUE NULLS NOT DISTINCT` and startup refuses the plain index by name. Over `NOT NULL`
    columns the two readings cannot differ and an ordinary index is exactly the mechanism. The
    same holds for a column the `where` rules out nulls in: a top-level `$null: false`, an
    equality or ordered comparison, or an `$in` whose list holds no null.

    With `skip_null` on, the ordinary index *is* the mechanism, and no predicate is needed. A
    predicate dropping the rows that hold a null also counts, if it is only `IS NOT NULL`
    conditions on fields of the tuple, joined by `AND`, covering each nullable field. Any other
    predicate, such as `code = 'x'` or `code IS NULL`, is refused. `NULLS NOT DISTINCT` is
    refused there too, unless such a predicate drops the null rows first, because it rejects
    the rows the exemption lets through.

!!! warning "A guarantee with no `where` covers soft-deleted rows too"

    A soft-deleted row is still a row: its tuple stays reserved and no replacement can be
    created. A spec with soft deletion usually filters the deleted rows out — which also means
    an un-delete can conflict, and is refused.

!!! note "A tenant-aware route keeps a guarantee per tenant"

    The in-memory store keeps one partition per tenant, so on a `tenant_aware` route two tenants
    may share a tuple. Startup therefore accepts an index over `tenant_id` plus the fields, in
    any order, such as `UNIQUE (tenant_id, number_id)`, and prints that form when the index is
    missing. An index over the fields alone also counts, but it is stricter: it refuses a
    duplicate across tenants that the in-memory store accepts. The undeclared-index warning
    leaves `tenant_id` out of its suggestion, since the spec cannot name it.

#### Guarantees the domain model implies

Some fields carry their own guarantee. `NumberIdMixin` gives a `number_id` that a counter hands
out once, so a spec whose write domain model uses it declares
`UniqueTogether(fields=("number_id",))` by itself. You add no line to the spec. The in-memory
store refuses a duplicate number, and startup expects the unique index on `number_id` (per
tenant on a tenant-aware route).

The rules:

- A uniqueness you declare over the same fields replaces the implied one, whatever its
  `where`, `skip_null` or `holds`. The spec never carries two, so there is never a second index
  to migrate.
- Only the write domain model counts. A read-only spec implies nothing, because nothing writes
  through it and its relation may be a view. Likewise, Postgres startup checks only declared
  guarantees on a route bound in `ro_documents`: the implied ones bind the writer's table.
- `spec.guarantees` lists both kinds, and every store reads it. `spec.declared_guarantees` holds
  only what you passed as `guarantees=`.
- The counter numbers per tenant only when its own route is `tenant_aware`. Give the counter
  route and the document route the same tenancy. A per-tenant counter feeding a relation that
  tenants share hands two tenants the same number, and the guarantee refuses the second.
- An implied guarantee needs a backend that keeps it, as a declared one does. The wiring
  refusal marks it `(implied by the domain model)` and names the opt-out. Firestore keeps no
  uniqueness, so wiring refuses a numbered spec there. To opt out, override the attribute on
  your domain model:
  `implied_guarantees: ClassVar[StorageGuarantees] = ()`. Nothing then refuses a duplicate
  number.
- A numbered domain whose read model does not store `number_id` is refused when the spec is
  built, because a guarantee must name a stored field.

A mixin of your own can do the same: set `implied_guarantees` as a `ClassVar` holding a tuple
of guarantees (`GuaranteeImplying` is the protocol). Anything else, such as a bare guarantee
outside a tuple, is refused as `implied_guarantees_invalid` when the spec is built.

#### Guarantee or invariant?

Both can express "one current row per fact", and the question comes up immediately.

| | `SystemInvariant` | Storage guarantee |
|---|---|---|
| Declares | a law over a read-set, reduced to one number | a property the store refuses to violate |
| Enforced by | the framework, in or after the writing transaction | the store, at write time |
| Detects | a violation that already happened, or prevents it under isolation | prevents it, always |
| Expresses | anything `SumOf` / `CountAll` can score | uniqueness, non-overlap |

Declaring both is legitimate where the guarantee is the prevention and the invariant is the
proof — a simulation asserts the invariant over a workload the guarantee is quietly keeping.

## Query port

### Fetch one

| Method | Returns | On miss |
|--------|---------|---------|
| `get(pk, *, owned_by=None, for_update=False, skip_cache=False)` | `R` | raises `not_found` |
| `get_many(pks, *, owned_by=None, skip_cache=False)` | `Sequence[R]` | raises `not_found` (lists missing) |
| `find(filters, *, for_update=False)` | `R \| None` | returns `None` |
| `project(filters, fields, *, for_update=False)` | `JsonDict \| None` | returns `None` |
| `select(filters, return_type, *, for_update=False)` | `T \| None` | returns `None` |

`for_update` takes a `RowLockMode` (`True` / `"nowait"` / `"skip_locked"`) to lock
the row inside a transaction. A locking read always goes to the database, past any read cache.

`owned_by` is the preferred way to say a row belongs to someone. With it, a row whose owner
field holds another value is **not found** — the same error as a missing row, from the same
read — so a handler cannot serve a foreign row by forgetting a check:

```python
from forze.application.contracts.document import OwnedBy

note = await ctx.doc.query(NOTES).get(pk, owned_by=OwnedBy(field="owner_id", value=principal_id))
```

The owner goes into the database predicate, so a locking read never locks a foreign row; a row
served from the read cache is checked before it is returned. In `get_many`, a foreign id fails
the call exactly as a missing id does, with one summary that names no id — a list of only the
missing ids would tell the caller the others exist. `field` must be a stored, filterable UUID
field of the read model: one the model lacks, one sealed with randomized encryption, or a lenient
read field is refused as a `configuration` error before the read, rather than answered with a
not-found on one path and something else on the other.
Pair it with a [non-disclosing posture](../errors.md#non-disclosing-denials) so the not-found
also renders like a denial.

### Fetch many

Each comes in `find` / `project` / `select` flavors and `_many` / `_page` /
`_cursor` containers. All take `filters`, `sorts`, and pagination:

| Method | Result |
|--------|--------|
| `find_many(filters=None, pagination=None, sorts=None)` | `CountlessPage[R]` (`.hits`) |
| `find_page(...)` | `Page[R]` (adds `.count`) |
| `find_cursor(filters=None, cursor=None, sorts=None)` | `CursorPage[R]` (keyset) |
| `project_many` / `project_page` / `project_cursor` `(fields, …)` | pages of `JsonDict` |
| `select_many` / `select_page` / `select_cursor` `(return_type, …)` | pages of `T` |

### Stream & aggregate

| Method | Result |
|--------|--------|
| `find_stream(filters=None, *, sorts=None, chunk_size=500)` | async generator of `Sequence[R]` |
| `project_stream` / `select_stream` | async generators of `JsonDict` / `T` batches |
| `aggregate_many(aggregates, filters=None, …)` | `CountlessPage[JsonDict]` |
| `aggregate_page(aggregates, …)` | `Page[JsonDict]` (group count) |
| `select_many_aggregated` / `select_page_aggregated` `(return_type, aggregates, …)` | typed aggregate rows |
| `count(filters=None)` | `int` |

`filters`, `sorts`, and `aggregates` use the [query DSL](../query-syntax.md);
`pagination` is `{"limit": …, "offset": …}`.

Without a `limit`, a read returns every matching row, fetched in batches. Rows come
back in the effective sort (`sorts`, else `default_sort`, else `id`), with `id`
breaking ties when the read model has one, and each batch starts after the last row
of the one before. Some reads are paged by offset instead, still with the `id`
tie-breaker: a projection or returned model that leaves out a sort key, and on
MongoDB any sort other than `id` alone. Firestore refuses offsets, so there such a
read is one query with no limit, streamed whole. A read model without `id` has no
tie-breaker, so rows tied on its whole sort can still repeat or go missing across
offset batches.

The `id` tie-breaker shapes the index such a read wants. On MongoDB, a `{key: 1}`
index cannot serve the `{key: 1, _id: 1}` sort, which then runs in memory; add a
compound `{key: 1, _id: 1}` index for a large collection read this way. Firestore
breaks ties on the document name, which forze sets to the id and every index already
ends in, so a single-field index still serves a read sorted by one key; a sort led by
`id` keeps ordering by the stored `id` field.

## Command port

Every mutating method takes `return_new: bool = True` — return the resulting read
model(s), or `None` when you don't need them back.

### Create

| Method | Signature | Notes |
|--------|-----------|-------|
| `create` | `create(payload, *, id=None, return_new=True)` | server-generates the PK unless `id` is given |
| `create_many` | `create_many(payloads, *, return_new=True)` | batch insert |
| `ensure` | `ensure(id, payload, *, return_new=True)` | insert-when-missing; **never mutates** an existing row (idempotent by PK) |
| `ensure_many` | `ensure_many(items, *, return_new=True)` | bulk insert-when-missing (`KeyedCreate`) |
| `upsert` | `upsert(id, create, update, *, return_new=True)` | insert `create`, else apply `update` (domain apply + OCC) |
| `upsert_many` | `upsert_many(items, *, return_new=True, set_based=False)` | bulk insert-or-update (`UpsertItem`); `set_based=True` is the bulk-load path below |

A bulk load that needs no rows back can pass `return_new=False, set_based=True`. Postgres
then inserts the missing rows and patches the stored ones with set-based statements, reading
nothing back: a stored row takes the update as its DTO encodes it, and only a row the update
changes gets a new `rev` and `last_update_at`. The stored rows are locked in id order first,
so concurrent loads of overlapping batches queue rather than interleave.

Like `update_matching`, it skips the domain model's update, so a spec whose updates need it
refuses `set_based` with a `configuration` error (`set_based_upsert_unsupported`): one with
revision history, materialized fields, a `SerializedBy` guarantee or randomized field
encryption; a domain with update validators, invariants, domain events, or pydantic validators,
constraints or serializers of its own (model-level, or on any field; a JSON-only serializer does not count); or an update field the domain lacks, freezes, merges
(a model or mapping) or derives from the other fields. A table
keyed by more than the id (and tenant), or a column an update can write that Postgres cannot
compare, takes the usual path instead. Mongo, Firestore and the mock apply the same refusals
and then write through their usual path.

Each item says what its update writes, on every backend and with or without `set_based`:

- `UpsertItem.mirror(id, payload)` updates with the create payload itself, for a mirrored
  source whose newer version rewrites every field the create carries, not only the update
  command's. A set-based upsert is judged on the fields the items' update types carry, so a
  mirror whose create holds a mapping, a model or a frozen field is refused.
- `skip_none=True`, on `mirror` or on `UpsertItem(...)`, leaves out each top-level field the
  update sets to `None`, so a stored value is kept instead of cleared. A `None` inside a mapping
  or model is not looked into, and the create is still inserted as it is.

An update writes the fields its DTO sets: a field the payload leaves unset keeps its stored
value, a mirror's too. On the domain path, a mirror whose create sets a frozen field (such as
`ImportTimestamps.created_at`) is refused for a stored row with "Field … is frozen".

### Update

| Method | Signature | Notes |
|--------|-----------|-------|
| `update` | `update(pk, rev, dto, *, return_new=True, return_diff=False, merge=True)` | optimistic — a stale `rev` is merged through history, or refused (`revision_mismatch`) without history or with `merge=False` ([details](../../writing-operation/concurrency-conflicts.md#refusing-any-change-since-you-read)); `return_diff` adds the change `JsonDict`, a merge patch holding the values the caller set on every backend (a `StrEnum` stays a member, a datetime is as given), not the store's column coercion |
| `update_many` | `update_many(updates, *, return_new=True, return_diff=False, merge=True)` | per-row update (`KeyedUpdate`: id, rev, dto) with OCC |
| `update_matching` | `update_matching(filters, dto, *, return_new=True)` | fast bulk patch by filter — **no per-row OCC, no domain side effects**; refuses a field set to a mapping or model, which only an update merges; not on Firestore; `return_new=False` → rows-updated count |
| `update_matching_strict` | `update_matching_strict(filters, dto, *, return_new=True, chunk_size=None)` | like `update_many` (per-row OCC + domain apply) over a filter |
| `touch` / `touch_many` | `touch(pk, *, rev=None, return_new=True)` | bump `rev` and `last_update_at` only; with `rev` (`revs=` per id on `touch_many`), only a document still at it |

### Delete

`kill(pk)` and `kill_many(pks)` **hard-delete** — there is no soft-delete or
`restore` on the port (model soft-delete is a domain concern, applied via
`update`). On a spec declaring `hard_delete=False` both refuse with a
`configuration` error (`hard_delete_forbidden`), whoever calls them. Both apply every
[`Reference`](#storage-guarantees) naming the spec. A row a foreign key still references is
refused with `conflict`, by `ON DELETE RESTRICT` and a plain key alike.

### Command hooks

A plane that owns a document can wrap its command port wherever it is resolved: a provider
registered under `DocumentCommandHookDepKey`, routed by the document's name, receives
`(ctx, port)` and returns the port handed out, before any other wrap and before it is cached.
Every write through `ctx.document.command(spec)` passes it, on every backend; a write around the
port (SQL, another spec over the same table) does not. The identity plane registers one on the
policy principal to raise a principal's epoch on deactivation (see
[Authn](../../integrations/authn.md#skip-the-eligibility-read)).

## Implemented by

| Backend | Tenancy ceiling | Integration |
|---------|-----------------|-------------|
| Postgres | `dedicated` | [Postgres](../../integrations/postgres.md) |
| Mongo | `dedicated` | [Mongo](../../integrations/mongo.md) |
| Firestore | `dedicated` | [Firestore](../../integrations/firestore.md) |

The in-memory mock implements the full surface, so an aggregate is testable without a
backend — see [Testing](../../testing/overview.md).
