---
title: Save a parent's child rows
icon: lucide/list-checks
summary: Reconcile a child collection against the list a client sends — delete the dropped, create the added, update only the changed
---

A client edits an order and sends its lines back whole. The stored lines have to match: a line
no longer sent is deleted, a new one is created, and a kept one is updated where it changed.
Deleting every line and recreating them is simpler, but it gives untouched lines new ids and
revisions, breaks anything that references them, and rewrites their audit fields on every save.
`ChildCollectionSync` writes only the difference, so a line nobody touched is not written.

The runnable version lives at `examples/recipes/child_collection/` and runs on the in-memory
mock store, so you need no infrastructure.

## Map the request to create commands

Each line the client sends becomes the child's create command, which names the parent and
carries the line's position:

```python
--8<-- "recipes/child_collection/app.py:request"
```

The key and the fields to compare are read off these commands, and the new children are
created from them.

## The diff, written out

This is the procedure, by hand. Read the current lines, match them to the incoming ones by
`sku`, and issue three batches:

```python
--8<-- "recipes/child_collection/app.py:by_hand"
```

The batches run in a fixed order: deletes, then creates, then updates. Deletes come first so a
new line can take a unique value a removed line held, such as its position. This version deletes
a line whatever another writer did to it since the read; the component does not (see the notes).

## The same, declared

`ChildCollectionSync` runs that procedure for one child spec. The handler touches the order
first, at the revision the client read, so a save based on an older order is refused before
any line is written:

```python
--8<-- "recipes/child_collection/app.py:sync"
```

| Field | What it says |
|---|---|
| `child_spec` | The child aggregate. |
| `parent_fk` | The child's field holding the parent's id. Every incoming command must name the parent. |
| `key_fields` | The child's business key within its parent. It may be several fields, and a null is part of the key. |
| `compare_fields` | The fields whose change makes a kept child an update. Any other field is ignored. |

Construction checks that every named field is stored on the read model and present on the
create command, and that every compare field can be written by the update command. It refuses
what it could not serve correctly, each with its own `configuration` code:

| Refused | Code |
|---|---|
| A key or compare field the spec does not store (`lenient_read_fields`, `write_omit_fields`): it reads back as its default. | `child_field_not_stored` |
| A key field whose type may not hash, such as a list, a mutable model or `Any`. | `child_key_unhashable` |
| A compare field that is a mapping, a nested model or `Any`, unless the domain model marks it [`ReplaceOnUpdate`](../core-concepts/domain-layer.md#rules-live-with-the-data): an update merges into it key by key, so a key the incoming value drops would stay stored. | `child_compare_field_merged` |
| An update command requiring a field no patch carries: a patch holds only compare fields and the soft-delete flag. | `child_update_requires_fields` |
| A spec declaring `query_params`: the children are read without parameters. | `child_spec_parametrized` |

Each incoming key and compare value is read as the read model's type for that field before it is
matched, so a float meets a stored decimal and a string a stored enum, and a naive datetime is
read as UTC. A value the read model cannot hold is refused (`validation`,
`child_value_unreadable`).

## Run it in a transaction

`reconcile` refuses to run outside a transaction
(`precondition`, `child_sync_outside_transaction`). Up to three batches run, and a failure in a
later batch must undo the earlier ones. The child's port must write on the transaction's own
connection too: one on another backend or pool is refused (`configuration`,
`core.tx.not_enlisted`), since each batch would commit alone. Bind the operation to a transaction
route that reaches the child's store:

```python
--8<-- "recipes/child_collection/app.py:registry"
```

## Use what it wrote

`reconcile` returns a `ChildCollectionChanges`:

| Attribute | Holds |
|---|---|
| `removed` | The children it deleted, as they were read before the delete. |
| `created` | The children it created, as the store returned them. |
| `updated` | `(as read, as written)` pairs for each child the store changed. |

A handler that moves stock or writes a ledger per line reads these instead of diffing again.
`updated` leaves out a patch the store found already applied, such as a value the domain
normalizes to what is stored. Every write is checked strictly against the revision read, so a
pair never carries another writer's change and `removed` never lists a child someone else
deleted.

## Notes

- **The key is required.** A collection with no business key cannot be reconciled, because a
  changed line cannot be told from one removed and another added. Two incoming commands with one
  key are refused (`validation`, `child_key_duplicate`). Two stored children with one key are
  refused too (`conflict`, `child_key_ambiguous`). A command naming another parent is refused
  (`validation`, `child_parent_mismatch`). Nothing is written in these cases.
- **Positions are yours.** No position is written for you: put it in the create commands and in
  `compare_fields`, as the recipe does. A reorder then updates every line whose position
  changed, so moving the last of four lines to the top writes all four. A unique position must
  hold at commit, not after every write:
  `UniqueTogether(fields=("order_id", "position"), holds="commit")`, which is
  `DEFERRABLE INITIALLY DEFERRED` on Postgres. See
  [storage guarantees](../reference/contracts/document.md#storage-guarantees).
- **Deletes follow the spec.** A child whose domain carries `is_deleted` is soft-deleted, at the
  revision read, and a deleted child is not part of the current collection. Its read model must
  store `is_deleted` and its update command must write it, or construction is refused
  (`child_soft_delete_unwired`). A re-added key creates a new row, so a uniqueness over the key
  must skip deleted rows: `where={"$values": {"is_deleted": False}}`. Any other child is
  hard-deleted with `kill_many`, and a spec declaring `hard_delete=False` is refused at
  construction.
- **Every write checks the revision read, strictly.** An update and a soft delete carry it with
  `merge=False`, and a hard delete is preceded by a touch at it, because `kill_many` takes no
  revision. A child another writer changed since the read is refused (`revision_mismatch`)
  rather than overwritten or deleted, even on a spec with revision history, which would
  otherwise merge the two changes. One another writer erased is `not_found`, and one it created
  with a unique key is `conflict`.
- **It takes no lock.** A child another writer adds after the read is not seen, so it survives
  the save. Touching the parent at the client's revision first, as the recipe does, refuses a
  stale save outright; on Postgres the row lock it takes also makes a second save wait. A spec
  declaring `SerializedBy` on the parent key can instead `hold` that owner before reconciling.
- **The parent is the handler's.** `reconcile` neither touches nor re-reads the parent. A handler
  that returns the parent with its children re-reads it after reconciling.
- **It reads every child.** The current children are read as a stream, so a collection larger
  than one read batch is read whole. Creates and updates return the written rows, which costs a
  re-read on stores that do not return rows from the write itself.

## See also

- [Transactions](../writing-operation/transactions.md) — what a transaction route binds.
- [Concurrency conflicts](../writing-operation/concurrency-conflicts.md) — retrying a refused
  save.
