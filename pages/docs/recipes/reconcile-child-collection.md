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
new line can take a unique value a removed line held, such as its position.

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
create command, and that every compare field can be written by the update command.

## Run it in a transaction

`reconcile` refuses to run outside a transaction
(`precondition`, `child_sync_outside_transaction`). Up to three batches run, and a failure in a
later batch must undo the earlier ones. Bind the operation to a transaction route:

```python
--8<-- "recipes/child_collection/app.py:registry"
```

## Use what it wrote

`reconcile` returns a `ChildCollectionChanges`:

| Attribute | Holds |
|---|---|
| `removed` | The children it deleted, as they were read before the delete. |
| `created` | The children it created, as the store returned them, in incoming order. |
| `updated` | `(as read, as written)` pairs for each child the store changed. |

A handler that moves stock or writes a ledger per line reads these instead of diffing again.
`updated` leaves out a patch the store found already applied. A store keeping revision history
can accept a stale revision whose patch matches the row as it stands, so a concurrent save that
made the same change is not counted twice.

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
- **It takes no lock.** A concurrent save of the same parent loses with the port's own error:
  `revision_mismatch` from an update, `not_found` from a delete, or `conflict` from a create
  against a unique key. Touching the parent at the client's revision first, as the recipe does,
  refuses a stale save outright; on Postgres the row lock it takes also makes a second save wait.
  A spec declaring `SerializedBy` on the parent key can instead `hold` that owner before
  reconciling.
- **The parent is the handler's.** `reconcile` neither touches nor re-reads the parent. A handler
  that returns the parent with its children re-reads it after reconciling.
- **It reads back what it writes.** Creates and updates return the written rows, which costs a
  re-read on stores that do not return rows from the write itself.

## See also

- [Transactions](../writing-operation/transactions.md) — what a transaction route binds.
- [Concurrency conflicts](../writing-operation/concurrency-conflicts.md) — retrying a refused
  save.
