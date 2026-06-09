---
title: Concurrency & conflicts
icon: lucide/git-compare-arrows
summary: Optimistic concurrency on rev, and how concurrent writes resolve or conflict
---

Two requests that read the same document and write it back shouldn't silently
clobber each other. Forze uses **optimistic concurrency** keyed on a revision
number, with a history-based merge that lets non-overlapping edits through and
rejects genuine collisions.

## The revision

Every [`Document`](../core-concepts/domain-layer.md) carries a `rev` — an integer
that starts at `1`, is frozen on the model, and is bumped by the store on each
write. You read at a revision, then write back the one you read:

```python
order = await ctx.document.query(order_spec).get(order_id)
await ctx.document.command(order_spec).update(
    order.id, order.rev, OrderUpdate(status="paid"),
)
```

The `rev` you pass is the revision you *expect* to be current.

## What happens on a concurrent write

The store compares your expected `rev` to the stored one:

- **Match** — apply the change and bump `rev`.
- **Differ** — load the history snapshot at your revision and run a **three-way
  merge**. Edits that touch *disjoint* fields merge through (last-writer-wins per
  field); edits that touch the *same* fields raise `exc.conflict`
  (`historical_consistency_violation`).
- **A future or unknown revision** (or a missing history snapshot) raises
  `exc.precondition` (`revision_mismatch`) — re-read the document and retry.

So a stale write doesn't blindly overwrite: it either merges cleanly or surfaces
a [conflict](errors.md) for the caller to resolve.

!!! note "History powers the merge"

    The three-way merge needs document history enabled. Without it, *any*
    revision mismatch is a `precondition` error rather than a merge attempt.

## Related write helpers

- `Document.validate_historical_consistency(old, patch)` is the check behind the
  merge — `True` when the concurrent change and yours touch disjoint fields.
- `command.touch(pk)` bumps `last_update_at` and `rev` without other changes.
</content>
