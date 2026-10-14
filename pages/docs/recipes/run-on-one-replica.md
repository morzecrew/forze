---
title: A publisher on one replica
icon: lucide/crown
summary: Run a publisher on one replica at a time, with fenced writes a stale leader cannot make
---

Some work must run on exactly one replica at a time: a publisher that numbers what it
sends, a snapshot builder, a coordinator. Run it on every replica and the output
duplicates; run it on one fixed replica and it stops when that replica does. This recipe
puts a publisher behind a leadership lease and fences its writes, so a leader whose lease
has passed on cannot overwrite its successor. The model — terms, deadlines, why fencing —
is in [Run one replica's worth of work](../running-in-prod/shutdown-and-fleets.md#run-one-replicas-worth-of-work).

The runnable version lives at `examples/recipes/leadership/` and runs on the in-memory
mock, two replicas in one process.

## The lease

The lock spec requires fencing tokens, so a lock backend without them fails at startup
instead of leaving the writes unfenced:

```python
--8<-- "recipes/leadership/app.py:spec"
```

`renew_every + yield_grace + margin` must be shorter than the TTL, and the spec refuses
anything else when it is built: the body's grace has to end before the lease could lapse.

## The fenced store

The store keeps the highest token that wrote. A term's first write **claims** its token —
the stored fence must be strictly lower — and its later writes accept an equal one. The
stand-in below is what the two SQL statements in its docstring do:

```python
--8<-- "recipes/leadership/app.py:store"
```

The claim is what settles a repeated token. After the lock backend loses its data, its
counter can issue a token a stale holder already drew; both try the claim, the store
serializes them, and exactly one finds the fence lower.

## The step

The claim runs as the `claim` hook, before the body. The body publishes one unit at a
time, checks `term.live()` between units, and stops at the first write the fence refuses:

```python
--8<-- "recipes/leadership/app.py:step"
```

Register the step on every replica, with each replica's own `owner`:

```python
from forze.application.execution import LifecyclePlan

lifecycle = LifecyclePlan.from_steps(publisher_step(store, owner=instance_id))
```

## Run it

```bash
uv run python -m examples.recipes.leadership.app
```

One replica publishes with token 1. The script stops it the way a deploy does: its term
ends as `stopped`, the body finishes its unit, the lease is released, and the other
replica takes over within half a second with token 2.

## Notes

- **Keep the body's units short.** It is told its term is ending `yield_grace` before the
  deadline and cancelled after that. A unit that cannot finish in the grace is cut off
  mid-way.
- **Fence every write the body makes as leader**, not only the first. A leader that is
  paused past its lease keeps its `term`, and only the store can refuse it.
- **Wire the store's check yourself.** The framework issues the token; refusing a lower
  one is the store's job, and without it the lease is best-effort exclusion.
