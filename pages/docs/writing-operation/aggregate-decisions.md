---
title: Aggregate decisions
icon: lucide/git-branch
summary: Put a state transition's rule on the aggregate as a decider — load, decide, apply — instead of scattering the guard and the change across handlers
---

A state transition has a rule: *which* states it is legal from, and *what* it changes. Left in the handler, that rule scatters — the guard in one place, the write in another, sometimes duplicated and easy to forget. A **decider** puts it back on the aggregate: a pure method that validates the transition and returns the merge-patch to persist.

## A decision on the aggregate

A decider is an ordinary method on the aggregate. It reads the current state, **raises** if the transition is illegal, and returns the update DTO — no I/O, no ports:

```python
--8<-- "recipes/order_fulfillment/app.py:order-aggregate"
```

`confirm()` is the single home for the `pending → confirmed` rule. The `@event_emitter` beside it still fires `OrderConfirmed` when the status actually changes — the decider owns *when the transition is allowed*, the emitter owns *what event it produces*.

## Load, decide, apply

A handler runs the decision through `AggregateRepository` (`forze_kits.aggregates`): **load** the aggregate (so its behavior can run), **decide** by calling the method, **apply** the patch under the aggregate's revision:

```python
--8<-- "recipes/order_fulfillment/app.py:confirm-step"
```

`apply()` persists the merge-patch as `update(id, rev, patch)`, so a concurrent write that moved the revision is rejected — the same optimistic-concurrency guard as any other write ([concurrency conflicts](concurrency-conflicts.md)). The re-applied patch fires the aggregate's emitters, and the resulting domain events dispatch in the same transaction; the repository never dispatches them itself.

## Why move it down

The hand-written alternative — `command.update(id, rev, OrderUpdate(status="confirmed"))` straight in the handler — works, but the *rule* lives nowhere: nothing guards that the order was pending, and the transition is re-encoded at every site that needs it (the step that writes it, the emitter that reacts to it). The decider:

- **single-sources the rule** — one `confirm()`, called from every site, instead of a literal patch repeated per call;
- **guards the transition** — an illegal `confirm()` raises `exc.domain` instead of silently writing a bad state;
- **stays visible to the correctness brand** — the guard runs on the operation path, so a [simulation](../dst/overview.md) that drives an illegal transition observes the rejection (unlike a pure [`@invariant`](../core-concepts/domain-layer.md#rules-live-with-the-data), which the mock enforces before the write even lands).

Reach for a decider when a write is a *transition* with a rule; a plain `command.update` is still right for a field edit that carries no domain rule. When the rule spans **more than one** record, its cross-record counterpart is a [system invariant](system-invariants.md).
