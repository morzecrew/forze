---
title: System invariants
icon: lucide/scale
summary: Laws that span more than one record — a conserved balance, a cardinality cap — declared once, then enforced (detective or preventive), dry-run-checked, and proved under simulation
---

An [`@invariant`](../core-concepts/domain-layer.md#rules-live-with-the-data) keeps a single entity correct: it sees one record and decides whether *that record* is valid. But some rules span records, and no single entity owns them — a ledger's entries must sum to zero, an order may have **at most one** captured payment, a tenant's seats in use must not exceed its plan. A check on one row cannot see the others. A `SystemInvariant` declares such a law, and Forze enforces it at the right isolation and proves it under simulation.

## A law over a read-set

A system invariant is a predicate over an **aggregate of a scoped set of records**. You declare three things: the records it ranges over (a `ReadSet`), how to collapse them to one number (`SumOf` or `CountAll`), and what that number must satisfy (`holds`):

```python
from forze.application.contracts.invariants import SystemInvariant, ReadSet, CountAll

one_capture_per_order = SystemInvariant(
    name="single-capture-per-order",
    read_set=ReadSet(
        spec=PAYMENT,
        scope_keys=("order_id",),
        where={"$values": {"status": "captured"}},
    ),
    aggregate=CountAll(),
    holds=lambda n: n <= 1,
)
```

The law holds **per distinct scope value** — once *per order* here, because `scope_keys=("order_id",)`. Empty `scope_keys` declares a *global* law (one check over the whole `where`-filtered collection); reuse `tenant_aware` in `where` for a per-tenant scope. `holds` is a pure function of the aggregate, and must be deterministic and side-effect-free, because the same predicate runs in production *and* inside the simulation. It receives a `CountAll`'s cardinality as an `int`, and a `SumOf`'s total as an exact `Decimal` when the read model declares the field `Decimal`, else as a `float`. The type follows the declaration, not the backend, so a law sees the same value on the mock, Postgres and Mongo: lines of `0.1` and `0.2` total exactly `Decimal("0.3")`, never `0.30000000000000004`. A sum the read model does not declare (a path into a `dict`) is passed as a `float`, with a warning when the law is declared.

!!! warning "A `Decimal` total does not mix with float arithmetic"

    Comparing a `Decimal` with an `int` or a `float` works, but arithmetic does not: `total * 1.1` raises `TypeError`. Write the factor as a `Decimal` (`total * Decimal("1.1")`). Violation messages print the value as it is, so a count reads `3`, not `3.0`.

## Compare two read-sets

Some laws bound one aggregate by another: an order's lines never exceed the order's quantity, refunds never exceed sales. Declare the second side with `against`, a `Measure` scoped by the same values, and `holds` takes both aggregates:

```python
from forze.application.contracts.invariants import Measure

lines_within_order = SystemInvariant(
    name="lines-within-order",
    read_set=ReadSet(spec=ORDER_LINE, scope_keys=("order_id",)),
    aggregate=SumOf("qty"),
    against=Measure(
        read_set=ReadSet(spec=ORDER, scope_keys=("id",)),
        aggregate=SumOf("qty"),  # the order's own field: a sum over its one row
    ),
    holds=lambda lines, order: lines <= order,
)
```

The two sides' `scope_keys` pair **by position**: here each order's lines (`order_id`) meet the order whose `id` matches. The oracles match a `UUID` or an `Enum` by its string form, so a `str` reference to a `UUID` id works; a pairing that can never match, such as an `int` against a `UUID`, is refused when the law is declared, as is a `holds` that does not take two arguments. Runtime enforcement takes the law's own scope params (`{"order_id": ...}`) and binds the other side from them.

Every scope with rows on **either** side is checked, and the side with no rows there counts as `0`. A scope with rows on neither side does not exist, so no evaluator calls `holds` for it. That is what makes a *minimum* checkable. A one-sided law only ever sees scopes that have rows, so `CountAll` with `n >= 1` cannot notice an order whose last line was removed. Put the orders on the other side and every order becomes a scope:

```python
order_has_a_line = SystemInvariant(
    name="order-has-a-line",
    read_set=ReadSet(spec=ORDER_LINE, scope_keys=("order_id",)),
    aggregate=CountAll(),
    against=Measure(read_set=ReadSet(spec=ORDER, scope_keys=("id",)), aggregate=CountAll()),
    holds=lambda lines, orders: orders == 0 or lines >= 1,
)
```

Keep the side you enforce from as `read_set`: an [aggregate kit](../recipes/governed-aggregate-kit.md) reads the law's own `scope_keys` off the row it wrote, so a kit over order lines needs `order_id` there, not `id`.

## Enforce it: detective or preventive

Two enforcement modes, from `forze_kits.invariants`, trade timing against cost:

- **`enforce(law, ctx, params)`** — *detective*. It schedules the check for **after** the writing transaction commits (via `run_or_defer`), so it observes committed state, and raises `exc.domain` if the law broke. The offending write is already durable — this *reports* the breach, it does not undo it. Cheap, and right when you can compensate after the fact.
- **`enforce_preventive(law, ctx, params)`** — *preventive*. It evaluates the law **inside** the writing transaction, before commit, so a violation rolls the write back — the bad state never lands. Call it after the writes, inside the scope:

```python
async with ctx.tx_ctx.scope(route, isolation=law.required_isolation):
    ...  # your writes
    await enforce_preventive(law, ctx, params)
```

Prevention is only sound at or above the law's `required_isolation` (default `SERIALIZABLE`): a weaker level lets a write-skew interleaving slip two concurrent writes past the check, each blind to the other. So `enforce_preventive` **fails closed** — it raises `exc.configuration` if the active transaction is weaker than required (or absent) rather than hand back enforcement a race would defeat. Opening the scope at the required level already pays the backend half of the gate, since `scope(isolation=…)` fail-closes against the manager's [`TxCapabilities`](transactions.md#isolation).

## Ask before you write: propose

For an agent or a UI that wants to know *"would this write be accepted?"* before committing to it, `propose` dry-runs the write:

```python
from forze_kits.invariants import propose

verdict = await propose(ctx, do_the_write, [(law, params)], route=route)
if not verdict.holds:
    handle(verdict.failed)   # the laws that would break
```

`propose` opens its own root transaction, applies the write through the governed ports (so tenancy, encryption, and domain guards all run), evaluates each law against the result, then **rolls everything back** — nothing persists. The `ProposalVerdict` is `holds=True` only if the write applied cleanly and every law held; `failed` names the laws that wouldn't, and `error` carries the rejection if the write itself was refused.

!!! warning "`propose` is a filter, not a proof"

    `holds=True` means "the write applied cleanly and the laws held *against the state the dry-run saw*". Under concurrency the answer can change between the dry-run and the real write (TOCTOU), and it is only as sound as the backend's rollback — so use it to filter and explain a proposed write, never to certify it safe. It also fails closed (`exc.precondition`) if called inside a transaction: a nested scope is only a savepoint, whose rollback isn't guaranteed across backends, so the dry-run's writes could ride the enclosing commit. The *proof* is the oracle below.

## Prove it under simulation

Because a law is declared as data — scope *fields*, not an opaque filter-builder — DST can compile it into an oracle that checks **every scope the run produced**, not a hand-listed few:

```python
from forze_dst import Simulation
from forze_dst.oracle import compile_oracle

oracle = compile_oracle(one_capture_per_order)
simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(),
    observe=oracle.observe,
    invariants=[*oracle.invariants],
)
```

By default the oracle checks the law over **final** state, with one grouped aggregate per side. A one-sided law checks only the scopes that have rows; a [paired law](#compare-two-read-sets) checks every scope with rows on either side. `compile_oracle(law, per_commit=True)` upgrades to a per-commit trace fold: it checks the law after *every* committed transaction, catching a violation that a later transaction heals — the transient a final-state check misses. Per-commit mode reads the value trace, so it needs `SimulationConfig(capture_values=True)`.

This is the guarantee `propose` can't give. Where `propose` filters one write against the state it saw, the oracle drives the law through thousands of [hostile interleavings](../dst/overview.md) and proves it holds across all of them — and because the same `holds` predicate runs in production enforcement, in the dry-run, and in the oracle, what you simulate is exactly what you ship.
