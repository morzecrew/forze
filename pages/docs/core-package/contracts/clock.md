# Deterministic time & ids

Time is **ambient** — there is one "now" per execution scope — so Forze treats it as a
context-scoped source rather than a routed dependency. `forze.base.primitives.utcnow()` and the
no-argument `uuid7()` read the active **`TimeSource`** (the system clock by default), so a bound
source makes every time/id read deterministic in tests and replay-stable in durable workflows —
**with no call-site changes** and without leaking `ctx` into the domain.

## Binding a source

    :::python
    from datetime import datetime, UTC
    from forze.base.primitives import bind_time_source, FrozenTimeSource

    with bind_time_source(FrozenTimeSource(instant=datetime(2020, 1, 1, tzinfo=UTC))):
        order = Order()                        # order.id / created_at are deterministic
        order.record_event(OrderPlaced(...))   # occurred_at / event_id deterministic too

`FrozenTimeSource` returns a fixed `now` and deterministic, time-ordered ids — the test seam.
Domain code keeps calling `utcnow()` / `uuid7()` and stays clock-free in its own source; the seam
controls the *read*, so domain self-stamping (`DomainEvent.occurred_at`/`event_id`,
`Document.id`/`created_at`) is controlled transparently. The explicit-timestamp
`uuid7(timestamp_ms=...)` path is unchanged (already deterministic).

Application code that reads time reads it the same way — `utcnow()` / `uuid7()` — so there is one
source of "now" for domain and application alike. (There is deliberately no separate clock *port*:
a routed dependency is the wrong shape for ambient time, and a second clock would risk diverging
from the one the domain stamps with.)

## Durable replay

The durable workers (e.g. Temporal) bind a deterministic `TimeSource` for the workflow scope —
backed by the runtime's replay-safe `now()`/`uuid` (`temporalio.workflow.now()` /
`workflow.uuid4()`) — so every `utcnow()`/`uuid7()` read inside a workflow reproduces across
replays. (Inside a workflow, `uuid7()` yields a runtime-deterministic id: determinism wins over
time-ordering in replayed code.) Activities run outside the sandbox and keep the system source.
