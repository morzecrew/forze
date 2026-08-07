# RFC 0035 — Ack-stream-group consumer kit

- **Status:** 📝 Draft — **parked (demand-gated).** Design recorded, deliberately unscheduled. Two named consumers exist on paper (the socket.io gateway's private loop, and the operation-progress kit's out-of-process projector) but **neither is asking today** — the gateway's loop works and progress projects inline. Pick this up when the trigger in §6 fires; reject it if the second consumer never materialises and the gateway loop stays the only one.
- **Scope:** One reusable consumer runner for the **ack** sub-model of the stream family (`AckStreamGroupQueryPort` — Redis-class: per-message ack, explicit `claim` recovery), living beside the two runners `forze_kits.integrations.consumer` already ships. Nothing else: no new port, no new delivery model, no change to the ack contract, no gateway rewrite in the same phase.
- **Related:** The two existing runners this would sit beside — `QueueConsumer` (queue sub-model, [`consumer/runner.py`](../src/forze_kits/integrations/consumer/runner.py)) and `CommitStreamGroupConsumer` (offset-log sub-model, [`consumer/commit_stream_runner.py`](../src/forze_kits/integrations/consumer/commit_stream_runner.py)). The port pair being consumed — `AckStreamGroupQueryPort` / `AckStreamGroupAdminPort` ([`contracts/stream/ports.py`](../src/forze/application/contracts/stream/ports.py)). The only implementation that exists today — `_consume_group_stream` ([`forze_socketio/gateway.py`](../src/forze_socketio/gateway.py)), private, and coupled to `SignalHandler` / `RealtimeGatewayStats`. Dedup — `process_with_inbox` ([`forze_kits/integrations/inbox`](../src/forze_kits/integrations/inbox)). Loop machinery already shared — `BackgroundLoopControl` / `run_supervised` ([`execution/background/`](../src/forze/application/execution/background/)). The candidate second consumer — the operation-progress kit's out-of-process projector.
- **Origin:** executing the operation-progress kit (2026-08-04). Asked "how would a projector consume progress out of process?", the honest answer was: through the outbox to a queue, using machinery that already exists — but that path carries **only status transitions**, because ticks are published straight to the stream and never staged. Getting the ticks across a process boundary means consuming the stream, and the survey that followed found the gap: of the three stream/queue sub-models the framework contracts, **the ack sub-model is the only one with no kit-level runner**. `AckStreamGroupQueryDepKey` appears in `contracts/stream` and in exactly one consumer — a private function inside a transport package.

---

## 1. The gap, precisely

The framework contracts four delivery models and ships runners for two:

| Sub-model | Port | Kit runner |
|---|---|---|
| Queue | `QueueQueryPort` | `QueueConsumer` ✅ |
| Offset-log (commit) | `CommitStreamGroupQueryPort` | `CommitStreamGroupConsumer` ✅ |
| Consumer group (ack) | `AckStreamGroupQueryPort` | **none** |
| Pub/sub (broadcast) | `PubSubQueryPort` | n/a by design (no delivery guarantee to run a loop around) |

The missing row is not theoretical: the socket.io gateway needs exactly this loop and therefore has one, `_consume_group_stream` — read a batch, process it, `claim` entries stranded by a crashed consumer, drop past a delivery ceiling, ack. That is the full ack-model consumer, written once, private, and unreachable by anything that is not the gateway (`forze_socketio` cannot be imported by kits, and the function is coupled to the gateway's own handler and stats types anyway).

So any second consumer of an ack stream group re-derives four subtleties that took the gateway real work to get right:

1. **Stranded entries are nobody's until claimed.** A group read never redelivers another consumer's pending entries, so a crashed worker's batch is invisible forever without an explicit `claim(idle=…)` pass. A naive loop looks perfect in tests and silently loses a batch per crash in production.
2. **A claim is a redelivery.** It increments the delivery count and resets the idle clock, so the poison ceiling has to be evaluated against a `pending` snapshot taken *including* the attempt about to run — the gateway's comment on this is a bug report in past tense.
3. **The PEL is unbounded.** Scanning it for delivery counts on every reclaim tick needs a bound, and the bound has to fail in the safe direction (an entry past it survives and is re-examined, never dropped on an unknown count).
4. **The batch is the unit boundary.** Stop must be honoured *between* batches, after the acks, or shutdown loses whatever was in hand.

## 2. What it would be

`AckStreamGroupConsumer` in `forze_kits/integrations/consumer/`, the third sibling, shaped like the two that exist: an attrs config object holding the stream spec, group, consumer name, handler, inbox spec and tx route; a `run(ctx, *, timeout=None, stop=None)` driving the loop; a `*_background_lifecycle_step` factory beside `queue_consumer_background_lifecycle_step`. The decision ladder is the queue runner's, with `claim` replacing broker redelivery:

- **read** a batch → **process** each message through `process_with_inbox` (dedup mark + handler in one transaction) → **ack**;
- **reclaim** stranded entries past `reclaim_idle`, poison-dropping past `max_deliveries` using a bounded `pending` snapshot;
- **stop** between batches; supervised restart on a broker fault via the shared `run_supervised`.

The gateway is then a candidate to refactor onto it — as a **separate phase, gated on the kit being proved by a second consumer first**. Refactoring the one working implementation onto brand-new shared code is how a working thing acquires someone else's bugs.

## 3. What stays out

- **No new port and no contract change.** The ack ports exist and are sufficient; this is a runner over them.
- **No in-process concurrency knob.** Same call as the queue runner's v1 (one sequential consumer per step; scale out with steps/processes), for the same reason: ordering and failure semantics stay explainable.
- **No pub/sub runner.** Broadcast has no delivery guarantee to build a loop around; a subscriber is a `tail()` and that is already the shape apps use.
- **No gateway rewrite in phase 1.** See above.

## 4. Why it is not the progress kit's job

The progress kit needs this only for the narrow case of a worker that reports progress and cannot reach the job store, and building it inside the progress kit would put a general consumer runner in a package about job records — the "mechanism without a consumer" shape the 7th-edition audit named. it documents the queue recipe (transitions only) and records the asymmetry there; this RFC owns the part that would let the ticks across.

## 5. Proof obligations (when built)

1. A consumer that crashes mid-batch strands its entries, and a second consumer's reclaim pass recovers **all** of them — the property a naive loop silently fails.
2. A claimed entry is processed once: redelivery + inbox dedup, asserted, not assumed.
3. The poison ceiling fires on the attempt that *reaches* it, not one redelivery later.
4. A PEL longer than the scan bound never causes a drop on an unknown delivery count.
5. Stop lands between batches: no unacked in-flight message at shutdown, no handler cancelled mid-flight.
6. Differential against the gateway's existing loop on the same stream — same acks, same drops, same order — since that loop is the de facto specification.

## 6. Gate

Build it when **two consumers** can be named that are asking, in the progress kit's two-consumer sense. Today's candidates:

- the socket.io gateway (an implementation exists — it is evidence of the shape, and it counts only once the refactor is actually wanted);
- the progress kit's out-of-process projector (**not asking** — every in-repo consumer of the progress plane runs where the store is);
- any future non-socket.io consumer of the realtime stream (an SSE-only deployment's analytics tap, an audit sink).

One consumer is a private function that already works. Parking is the correct state until a second one appears.

## 7. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Runner only — no new port, no contract change | locked |
| 2 | Third sibling in `forze_kits.integrations.consumer`, shaped like the queue runner (config object + `run` + background step) | locked |
| 3 | Reclaim (`claim`) replaces broker redelivery; inbox dedup is mandatory, not optional | locked |
| 4 | The gateway refactor is a **later phase**, gated on a second consumer proving the kit | locked |
| 5 | No in-process concurrency knob in v1 | locked |
| 6 | Parked until the §6 gate is met; rejected outright if it never is | locked (08-04) |
