# RFC 0025 — The failure-disposition ladder

- **Status:** 📝 Draft — problem stated, design open. Written to hold the decision, not to make it.
- **Scope:** What a delivery loop does with a failure it cannot complete, and whether it can enact that choice on the delivery it chose it for. Today the first is decided by `exception_egress_policy(kind).retryable`, a boolean, plus per-loop code checks — the loops between them already act on **four** dispositions and one path opts out of the vocabulary entirely. The second is not decided anywhere.
- **Related:** [`exception_egress_policy`](../src/forze/base/exceptions/egress.py) (the boolean), `is_draining_refusal` / `is_payload_cipher_missing` (the two classifications already shared), the queue and commit-stream consumer runners, the outbox relay, `saga_executor`, `resilience/executor.py`; for §2, the routed clients [`forze_rabbitmq`](../src/forze_rabbitmq/kernel/client/routed_client.py) and [`forze_kafka`](../src/forze_kafka/kernel/client/routed_client.py) over the guarded LRU tenant pool.
- **Origin:** Two independent instances of "an `infrastructure` kind is retried forever" — counter overflow on Redis/Mongo, and a missing storage bucket. The storage half was fixed by reclassification (a missing bucket is now `configuration`); the counter half cannot be, because the store does the arithmetic and reports it in prose. Chasing a shared mechanism for both surfaced the real finding: `retryable: bool` is not the vocabulary these loops speak. §2 was folded in from review of #337, which found the same conclusion approached from the settlement side.

---

## 1. The problem

`ExceptionKind` → `retryable: bool` is the only shared statement the framework makes about what to do with a failure. The loops need more than two answers, so each has grown its own:

| Disposition | Meaning | Where it is decided today |
| --- | --- | --- |
| **Retry** | redeliver; the condition may clear | `exception_egress_policy(kind).retryable` |
| **Poison** | park *this* message; the rest are fine | the fall-through from the above |
| **Abort the pass** | stop; *nothing* here can progress | `is_payload_cipher_missing`, three sites |
| **Retry, uncounted** | redeliver without advancing the poison ceiling | `is_draining_refusal`; the config-fault streak |

The third and fourth are not expressible as `retryable` true or false, which is why both are keyed on a **code** rather than a kind. That is not an accident of style — a code round-trips a durable journal and a Temporal failure conversion, and a kind-derived boolean cannot carry a nuance the kind does not have.

Two further facts frame the design:

- **The consequence is legitimately per-loop.** A queue consumer's "retry uncounted" is `nack(requeue=True, count=False)`; the offset-log twin's is "commit what is earned, rewind, stop" — because an ordered log cannot skip a message and come back. `_draining.py` already made this call explicitly: share the *classification*, leave the *consequence* to each ladder. Any design here inherits that split or argues against it.
- **One path takes no disposition at all.** The queue consumer's handler branch requeues every exception except draining ([runner.py:599](../src/forze_kits/integrations/consumer/runner.py#L599)). Kind never reaches it, so `max_deliveries` — opt-in, and inert on brokers that do not report delivery counts — is the only thing that stops a permanently-failing handler.

## 2. The other half: a disposition the loop cannot enact

Every row above assumes something the framework does not guarantee — that when a loop
settles a message, the settlement reaches the delivery it names. Under the routed
(multi-tenant) clients it does not, and the failure is silent.

A routed client leases a per-tenant client from the guarded LRU pool for the duration of
**one call**. The state a settlement needs outlives that lease:

- **RabbitMQ.** A delivery tag lives in the per-client `__pending` map, and `receive` and
  `ack` take separate leases. An eviction between them sends the ack to a *different*
  client, which finds no tag and returns `0` — a return
  [the runner discards](../src/forze_kits/integrations/consumer/runner.py#L164). The
  evicted client requeues on close, so "ack it" and "poison it" both come out as *retry*.
- **Kafka.** `get_consumer` releases the lease when it *returns* the consumer; the adapter
  then polls an object owned by a client that may be evicted and closed underneath it.

This is not a fifth disposition. It is the precondition the other four share, and it fails
in the direction that costs most: not an error, but a settlement that silently did nothing.
A ladder that names four dispositions on a rotating pool where none of them is guaranteed
to land has settled the vocabulary and left the mechanism.

Both instances **pre-date** #337 — routed `receive` and `ack` have always taken separate
leases. What #337 changed is the exposure, by making a routed consume stream survive
rotation instead of dying with it.

The shape of a fix is an ownership handle: a settlement carries the identity of the client
that produced the delivery, and the lease is held until settlement or discard. `ack(queue,
ids)` cannot express that, so this is a port-surface change — which is the reason it is
folded in here rather than patched. Two local patches would produce two more per-loop
conventions, which is the finding of §1 repeated in a new place.

## 3. The questions this RFC exists to answer

**(a) What should a delivery loop do when a step can never succeed?**

Not "how do we name it". The naming is easy; every candidate answer for the handler path has teeth:

- *poison it* — a transient misconfiguration destroys the backlog at consume speed, the exact outcome `is_payload_cipher_missing` exists to prevent;
- *retry uncounted* — today's unbounded spin, now blessed by design;
- *abort the pass* — one bad route stops a consumer serving many.

The split is probably **per-message vs per-deployment** — which is precisely the distinction the cipher-missing rule already draws by hand, for exactly one condition. Generalising that distinction is the substance of this RFC.

**(b) What lets a loop enact the disposition it chose?**

Whatever §2 resolves to has to hold for *every* disposition, not just the settling ones —
"retry, uncounted" is as unenactable as "poison" if the count lives on a client that is
gone. The two questions are one design because the answer to (a) is a vocabulary and the
answer to (b) is what makes any word in it mean something.

## 4. What is already true (do not re-litigate)

- Classification is shared; consequence is not (`_draining.py`, and now `is_payload_cipher_missing`).
- All three cipher-missing sites are verified, so the drain-gate failure mode — a rule present in one runner and absent from its twin — cannot recur *for the rules that exist*.
- A missing bucket is `configuration`, non-retryable, on S3, GCS and the mock.
- Counter overflow stays `infrastructure`/retryable and is catalogued DECLARED: detecting it needs error-text matching, rejected deliberately.
- The lease itself is sound *within* a call, and must not span a `yield`: a lease held
  across one lasts as long as the caller's processing, and a guarded eviction waiting on it
  deadlocks. That is measured — `test_routed_rabbitmq_guarded_registry_full_facade` hung on
  exactly that shape. Any ownership handle §2 proposes has to bound the lease by
  *settlement*, not by suspending the loop inside it.

## 5. Non-goals

- A per-instance `retryable` override on `CoreException`. Considered and rejected: after the storage reclassification it has exactly one user, and a new field must survive every serialization boundary (durable journal, Temporal failure, outbox envelope, HTTP envelope) or silently fail **open** — back to retrying. Codes already round-trip by construction.
- A registry of non-retryable codes. Same single user, and it hardens the two-value vocabulary this RFC says is too narrow.
- Reclassifying the 188 `exc.infrastructure` sites. A sweep on the scale of the 637→99 `exc.internal` pass; out of scope until the vocabulary is settled.
- Pinning a client for the life of a routed consume stream. It re-creates the deadlock in §4 and defeats the pool: a tenant that stops iterating holds a slot indefinitely.
- Making the runner raise on `ack` returning `0`. It converts a silent non-settlement into a loud one without making the settlement land, and on a non-routed client `0` is already a legitimate "someone else acked it".

## 6. Open questions

1. Is the disposition a property of the failure, of the loop, or of the pair? (`_draining.py` says: classification of the failure, consequence of the loop.)
2. Does the handler path get a disposition, and does it need a classification the handler itself can raise — an explicit "this will never succeed" the application can signal?
3. Should `max_deliveries` default to a finite value? It is the only backstop the handler path has, and it is off by default.
4. Do the four dispositions cover the offset-log runner's "pause the group and alert", or is that a fifth?
5. Where does the ladder live so that `forze.application` may not import `forze_kits` — core classification, kit consequence?
6. Does settlement take an opaque ownership token minted by `receive`, or does the routed
   client keep an internal delivery → client map and pin only what is unsettled? The first
   changes the port surface for every broker; the second keeps it, but makes eviction
   conditional on outstanding work — which is a pool-policy change, not a port one.
7. Is a consumer handle (Kafka) the same problem as a delivery tag (RabbitMQ), or does a
   long-lived handle want a lease that the *holder* releases — closer to `client_scope`
   than to a settlement token?
8. What is the honest guarantee when eviction is unavoidable — a closing tenant, a pool at
   capacity? "At-least-once, and a rotation may duplicate" is defensible if it is
   *documented*; today it is neither guaranteed nor written down.

## 7. Decision log

*(empty — this RFC records a problem, not yet a resolution)*
