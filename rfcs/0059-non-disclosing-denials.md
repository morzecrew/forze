# RFC 0059 — Non-disclosing denials

- **Status:** 📝 Draft — execution-ready, and a prerequisite for [RFC 0055](0055-scoped-disclosure.md) and [RFC 0057](0057-derived-permissions-and-config-grants.md), both of which need a denial that does not answer "does this object exist?".
- **Scope:** A declared denial posture that collapses an authorization refusal on a resource into the same status, body and code as a not-found for that resource type, and an `owned_by` predicate on the document read port so ownership is part of the lookup rather than a check after it. Lands in `forze.base.exceptions` (the shared envelope) plus the read port and its adapters — **not** in the FastAPI adapter, for the reason §3 gives. One contract addition, no change to `ExceptionKind`.
- **Related:** [`src/forze/base/exceptions/envelope.py:139`](../src/forze/base/exceptions/envelope.py) (`error_envelope` — the one projection FastAPI and Socket.IO both render), [`src/forze/base/exceptions/egress.py:97-122`](../src/forze/base/exceptions/egress.py) (`_EXC_KIND_HTTP_STATUS`: `AUTHORIZATION` → 403, `NOT_FOUND` → 404; `exception_egress_policy`), [`src/forze_fastapi/exceptions.py:34`](../src/forze_fastapi/exceptions.py) (`build_core_exception_response`, and the `X-Error-Code` header), [`src/forze/application/contracts/document/ports.py:80`](../src/forze/application/contracts/document/ports.py) (`get(pk, *, for_update, skip_cache)` — no ownership argument), [`src/forze_identity/authz/services/policy.py:95`](../src/forze_identity/authz/services/policy.py) (the `owner_id` ABAC check that runs *after* the read).
- **Origin:** A working-time ledger with one constant 403 body for every `/me/*` denial — missing capability, unmapped subject, inactive employee — and one constant 404 for unknown, inactive or foreign targets, with ownership in the `WHERE` predicate so a foreign id is indistinguishable from a nonexistent one, and denials raised before any data is materialized.

---

## 1. Summary

A deployment declares that, for a resource type, an authorization failure and a not-found are
the same answer: same status, same body, same error code, no context. Ownership moves into the
lookup — `get(pk, owned_by=...)` raises `not_found` rather than returning a row somebody else
owns — so the 403/404 split cannot reappear one handler at a time.

## 2. Motivation

The 403-versus-404 split is an existence oracle. A caller who gets 403 on one id and 404 on
another has learned which ids exist, and in a multi-user product that is the whole enumeration
attack: iterate ids, keep the 403s, and you have a list of other people's objects — their
count, their id space, sometimes their creation rate.

Forze maps the two kinds to the two statuses today, and exposes a `context` field on the
client-safe ones, which is the leak at its most precise: not just *which* ids exist, but a
sanitized detail about the denial.

## 3. Current state

**One projection serves every transport.** `error_envelope(exc)` computes status, detail,
`retryable`, `server_error` and `context`; `forze_fastapi.build_core_exception_response` renders
it, and its docstring says the Socket.IO transport renders the same envelope "so both stay in
lock-step". So a denial posture implemented in the FastAPI adapter — which is what the source
proposal asked for — would leave every other surface leaking. **It belongs in the envelope.**

**The mapping is exactly the one §2 describes.** `_EXC_KIND_HTTP_STATUS` has
`AUTHORIZATION: 403` and `NOT_FOUND: 404`; `error_envelope` attaches sanitized `context` when
the kind's egress policy allows details and the error is not a server error. Nothing is wrong
with any of that in isolation; together they answer the existence question.

**Ownership is checked after the read.** `AuthzPolicyService.decide` compares
`resource.attributes["owner_id"]` to the subject — which means the row has already been read to
produce the attribute. The read port offers no ownership predicate:
`get(pk, *, for_update, skip_cache)`. So even a correct handler reads a foreign row before
refusing, and a *careless* one returns it.

**There is no `context`-stripping switch.** The field is per-kind via the egress policy, which is
a framework-wide default, not a per-resource-type posture.

## 4. Goals / Non-goals

**Goals**

- One declared posture, applied by the shared envelope, so every transport inherits it.
- Ownership in the lookup, so the refusal happens before a row is materialized.
- Byte-identical denials across the enumerated reasons — asserted, not asserted-by-inspection.
- No change to what handlers raise: a handler keeps raising `authz` and `not_found` for the
  reasons it always did.

**Non-goals**

- **Not collapsing every denial.** Authentication stays 401: "you are not logged in" reveals
  nothing about an object, and collapsing it into 404 breaks every client's login redirect.
- **Not hiding errors from logs.** The server-side record keeps the real kind and reason; this is
  about what crosses the boundary.
- **Not a rate limiter.** Enumeration is also a volume problem; throttling is the throttled kind's
  business.
- **Not per-route configuration.** The posture is per resource type. A route-level posture is how
  two routes over one resource end up disagreeing.

## 5. Design

### 5.1 The posture

```python
DenialPosture(mode="non_disclosing", resource_types=frozenset({"work_interval", "snapshot"}))
```

Bound once at runtime construction and read by `error_envelope`. For a `CoreException` whose kind
is `AUTHORIZATION` and whose details name a covered resource type, the envelope returns the
not-found status, the not-found detail, the not-found code and **no context**. Everything else is
unchanged.

The code matters as much as the body: `X-Error-Code` is a header on every FastAPI error response,
so a collapsed denial that keeps its own code is still an oracle.

Default is `mode="standard"` — today's behaviour. Turning the posture on changes what clients
see, and a framework that silently rewrites 403s into 404s on upgrade would break every client
that branches on them.

### 5.2 Ownership in the lookup

```python
def get(self, pk: UUID, *, owned_by: OwnedBy | None = None, for_update=False, skip_cache=False)
OwnedBy(field="owner_id", value=principal_id)
```

`owned_by` becomes part of the predicate, so a row owned by someone else is **not found** — the
same answer as a row that does not exist, produced by the same code path rather than by a policy
that collapses two answers afterwards. Additive and optional; every adapter that already builds a
filter can add one term, and the mock enforces it so DST sees the same semantics.

`get_many` takes the same argument: the plural form filters, and a caller learns nothing from a
short result about *which* ids were foreign versus absent.

### 5.3 The cache interaction

A cached row must not be served past an ownership predicate. The document cache is keyed by pk, so
`owned_by` participates in the cache key or bypasses the cache — the RFC requires participation,
because bypassing turns every owned read into a database read and quietly removes a shipped
feature's benefit.

### 5.4 What stays visible

`authz` still renders 403 for **action**-level denials with no resource: "you may not create
invoices" reveals nothing about an object. The collapse applies to a denial *about a resource*,
which is the case where the answer doubles as an existence check.

### Alternatives considered

- **Do it in the FastAPI adapter** (the source proposal). Smaller diff, and §3 shows it leaves
  Socket.IO, MCP and any future transport leaking, because they render the same envelope.
- **Always collapse, no declaration.** One behaviour, no configuration — and it changes every
  existing client's contract on upgrade, and it is wrong for a public API that deliberately says
  403.
- **Collapse in the authz hook** by raising `not_found` instead of `authz`. Loses the real kind on
  the server side, so logs and metrics can no longer tell a denial from a miss.
- **Keep `context` on collapsed denials, strip only the status.** A sanitized context still names
  the resource and often the reason, which is most of the oracle.

## 6. Tests

- **Byte equality across reasons**, the load-bearing battery: for a covered resource type, the
  responses for a missing capability, an inactive principal, a foreign row and a nonexistent row
  are compared **as rendered bytes plus headers** — status, body and `X-Error-Code`. Comparing
  statuses only is how the code header keeps leaking.
- The same comparison over the Socket.IO envelope, since that is the claim §3 makes about the
  shared projection.
- Posture off (default) ⇒ 403 stays 403 and keeps its context.
- Action-level `authz` with no resource ⇒ still 403 with the posture on.
- `owned_by`: a foreign row raises `not_found`; the same row read by its owner returns; `get_many`
  filters foreign ids; the cache does not serve a foreign row after the owner read it (the
  cache-key leg).
- DST: an invariant over the run's error records asserting that, per covered resource type, every
  denial rendered identically — the `denial_bodies_identical` property, as a history invariant.
- **Not tested:** that enumeration is impossible. Timing and volume channels are out of scope
  (§8); what is asserted is response equality.

## 7. Docs

A short section in the errors page, with the honest framing: **this closes the response-shape
oracle, not the timing one**, the posture is off by default, and switching it on is a
client-visible contract change. Plus `owned_by` in the document-port reference, named as the
preferred way to express ownership.

## 8. Out of scope

- **Timing side channels.** A foreign row that exists may still take longer to refuse than a
  missing one. Named because it is the next question a reviewer asks; closing it needs constant-
  time paths this design does not attempt.
- **Volume.** Enumeration at scale is throttling's job.
- **Collapsing 401.** §4.
- **Per-route postures.** §4.

## 9. Risks

- **A client-visible contract change.** Any client branching on 403 versus 404 breaks when the
  posture is enabled. Mitigation: off by default, documented as a contract change, and per
  resource type so it can be adopted a type at a time.
- **Security theatre if adopted partially.** Covering one resource type while a sibling route over
  the same data stays standard leaks through the sibling. Mitigation: the posture keys on resource
  type rather than route, so every route over that type inherits it; the docs say to enumerate the
  types, not the endpoints.
- **`owned_by` and the cache.** Getting the key wrong serves a foreign row from cache — a worse
  bug than the one being fixed. Mitigation: §5.3 requires key participation and the battery pins
  it.
- **Debuggability.** Support loses the ability to tell a customer "you lack permission" from "it
  does not exist". Accepted: the server-side record keeps both, and that is where the answer
  belongs.

## 10. Unresolved questions

- **How does the envelope learn the resource type?** From `exc.details` by convention, or from a
  typed field on `CoreException`. A convention over a details key is weaker; a new field touches
  the core exception. Leaning: a typed optional field, because a convention that a handler forgets
  fails open.
- **Is `owned_by` a filter expression or a narrow value object?** A full expression is flexible and
  invites an ownership rule the cache key cannot represent.
- **Does `list` take `owned_by` too?** Listing already filters, so an app can express it — but the
  same argument said that about `get`.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The posture lives in the **shared envelope**, not the FastAPI adapter. Every transport renders `error_envelope`, so an adapter-level fix leaves the others leaking — and the source proposal's version would have. |
| 2 | `LOCKED` | A collapsed denial matches the not-found answer in **status, body and error code**. `X-Error-Code` is on every response, so a kept code is still an oracle. |
| 3 | `LOCKED` | Handlers keep raising `authz`; the collapse happens at egress. The server-side record must retain the real kind, or logs and metrics can no longer distinguish a denial from a miss. |
| 4 | `LOCKED` | Default is **off**. Enabling it changes what clients see, and doing that on upgrade would break every client that branches on 403. |
| 5 | `ASSUMED` | Ownership belongs in the lookup (`owned_by`), so a foreign row is not-found by construction rather than by a policy applied after the row was read. |
| 6 | `ASSUMED` | The posture keys on **resource type**, not route, so every route over that type inherits it and partial adoption is harder to get wrong. |
| 7 | `ASSUMED` | `authz` without a resource still renders 403: an action-level denial reveals nothing about an object, and collapsing it would cost clarity for nothing. |
| 8 | `OPEN` | How the envelope learns the resource type (a `details` convention versus a typed field on `CoreException`), and whether `owned_by` is a value object or a filter expression. |

## 12. Phasing

- **P1** — `owned_by` on `get` / `get_many`, adapters, the cache-key rule, batteries. Useful alone:
  a foreign row stops being readable by a careless handler.
- **P2** — `DenialPosture` in the envelope, the byte-equality battery across both transports, the
  docs section. Unblocks [RFC 0055](0055-scoped-disclosure.md) and
  [RFC 0057](0057-derived-permissions-and-config-grants.md)'s `require_permission`.
- **P3** — the DST history invariant over rendered denials.
