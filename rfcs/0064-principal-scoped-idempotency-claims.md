# RFC 0064 — Principal-scoped idempotency claims

- **Status:** 📝 Draft — **execution-ready and the batch's only confirmed defect in shipped code.** The source proposal asked for a verification ("if it already is, document it"); it is not, and §3 shows what the current key admits.
- **Scope:** Add the acting principal to an idempotency claim's identity, so a key is unique per `(tenant, principal, op, key)` rather than per `(tenant, op, key)`. Touches the idempotency port's method signatures or its ownership mixin, the four stores, the AAD binding of the encrypted variant, and the replay semantics for a foreign key. **Behaviour change on a shipped mechanism**, which is why §5.5 carries a compatibility path.
- **Related:** [`src/forze/application/contracts/idempotency/ports.py:53-73`](../src/forze/application/contracts/idempotency/ports.py) (`begin(op, key, payload_hash)` and its documented `conflict` contract), [`src/forze/application/contracts/idempotency/ownership.py`](../src/forze/application/contracts/idempotency/ownership.py) (`ClaimOwnerMixin` — the precedent for adding an identity to a claim *without* changing the port signature, and the two-slotted-bases note), [`src/forze/application/contracts/idempotency/specs.py:32`](../src/forze/application/contracts/idempotency/specs.py) (the encrypted variant whose "AAD binds tenant + operation/key"), [`src/forze/application/hooks/idempotency/plans.py:104-133`](../src/forze/application/hooks/idempotency/plans.py) (`tenant_provider`, and where the key is read off `ctx.inv_ctx`), [`src/forze/application/contracts/authn/value_objects/identity.py:11-32`](../src/forze/application/contracts/authn/value_objects/identity.py) (`AuthnIdentity` — the shipped subject/actor pair `get_authn()` already returns), [RFC 0059](0059-non-disclosing-denials.md) (why a foreign key's answer must not be a distinguishable conflict).
- **Origin:** A working-time ledger where `client_request_id` is unique per table **globally**, so two employees generating the same UUID collide and the second gets a 422 that reveals the collision. Its stated correct behaviour: uniqueness per `(owner, key)`.

---

## 1. Summary

A claim is identified by `(tenant, op, key, payload_hash)` today. Two principals in one tenant
who submit the same idempotency key collide: with different arguments the second gets `conflict`,
and **with identical arguments the second is served the first principal's stored result.** Adding
the principal to the claim's identity closes both.

## 2. Motivation

An idempotency key is client-generated. Nothing makes it globally unique, and plenty makes it
collide: a client library seeded per install, a retried form submission with a key derived from
its contents, a mobile app that reuses a request id per screen, an integration that numbers its
requests from one.

Two consequences today, and the second is the serious one:

- **Different arguments ⇒ `conflict` (409).** The message is correct and the *existence* of the
  conflict tells caller B that caller A used that key. An enumerable key space becomes an oracle
  for other principals' activity — the same class of leak
  [RFC 0059](0059-non-disclosing-denials.md) closes for objects.
- **Identical arguments ⇒ caller B receives caller A's result.** This is the disclosure. Two
  principals submitting the same operation with the same normalized arguments is not exotic — an
  empty-bodied POST, a "start my shift" command whose arguments are the same for everyone — and
  the second caller is handed the first's stored record.

## 3. Current state

**A claim's identity is `(op, key, payload_hash)`, plus tenant, plus an owner *fence*.**
`begin(op, key, payload_hash)` is the port; every store inherits `TenancyMixin`, so the tenant is
in the key; `ClaimOwnerMixin` adds the *invocation's* `execution_id` as a fencing token so a late
`commit` from an overrun operation cannot overwrite a duplicate's live claim.

`ClaimOwnerMixin` is the precedent this RFC follows: an identity added to a claim, delivered as a
wired callable, with **no port signature change** — its docstring spells out why ("delivered the
way the tenant already is … so no port signature changes"). The principal can arrive the same way.

**Nothing in that identity is the acting principal.** Verified: the hook reads the key from
`ctx.inv_ctx.get_idempotency_key()` and the tenant from `ctx.inv_ctx.get_tenant`, and passes
neither a principal nor anything derived from one.

**The conflict contract is deliberate and documented.** The port docstring says `conflict` is part
of the contract rather than each store's choice, because the boundary renders 409 and clients must
not get different answers per store. So the *current* behaviour is intentional and correct for its
own model — the model is what this RFC changes.

**The encrypted variant binds AAD to "tenant + operation/key".** So the principal has to enter the
AAD too, or a re-scoped claim would be decryptable under the old binding — which makes this a
crypto-touching change, not a key-format change.

## 4. Goals / Non-goals

**Goals**

- A key belongs to the principal that used it; two principals using one key never meet.
- A replay attempted by a **different** principal is indistinguishable from an unused key — not a
  conflict, because a conflict is the oracle in §2.
- The AAD binding follows the claim's identity, so nothing is decryptable under the old scope.
- Existing deployments have a stated path, since this changes a shipped mechanism's semantics.

**Non-goals**

- **Not a new port.** `ClaimOwnerMixin` shows the shape: wired providers, no signature change.
- **Not key generation.** Clients generate keys; the framework does not mint them.
- **Not cross-principal deduplication.** "Two principals did the same thing once" is a business
  rule, not idempotency.
- **Not a change to the fencing token.** `execution_id` fencing stays exactly as it is; this is a
  different axis of the same claim.

## 5. Design

### 5.1 The principal in the claim

The acting principal is already on the invocation context: `get_authn()` returns
`AuthnIdentity(principal_id, actor)`
([`invocation.py:112`](../src/forze/application/execution/context/invocation.py)), whose
`principal_id` aligns with `PrincipalRef` and whose `actor` names the agent on a delegated call. So
the store takes a `principal_provider` beside `owner_provider`, wired like `tenant_provider` and
reading that identity rather than a new channel. Every store writes it into the claim and adds it to the predicate `begin`,
`commit` and `fail` already use.

**Degradation is where the two mixins differ.** `ClaimOwnerMixin` treats a missing owner as the
previous behaviour, because fencing is additive and refusing would break direct construction. A
missing *principal* cannot degrade the same way: an unscoped claim is exactly the defect. So the
rule is: **no principal available ⇒ the claim is scoped to the tenant with an explicit
`principal=None` marker**, and a claim written with a principal never matches one written without.
An unauthenticated operation's claims therefore live in their own space rather than sharing the
anonymous one with every authenticated caller.

### 5.2 A foreign key's answer

`begin` with a key that exists **for another principal** behaves as if the key were unused: a fresh
claim is taken in the caller's own space. No `conflict`, no record returned — the two claims are
different rows and neither knows about the other. That is what removes the oracle, and it falls out
of the key rather than being a case anybody has to handle.

### 5.3 The AAD

The encrypted variant's AAD becomes tenant + principal + operation/key. A record written under the
old binding does not open under the new one; §5.5 is how that is handled rather than discovered.

### 5.4 Replay by the same principal is unchanged

Same tenant, same principal, same op, same key: identical arguments replay the stored record,
different arguments are `conflict`. That is today's contract, and it is correct — the change is
only about who "same caller" means.

### 5.5 Compatibility

The claim's scope is part of its stored shape, so the change is visible rather than silent:

- Stores read and write a claim **version**. A v1 claim (no principal) is matched only by a v1
  lookup, and a v1 lookup happens only when no principal is available (§5.1).
- Consequence, stated plainly: **in-flight keys from before the upgrade do not replay for an
  authenticated caller after it.** Their operations may execute a second time if a client retries
  across the deployment. For a dedup window measured in hours this is a real exposure, and the
  mitigation is operational — drain the window, or accept it — not something the framework can
  hide.
- No dual-read fallback: reading v1 claims for authenticated callers would preserve exactly the
  cross-principal match this RFC exists to remove.

### Alternatives considered

- **Document the current behaviour as intended.** What the source proposal offered as the cheap
  outcome. Rejected on §2's second consequence: a client-generated key is not a secret, and
  "returns another principal's result" is not documentable as a feature.
- **Namespace the key at the boundary** (prefix the principal id onto the key in the FastAPI
  adapter). No core change, and it leaves every other surface — MCP, Socket.IO, an internal caller
  — unscoped, which is the same mistake [RFC 0059](0059-non-disclosing-denials.md) §3 caught in the
  denial posture.
- **Return `not_found` for a foreign key** instead of taking a fresh claim. Explicit, and it makes
  a legitimate first request fail because someone else picked the same key.
- **Hash the principal into `payload_hash`.** One fewer column, and it destroys the diagnostic — a
  conflict could no longer say whether the arguments or the caller differed.

## 6. Tests

- **The disclosure leg, first and named:** two principals, one tenant, same op, same key,
  **identical** arguments — the second must execute its own operation and must not receive the
  first's record. This is the test that fails today.
- Two principals, same key, different arguments: no `conflict` for either.
- Same principal, same key, identical arguments: replays the record (today's contract, pinned).
- Same principal, same key, different arguments: `conflict` (today's contract, pinned).
- Cross-tenant unchanged: a key in tenant A never meets tenant B's.
- Anonymous space: a claim written with no principal does not match one written with a principal,
  either direction.
- AAD: a record written under the old binding does not open under the new one, and a v1 claim is
  invisible to a principal-scoped lookup.
- All of the above **per store** (the four adapters), since the predicate is each store's code.
- DST: concurrent duplicate submissions from two principals with one key, asserting two effects and
  two records — with the fencing token's existing property still holding.

## 7. Docs

The idempotency page gains the scope sentence — **a key is scoped to tenant, principal and
operation** — and a release note that says what §5.5 says: in-flight keys do not carry across the
upgrade. The migration honesty matters more than the feature description here.

## 8. Out of scope

- **Purging v1 claims.** A sweep can come later; expiry already removes them.
- **Making the principal mandatory** (refusing an idempotency key on an unauthenticated operation).
  Defensible, and it would break public-endpoint retries that work today.
- **Cross-principal dedup.** §4.
- **Key format validation.** Whether a key is a UUID is the boundary's business.

## 9. Risks

- **Behaviour change on a shipped mechanism.** The exposure is §5.5's: an in-flight key stops
  replaying, so a client's retry can execute twice across the upgrade. Mitigation is operational and
  must be in the release note; hiding it behind a dual read would restore the defect.
- **Four stores, one predicate each.** A store that forgets the principal in `commit`'s predicate
  fences against the wrong claim. Mitigation: the battery runs per store, and the mixin owns the
  predicate construction so there is one place to get it right.
- **A `None` principal space that becomes the common case.** An app that never wires the provider
  gets the old behaviour under a new name. Mitigation: `check_wiring` warns when an idempotent
  operation resolves with no principal provider and an authn hook in its plan — the combination that
  says a principal exists and was not wired.
- **Two identities on one claim** (fencing owner, and now principal) reads as duplication.
  Mitigation: the docs table — the owner fences *which invocation* may commit, the principal scopes
  *whose* key it is.

## 10. Unresolved questions

- **Subject or actor?** `AuthnIdentity` carries both, and they differ on a delegated call: keying on
  the subject means an agent's retry replays the user's claim, keying on the actor means two agents
  acting for one user do not share one. Leaning: the **subject** (`principal_id`), because
  idempotency is about the effect on the user's data — and the row is graded so execution can
  depart with evidence.
- **Does a service-to-service call have a principal?** If not, those claims land in the `None` space
  and share it. A machine identity would be the better answer and is an identity question, not an
  idempotency one.
- **Does `check_wiring` warn or refuse** for the unwired-provider case in §9?

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | A claim is scoped to **tenant, principal, operation and key**. A client-generated key is not a secret, and two principals sharing one must not meet. |
| 2 | `LOCKED` | A key that exists for another principal behaves as **unused**, never as a conflict. The conflict's existence is the oracle, and a legitimate first request must not fail because someone else picked the same key. |
| 3 | `LOCKED` | The principal arrives as a wired provider with **no port signature change**, following `ClaimOwnerMixin`'s shape and for its stated reason. |
| 4 | `LOCKED` | A missing principal does **not** degrade to the old unscoped behaviour: the claim is written with an explicit `None` marker and never matches a principal-scoped one. Degrading here would preserve exactly the defect. |
| 5 | `LOCKED` | The AAD binding follows the claim's identity, so a record written under the old scope does not open under the new one. |
| 6 | `ASSUMED` | No dual-read of v1 claims for authenticated callers: the compatibility cost is stated in the release note instead, because a fallback restores the cross-principal match. |
| 7 | `ASSUMED` | Fencing by `execution_id` is untouched; this is a different axis of the same claim, and the docs carry the two-sentence table that keeps them apart. |
| 8 | `OPEN` | Whether the claim keys on `AuthnIdentity.principal_id` (the subject) or `.actor` on a delegated call, what a service-to-service caller uses, and whether the unwired-provider case warns or refuses at wiring. |

## 12. Phasing

- **P1** — the provider, the mixin, the predicate in all four stores, the claim version, the
  batteries per store (disclosure leg first).
- **P2** — the AAD change for the encrypted variant, with the non-openability battery.
- **P3** — the wiring signal for an unwired provider, and the docs plus release note.
