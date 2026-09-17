# RFC 0055 — Scoped disclosure: purpose-, period- and grantee-bound grants over frozen snapshots

- **Status:** 📝 Draft — the largest design in the batch, and **sequenced after [0052](0052-versioned-facts-correction-lineage.md), [0056](0056-sealed-preview.md) and [0060](0060-audit-spec.md)**: it needs addressable versions, a canonical fingerprint and an audit port, and building it first would invent all three privately.
- **Scope:** A `forze_kits.aggregates.disclosure` kit: an `AccessRequest` that carries no data, a `DisclosureGrant` bound to grantee, purpose, scope, period and a frozen snapshot, a `Snapshot` whose payload is immutable and fingerprinted, the four commands that move a request through its states, an authz resolution that admits a read only through exactly one live grant, a guard refusing destructive edits to disclosed facts, and audit-on-read. Touches `forze_kits` and adds a `CapabilityProvider`-shaped resolution hook in `forze_identity.authz` ([RFC 0057](0057-derived-capabilities-and-config-grants.md) owns the protocol). No document-port change.
- **Related:** [`src/forze_identity/authz/domain/models/bindings.py:181`](../src/forze_identity/authz/domain/models/bindings.py) (`DelegationGrant` — actor and subject, and nothing else), [`src/forze_identity/authz/services/policy.py:65`](../src/forze_identity/authz/services/policy.py) (`AuthzPolicyService.decide`, the `owner_id` ABAC check and the owner-override keys), [`src/forze/application/contracts/search/specs.py:252`](../src/forze/application/contracts/search/specs.py) (`SearchResultSnapshotSpec` — the framework's one existing "frozen result" precedent, TTL'd ids), [RFC 0056](0056-sealed-preview.md) (the fingerprint this binds), [RFC 0060](0060-audit-spec.md) (the port audit-on-read calls), [RFC 0052](0052-versioned-facts-correction-lineage.md) (why a disclosed fact must be addressable by version).
- **Origin:** A working-time ledger whose `disclosure/` package is the product: 844 lines plus routes and models, covering request-without-data, owner-grants-exactly-the-requested-scope, a frozen append-only month snapshot, re-grant that revokes and supersedes, audit only after authorization, 410 for revoked or superseded grants, and a month-wide refusal of destructive edits to disclosed facts. Two things it does not have: expiry, and owner-initiated revoke.

---

## 1. Summary

A data subject grants a named recipient access to a named slice of their own data, for a stated
purpose, for a bounded period, over a payload frozen at the moment of granting. The grantee can
read exactly that payload and nothing else; the owner can revoke; a re-grant supersedes; every
read is audited after authorization; and while a grant is live, the facts behind it cannot be
destructively edited.

## 2. Motivation

This is the GDPR shape, and it is a product category rather than a feature: a patient shares a
record with a clinic for one visit, an employee shares a month with HR, a tenant shares a
ledger with an auditor for one quarter. What the shape demands is specific, and each demand
rules out the obvious implementation:

- **The request carries no data.** A request that includes what it wants to see has already
  disclosed the shape of it.
- **The grant is bound to a payload, not to a query.** A grant over a live query discloses
  whatever the data becomes, which is not what the owner consented to.
- **A read is audited only after it is authorized.** Auditing the attempt records the grantee's
  interest in data they could not see, which is itself a disclosure.
- **Facts under a live grant cannot be destructively edited.** Otherwise the owner can withdraw
  evidence a recipient has already relied on.

Forze has none of it, and the piece it does have — `DelegationGrant` — is the wrong axis: it
delegates *who may act*, not *what may be seen*.

## 3. Current state

**`DelegationGrant` is `(actor_id, subject_id)`.** Verified at
[`bindings.py:181-202`](../src/forze_identity/authz/domain/models/bindings.py): a principal may
act on behalf of another, with no purpose, no period, no resource scope and no payload binding.
It is the "act as" primitive, and it is unbounded in exactly the dimensions a disclosure has to
bound.

**Authorization is catalog grants plus one ABAC check.** `AuthzPolicyService.decide` matches an
action against `EffectiveGrants.permissions` and, when the resource carries an `owner_id`
attribute, requires it to equal the subject — with `admin` / `<type>.admin` as documented
owner-override keys. There is no hook for "this principal may read this *object* because a
grant says so", which is the shape a disclosure needs: the decision is per object, not per
action.

**`EffectiveGrants` is closed over the catalog.** `resolve_effective_grants` unions
principal-permission, role-permission (with role lineage) and group-permission bindings, and
every `PermissionRef` carries a catalog `permission_id`. A grant that does not exist as a
catalog row has no representation today — which is why this RFC depends on
[RFC 0057](0057-derived-capabilities-and-config-grants.md)'s provider protocol rather than
inventing one.

**The framework's only frozen-result precedent is TTL'd ids.**
`SearchResultSnapshotSpec(enabled, ttl=5min, max_ids=50_000, chunk_size)` freezes an *ordered id
list* for pagination stability. It is not a payload, it expires, and it is not addressable as a
record — so it is a precedent for the idea and not a mechanism to reuse.

**There is no audit port** (verified: "audit" appears in `src/` only in prose and in DST's own
witness machinery), so audit-on-read has nowhere to land until
[RFC 0060](0060-audit-spec.md) ships.

## 4. Goals / Non-goals

**Goals**

- A request that carries no data, and a grant that carries the whole binding: grantee, purpose,
  scope, period, snapshot.
- Reads resolved through **exactly one** live grant, and refused with a status that does not
  reveal whether the object exists ([RFC 0059](0059-non-disclosing-denials.md)).
- Owner revoke and expiry as first-class, not as the two things left out.
- Re-grant supersedes atomically, so two live grants for one scope are impossible.
- Destructive edits to facts under a live grant refused, by scope.

**Non-goals**

- **Not a consent registry.** Consent for processing is a different lifecycle; this is one
  subject sharing one slice with one recipient.
- **Not an export.** A snapshot is read through the plane, not shipped as a file. The
  portability plane is [RFC 0065](0065-snapshot-consistent-export-verification.md)'s subject.
- **Not a sharing UI or a notification system.** Events are emitted; who gets told is the app's.
- **Not general object-level authorization.** The hook admits a read through a disclosure grant.
  A full ReBAC/Zanzibar surface is out of scope and deliberately not approached.

## 5. Design

### 5.1 The three documents

```python
AccessRequest(id, requester_id, owner_id, purpose, scope, state, created_at)
DisclosureGrant(id, owner_id, grantee_id, purpose, scope, snapshot_id,
                supersedes_id, granted_at, expires_at, revoked_at)
Snapshot(id, scope, payload, fingerprint, policy_version, frozen_at)
```

`scope` is a typed value object, not a string: `(spec_name, key_values, period)`. It has to be
comparable, because §5.3's supersede rule and §5.5's guard both range over "the same scope".

`payload` is written once and never updated — enforced by the kit registering the spec with no
update command, so "append-only" is a property of the wiring rather than a convention.
`fingerprint` is [RFC 0056](0056-sealed-preview.md)'s canonical fingerprint over the payload with
private fields excluded, which makes a snapshot citable: a recipient can prove what they were
shown.

### 5.2 The commands

- `request(owner_id, purpose, scope)` → an `AccessRequest`. Carries no data, and the requester
  learns nothing about whether the scope has any.
- `grant(request_id, expires_at)` — **owner only**. In one transaction: project the scope through
  a caller-supplied projector, freeze it as a `Snapshot`, insert the grant, and revoke plus
  supersede every live grant for the same `(grantee, scope)`.
- `revoke(grant_id)` — owner or grantee. Sets `revoked_at`; the grant stays readable as a record.
- `decline(request_id)` — owner. Terminal, and indistinguishable to the requester from a request
  that is simply unanswered, because the alternative tells them the owner saw it.

`expires_at` is **required** with no default. A nullable expiry is the origin application's
missing feature with a column for it; a framework default would pick a retention period for a
jurisdiction it knows nothing about.

The projector is the app's: only the app knows what "my March" means. The kit owns freezing it,
fingerprinting it and binding it.

### 5.3 Resolution: exactly one live grant

A read of `Snapshot(s)` by principal `p` is admitted iff exactly one `DisclosureGrant` has
`grantee_id = p`, `snapshot_id = s`, `revoked_at IS NULL` and `expires_at > now`. Not "at least
one": two live grants for one `(grantee, scope)` means the supersede rule failed, and admitting
the read would hide that. Zero and two are both refusals, and the two-grant case logs.

The hook is [RFC 0057](0057-derived-capabilities-and-config-grants.md)'s provider shape —
derived authorization from document state, which is exactly what this is.

### 5.4 Audit on read, after authorization

An admitted read records `(actor=grantee, action=snapshot.read, object=snapshot, subject=owner)`
through [RFC 0060](0060-audit-spec.md)'s port, **after** the decision. A refused read records
nothing, and §2 says why. Until 0060 ships the kit refuses to wire with audit enabled rather
than logging to nowhere.

### 5.5 The destructive-edit guard

`refuse_destructive_if_disclosed(scope)` — a `Before` hook on the app's own write operations,
refusing a delete or a hard update whose row falls inside any live grant's scope, as
`precondition`. Two things it is not: it is not a lock (a *correction* under
[RFC 0052](0052-versioned-facts-correction-lineage.md) is allowed, because it adds a version
rather than removing evidence), and it is not automatic — the app declares which operations it
guards, because only the app knows which of its writes are destructive.

### Alternatives considered

- **A grant over a live query instead of a frozen snapshot.** Simpler, no snapshot table, and it
  discloses whatever the data becomes. The owner consented to what they saw.
- **Extending `DelegationGrant` with purpose and period.** Reuses a shipped document, and
  conflates "may act as you" with "may see this": a delegate acts with the subject's
  permissions, a grantee reads one frozen payload with none.
- **Expiry as a scheduled sweep that revokes.** Needs a timer and leaves a window where an
  expired grant is live. Expiry is evaluated in the resolution predicate, and a sweep may
  *archive* later.
- **Auditing refused reads too.** Better forensics, and it accumulates a log of who tried to see
  whose data — a second disclosure surface. Refused.

## 6. Tests

- State machine: every legal transition, and every illegal one refused (`grant` on a declined
  request, `revoke` of a revoked grant, a second `grant` producing exactly one live grant).
- Snapshot immutability: the spec has no update command, and an attempt to write one fails at
  wiring; the fingerprint matches [RFC 0056](0056-sealed-preview.md)'s over the same payload.
- Resolution: zero, one and two live grants; an expired grant refused at the boundary second;
  a revoked grant refused; a grantee reading another snapshot refused with the same status and
  body as a nonexistent one ([RFC 0059](0059-non-disclosing-denials.md)).
- Audit: exactly one row per admitted read, none per refused read.
- Guard: a destructive op inside a live scope refused; a correction inside it allowed; the same
  op outside every scope allowed.
- DST: `grant` racing `revoke` on one scope, and two concurrent `grant`s, with a history
  invariant asserting no interval has two live grants for one `(grantee, scope)` — the
  `mutual_exclusion` shape over grant intervals.
- **Not tested:** that the projector produces the right payload. That is the app's.

## 7. Docs

Its own page, not a section: this is a product shape, and the page has to carry the threat model
— what a grantee can and cannot see, what the owner can take back, what a snapshot proves, and
the sentence that stops the wrong reading: **a snapshot is a disclosure record, not a backup,
and freezing it does not make the underlying facts immutable.**

## 8. Out of scope

- **Delegated re-sharing** (a grantee granting onward). Named because every sharing product grows
  it; it needs a policy about whose consent bounds the chain.
- **Partial-field grants** within a scope. The scope is the unit; field-level narrowing is the
  projector's job today.
- **Notification and reminders.** Events are emitted, nothing is delivered.
- **Automatic scope inference.** The app declares scopes; guessing them from a spec is how a
  disclosure over-collects.

## 9. Risks

- **A framework feature that reads as compliance.** Shipping "GDPR-shaped sharing" invites the
  reading that an app using it is compliant. Mitigation: the docs state what it is — one
  mechanism, no legal claim — and the page leads with the threat model.
- **The kit is large enough to be a plane.** 844 lines in the origin, and this design is not
  smaller. Mitigation: it composes shipped primitives (documents, invariants, hooks, audit) and
  introduces no port; if it needs a port, that is the signal to split it.
- **Four dependencies before it can ship.** 0052, 0056, 0057, 0060. Mitigation: that is the
  sequencing, stated in the status line; building it first would privately reinvent all four,
  which is precisely what the origin did.
- **Audit-on-read is a write on the read path.** It costs a row per read and can fail. The port
  contract decides whether a failed audit fails the read; the leaning is yes for a disclosure
  read specifically, and [RFC 0060](0060-audit-spec.md) owns that decision.

## 10. Unresolved questions

- **Does a failed audit fail an admitted disclosure read?** Failing closed loses availability;
  failing open loses the record of a disclosure that happened. Leaning: fail closed here, and
  let 0060's default be the opposite for ordinary actions.
- **Where does the snapshot payload live?** A document field (queryable, size-bounded by the
  row) or the storage plane (arbitrary size, one more dependency). Leaning: document field first,
  storage as the declared escape hatch.
- **Is `scope` comparable by structural equality or by a canonical key?** §5.3 and §5.5 both
  compare scopes; a canonical string key is indexable, structural equality is honest about
  nesting.
- **Does the grantee see the owner's identity?** In the origin they do. It is a policy question
  an app may need to answer differently, so it may have to be a declaration.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | A grant binds a **frozen snapshot**, never a live query. The owner consented to what they saw, and a query re-evaluated later discloses what they did not. |
| 2 | `LOCKED` | A request **carries no data** and a decline is indistinguishable from silence. A request that names what it wants has already disclosed the shape of it. |
| 3 | `LOCKED` | `expires_at` is **required, with no framework default**. A nullable expiry is the origin's missing feature with a column for it, and a default would pick a retention period for an unknown jurisdiction. |
| 4 | `LOCKED` | A read is admitted through **exactly one** live grant; zero and two are both refusals, and two logs. "At least one" would hide a failed supersede. |
| 5 | `LOCKED` | Audit records **admitted reads only**. A log of refused attempts is a record of who wanted to see whose data — a second disclosure surface. |
| 6 | `ASSUMED` | Expiry is evaluated in the resolution predicate, not by a revoking sweep. A sweep leaves a live window and needs a timer; archiving expired grants may still be a sweep later. |
| 7 | `ASSUMED` | The guard refuses destructive writes but permits a **correction** ([RFC 0052](0052-versioned-facts-correction-lineage.md)): a correction adds a version, it does not remove evidence a recipient relied on. |
| 8 | `ASSUMED` | The projector is the app's; the kit owns freezing, fingerprinting and binding. Only the app knows what a scope means. |
| 9 | `OPEN` | Whether a failed audit write fails an admitted disclosure read (§10, leaning yes for this kit and no for ordinary actions). |
| 10 | `OPEN` | Snapshot payload in a document field or on the storage plane, and whether `scope` compares structurally or by a canonical key. |

## 12. Phasing

- **P1** — the three documents, the four commands, the supersede rule, resolution through the
  provider hook, batteries and the DST leg. Requires 0052 (addressable versions), 0056
  (fingerprint), 0057 (the provider protocol).
- **P2** — audit-on-read, once [RFC 0060](0060-audit-spec.md) ships; until then the kit refuses to
  wire with audit enabled.
- **P3** — the destructive-edit guard as a declarable hook, plus the docs page's threat model.
- **P4** *(demand-gated)* — storage-backed payloads, scope narrowing, onward sharing.
