# RFC 0056 — Preview binding: what you saw is what you submit

- **Status:** 📝 Draft — execution-ready, one small PR. [RFC 0055](0055-scoped-disclosure.md) depends on it.
- **Scope:** A `forze_kits.domain.preview` module: a `Reviewed[T]` wrapper carrying a fingerprint, a `ReviewedCommand` mixin whose handler recomputes the projection and refuses on mismatch, and one decorator that wires the check into an operation. Built on the shipped `stable_payload_fingerprint`; **no new hashing, no port, no contract change.**
- **Related:** [`src/forze/base/primitives/fingerprint.py:80`](../src/forze/base/primitives/fingerprint.py) (`stable_payload_fingerprint`, canonical-JSON SHA-256 with a prefix), [`src/forze/application/contracts/querying/pagination/cursor_token.py:276`](../src/forze/application/contracts/querying/pagination/cursor_token.py) (`fingerprint_filter` — the precedent for binding a client-held token to server state, and its PYTHONHASHSEED note), [`src/forze/application/contracts/idempotency/ports.py:53`](../src/forze/application/contracts/idempotency/ports.py) (`begin(op, key, payload_hash)`, the adjacent mechanism this is **not**), [RFC 0055](0055-scoped-disclosure.md) (the consumer that needs a snapshot to be citable).
- **Origin:** A working-time ledger whose preview endpoint returns a projection plus a SHA-256 over its canonical JSON with private fields excluded; the submit command recomputes the projection and refuses with `SUBMISSION_CHANGED` if the fingerprint differs, so a recipient can only ever receive exactly what the submitter previewed.

---

## 1. Summary

A preview returns its data and a fingerprint. The confirming command takes that fingerprint
back, recomputes the projection at commit time, and refuses as `precondition` if anything
changed. One field on the DTO, one decorator on the operation.

## 2. Motivation

"Confirm this" flows are everywhere — quotes, filings, consent forms, invoices, disclosure
grants — and they all have the same gap between preview and submit. The facts can move in
between: another user edits a row, a scheduled job lands, a correction arrives. The submitter
then confirms something they never saw, and in a disclosure flow they disclose something they
never saw.

Forze has idempotency keys, which solve the neighbouring problem (*the same request twice*) and
not this one (*a different state than the one reviewed*). Nothing binds a command to the state
the caller read.

## 3. Current state

**The hashing primitive exists and is already trusted for this class of job.**
`stable_payload_fingerprint(payload, prefix="sha256")` hashes canonical, key-sorted JSON via
`stable_json_bytes` and returns `"sha256:<hex>"`. The cursor-token path uses the same idea with
its own canonicalizer and states the property this depends on:
PYTHONHASHSEED-independence, "computed identically at mint and verify".

**Nothing binds a command to a reviewed state.** The idempotency claim is
`(op, key, payload_hash)` scoped by tenant — `payload_hash` is a hash of *the caller's own
arguments*, which is exactly the thing that did not change. Two duplicates of one request share
it; a request against changed server state also shares it.

**There is no projection concept at the operation layer.** A preview today is an ordinary query
operation whose result the app assembles. That is fine and is why this design is small: the
projector stays the app's function, and the module owns only the fingerprint and the refusal.

## 4. Goals / Non-goals

**Goals**

- One fingerprint computation, shared by preview and submit, so the two cannot drift.
- A mismatch is a **refusal**, never a merge or an overwrite.
- Private and volatile fields excluded from the fingerprint by declaration, so a rendering
  detail does not invalidate every preview.
- Usable without the rest of the batch: a small app should be able to bind one flow.

**Non-goals**

- **Not optimistic concurrency.** `rev` is per row; this is per *projection*, which may span
  rows and specs. An app wanting row-level conflict detection already has `rev`.
- **Not idempotency.** Different question, shipped mechanism, and §3 says why it does not
  transfer.
- **Not a signed token.** The fingerprint is a hash the server recomputes, not a bearer of
  authority. An app that needs the client's copy to be tamper-evident signs it with the cursor
  token machinery instead, and §9 records why signing is not the default.
- **Not a lock.** Nothing is held between preview and submit. The mismatch is detected, not
  prevented — preventing it is [RFC 0063](0063-per-owner-write-serialization.md)'s axis.

## 5. Design

### 5.1 The wrapper and the mixin

```python
class Reviewed[T](BaseDTO):
    data: T
    fingerprint: str          # "sha256:…" over data, minus the excluded fields

class ReviewedCommand(CoreModel):
    fingerprint: str = Field(frozen=True)
```

```python
def canonical_fingerprint(model: BaseModel, *, exclude: frozenset[str] = frozenset()) -> str
```

`exclude` names fields the fingerprint ignores: a rendering hint, a computed label, a
`generated_at` stamp. Declared per projection, in one place both sides read — the whole failure
this prevents is a preview and a submit that exclude different fields and never match.

### 5.2 The check

```python
@confirms_preview(projector=preview_for, exclude=frozenset({"generated_at"}))
async def submit(ctx, args: MySubmitCmd) -> Out: ...
```

The decorator wraps the handler: recompute `projector(ctx, args)`, fingerprint it, compare to
`args.fingerprint`, and raise `precondition(preview_changed)` on mismatch. Inside the operation's
transaction, so what is compared is the state the handler is about to write against — comparing
before the transaction opens reintroduces the gap this closes, one transaction narrower.

A `wrap` middleware rather than a `before` hook, for that reason: `before` runs outside the
handler's transaction.

### 5.3 What the caller sees

`precondition` renders 400 with the code, and the detail says the preview is stale without
saying what changed. Saying what changed is a disclosure: the caller may no longer be
authorized to see the new value. The remedy is stated as a behaviour — re-preview and confirm
again — not as a diff.

### Alternatives considered

- **Sign the fingerprint** so a tampered client copy is detected. A tampered fingerprint makes
  the submit *fail*, which is the safe direction, so signing buys nothing against a client that
  is only hurting itself. Named as the escape hatch for a flow where the preview is produced by
  one party and confirmed by another.
- **Store the preview server-side and hand back an id.** Stronger (the reviewed payload is
  retrievable, not just verifiable) and it needs a store, a TTL and a cleanup path. That
  mechanism is [RFC 0055](0055-scoped-disclosure.md)'s `Snapshot`, which is exactly this design
  plus persistence — which is the argument for keeping this one stateless.
- **Compare `rev`s of the rows the projection touched.** Precise for single-spec projections and
  wrong for anything computed, aggregated or cross-spec.

## 6. Tests

- Round trip: preview, submit, success. Preview, mutate an included field, submit → `precondition`
  with the code. Preview, mutate an excluded field, submit → success.
- Field-set asymmetry: a projector whose `exclude` differs between the two sides is the defect
  the shared declaration prevents; the battery pins that the decorator and the preview read the
  same declaration, by using it from one place.
- Determinism: the same payload fingerprints identically across processes — the subprocess-×4
  shape the cursor-token path already uses, because a per-process hash seed would make every
  submit fail behind a load balancer.
- Ordering: key order, float representation and datetime serialization do not change the
  fingerprint; a changed value does.
- DST: a concurrent write between preview and submit produces the refusal rather than a
  silently changed commit.

## 7. Docs

A short section next to idempotency, leading with the distinction — **idempotency answers "did I
already send this?", preview binding answers "is this still what I saw?"** — the decorator, and the
`exclude` rule.

## 8. Out of scope

- **Partial re-confirmation** ("three of five lines changed, confirm the rest"). Needs a diff,
  which §5.3 refuses to expose.
- **Preview persistence.** [RFC 0055](0055-scoped-disclosure.md)'s `Snapshot`.
- **Cross-operation binding** (a fingerprint spanning a multi-step wizard). Each step binds its
  own projection; a wizard-wide binding needs a session concept this module does not have.

## 9. Risks

- **A false sense of atomicity.** Binding detects change; it does not prevent it. Two callers can
  both preview, and the second's submit refuses — correct, and not what a "binding" sounds like.
  Mitigation: the docs name it as detection, and point at 0063 for prevention.
- **`exclude` grows until the fingerprint means nothing.** An app that excludes every volatile
  field can end up hashing a constant. Mitigation: the refusal message names the projection, and
  the docs say to exclude rendering details rather than data.
- **Fingerprint stability across framework versions.** A change to canonical JSON invalidates
  every in-flight preview. Mitigation: the prefix is already there (`"sha256:"`), so a future
  canonicalizer takes a new prefix and old previews refuse rather than silently matching.

## 10. Unresolved questions

- **Is `exclude` on the decorator, on the model, or both?** On the model it travels with the DTO;
  on the decorator it stays with the operation. Leaning: the declaration is one object passed to
  both, which is what §6's asymmetry test pins.
- **Does the preview operation get a helper that returns `Reviewed[T]` automatically?** It would
  make the two sides symmetric; it also puts the module in the query path. Implementation
  decides.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | A mismatch **refuses** as `precondition(preview_changed)`. Never merge, never overwrite: the caller confirmed a state that no longer exists. |
| 2 | `LOCKED` | The detail says the preview is stale and **not what changed**. The caller may no longer be authorized to see the new value, so a diff is a disclosure. |
| 3 | `LOCKED` | The check runs **inside the handler's transaction** (`wrap`, not `before`), or the gap it closes reopens one transaction narrower. |
| 4 | `ASSUMED` | Built on `stable_payload_fingerprint` rather than a new hash, and the prefix is kept so a future canonicalizer refuses old previews instead of matching them silently. |
| 5 | `ASSUMED` | The fingerprint is **not signed**. A tampered client copy makes the submit fail, which is the safe direction; signing is the named escape hatch for a two-party preview. |
| 6 | `ASSUMED` | The projector stays the app's function. Only the app knows what the caller was shown. |
| 7 | `LOCKED` | The vocabulary is **`Reviewed` / preview binding**, never "sealed". In this codebase *sealed* means field-encrypted at rest — sealed fields, `ArchiveSealer`, "sealed roots are left untouched" — so a `Sealed[T]` DTO would read as ciphertext. The source proposal's name is recorded here and deliberately not used. |
| 8 | `OPEN` | Where `exclude` is declared (model, decorator, or one shared declaration object), and whether the preview side gets a `Reviewed[T]` helper. |

## 12. Phasing

One PR: `Reviewed`, `ReviewedCommand`, `canonical_fingerprint`, the `@confirms_preview` middleware, the
battery including the cross-process determinism leg, and the docs section. No dependencies;
[RFC 0055](0055-scoped-disclosure.md) consumes it.
