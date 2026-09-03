# RFC 0043 — Capability-gated values and the pinned-read scope

- **Status:** 📝 Draft — design locked at the level of §11's grades, **demand-gated**:
  neither mechanism is built until a first in-tree consumer names itself (§10 Q1). This
  document exists because the design has a known ceiling and a known misreading, and both
  belong on paper before any code does.
- **Scope:** Two small, independent mechanisms. A **capability gate** in a new
  `forze.base.gating` module: a frozen pydantic value whose every construction path runs
  through a mint function scoped to its owning package, so a value that must carry
  provenance cannot be assembled elsewhere — with the `model_construct` bypass documented
  as a ceiling, not fought. A **pinned-read scope** beside
  `src/forze_kits/scopes/dlock.py`: something resolved once per scope and threaded
  implicitly, so within the scope a caller can neither read unpinned nor introduce a
  second pin. No contract changes; no existing module is edited beyond exports.
- **Related:** `src/forze/base/primitives/` (the tier the gate joins),
  `src/forze/base/validators.py`, `src/forze_kits/scopes/dlock.py` (the scope shape the
  facade parallels), `src/forze/application/execution/operations/planning/scopes.py`,
  `src/forze/base/serialization/pydantic.py` (the codec whose paths the gate must
  survive).
- **Origin:** Generalized from `allostra/backend-mvp`'s knowledge plane (`_GateToken` /
  `DecisionFact` / `open_decision_reads`) via the engine-extraction analysis, item 07.

---

## 1. Summary

Two provenance mechanisms, shipped only when something in-tree needs them. The gate makes
"where did this value come from" a property of the type: constructing it anywhere but its
owning package's mint function raises, on every pydantic construction path at once. The
pinned-read scope makes "every read through this facade saw the same pin" a property of
the call shape: the pin is resolved once per scope, the facade's methods take no pin
argument, and a second pin cannot be introduced *within the scope* because there is
nowhere to pass it. Whether a unit of work may open a second scope is a policy this
document delegates rather than claims away (§11 row 9).

## 2. Motivation

Both mechanisms close the same class of gap: an invariant that today lives in review
("only the resolver mints decision values", "always read against the pin you resolved at
entry") and is therefore violated by whoever did not read the review comment.

The concrete failure the gate prevents: a value type that carries provenance fields — the
policy that authorized it, the snapshot it was computed against — can be constructed by
hand with those fields forged or stale, and nothing distinguishes it from the real thing.
A convention ("only construct this in module X") is exactly what a refactor silently
breaks. The product this generalizes from needed the guarantee for authorization facts;
in forze the same shape recurs wherever a framework value must prove its origin — a
verified-import receipt, an attested quiesce report handed across process boundaries, a
grant snapshot.

The concrete failure the scope prevents: a request that resolves a pin (a snapshot
handle, an as-of timestamp, a release id) at entry and then makes five reads has five
opportunities to forget the pin argument, and the forgotten one reads *latest* — a wrong
answer with no error. `DistributedLockScope` already solves the write-side twin of this
(resolve once, thread implicitly, fail loudly on loss); reads that must be
point-consistent have no equivalent.

## 3. Current state

Verified against the tree at `4443635`:

- `forze.base` has no gating or minting concept. `src/forze/base/validators.py` holds
  field-combination validators; `src/forze/base/primitives/` holds value primitives
  (fingerprint, entropy, deadline, cell). Nothing constrains *who* constructs a model.
- `src/forze_kits/scopes/` contains exactly one scope, `DistributedLockScope`
  (`dlock.py`) — acquire once, heartbeat implicitly, raise on loss. Its docstring is the
  behavioural template the pinned-read scope mirrors on the read side.
- `model_construct` appears four times in `src/`: two runtime call sites — the
  outbound-HTTP empty-body path (`forze_http/adapters/http_service.py:120`) and its mock
  twin (`forze_mock/adapters/http.py:144`) — plus the config docstring that names that
  path (`contracts/http/specs.py:66`) and a comment in
  `base/serialization/pydantic.py:108` explaining why the codec has no skip-validation
  fast path. None touches a would-be gated type today; the two live call sites confirm
  the bypass is a real, used pydantic surface — which is why §5.1 documents it rather
  than pretending to close it, and why a gate rollout must leave those paths alone.
- No in-tree read surface currently takes a "pin" the scope could resolve: search
  snapshots (`SearchResultSnapshotPort`) come closest but already thread their handle
  explicitly per call. This is why the RFC is demand-gated rather than executed.

## 4. Goals / Non-goals

**Goals**

- A generic gate any framework package can apply to one of its value types in ~ten
  lines, with the guard holding on `__init__`, `model_validate`, `model_validate_json`,
  and `model_copy(update=...)` simultaneously.
- A read-side scope with the same ergonomics as `DistributedLockScope`: enter once,
  everything inside is consistent by construction; post-exit lifetime and any cleanup a
  resource-owning pin needs are settled by §11 row 8 before first use.
- Honest documentation of what the gate is: provenance hygiene that turns forgery into a
  greppable act, **not** a security boundary.

**Non-goals**

- Not a security mechanism. Python offers no in-process capability enforcement;
  `model_construct` bypasses validation by definition and stays open (§5.1). Anything
  needing an actual trust boundary needs a signature, not a type.
- Not a general effect system or context-threading framework — the scope resolves one
  pin for one unit of work; composing pins is the application's problem.
- Not applied to any existing forze type in this RFC. Retrofitting the gate onto, say,
  `QuiesceReport` is its own change with its own compatibility questions.

## 5. Design

### 5.1 The capability gate (`forze.base.gating`)

```python
gate = MintGate(owner="forze_kits.integrations.portability")  # module-private constant

class VerifiedReceipt(GatedModel):
    __mint_gate__: ClassVar[MintGate] = gate
    archive_path: str
    registry_fingerprint: str

receipt = gate.mint(VerifiedReceipt, archive_path=..., registry_fingerprint=...)
```

`GatedModel` is a frozen `BaseModel` whose `model_validator(mode="before")` refuses
construction unless the gate is *open* — and the gate is open only inside `mint()`,
which sets a token on a `ContextVar` scoped to that gate instance and clears it in a
`finally`. `mode="before"` is load-bearing: pydantic has several construction paths and
a guard on `__init__` alone is a guard on none — the before-validator is the one hook
they all share. `model_copy(update=...)` skips validators entirely, so `GatedModel`
overrides it to refuse a non-empty `update` — copying a gated value is fine, swapping
its fields while keeping the provenance attached is the exact forgery the gate exists to
stop.

**The ceiling, stated rather than fought:** `model_construct` bypasses the gate and
cannot be closed, because it bypasses validation by definition. That converts forgery
from an accident into a greppable act — `grep model_construct` finds every deliberate
bypass — and that is the honest claim the docs make. A `ContextVar` token also means a
callback *invoked synchronously inside* `mint()` could construct extra instances; mint
functions therefore do nothing but validate-and-return (a rule the gate's docstring
carries, and its tests pin).

**Rejected alternative — token-as-field** (the product's shape: a private sentinel
passed as a model field). It works, but the sentinel leaks into `model_dump()` handling,
every schema export, and every serializer edge; the `ContextVar` keeps the payload
schema clean and survives `model_validate_json`, which cannot carry a Python sentinel at
all.

### 5.2 The pinned-read scope (`forze_kits/scopes/pinned.py`)

```python
@asynccontextmanager
async def pinned(resolve: Callable[[], Awaitable[P]],
                 build: Callable[[P], F]) -> AsyncIterator[F]:
    pin = await resolve()          # exactly once per scope
    yield build(pin)               # facade methods close over the pin; no pin parameter
```

The shape is deliberately thin: `resolve` produces the pin (a snapshot handle, an as-of
value, a release row) and `build` returns the application's facade closed over it. The
guarantee is structural — the facade's methods have no pin parameter, so a caller cannot
pass a second pin, and the only way to read unpinned is to not use the facade, which is
greppable the same way `model_construct` is. Whether the facade is a Protocol the
application implements or a generated wrapper over a document query port is delegated to
implementation (§11 row 7): the first consumer's shape should decide it, not this
document.

The sketch treats a pin as a plain value (a handle, a timestamp, a row) with nothing to
release; a pin that *owns* a resource — an open snapshot the backend must drop — needs a
close protocol the sketch deliberately does not invent. Both halves of the post-exit
question (does the facade refuse use after exit; does the pin need releasing) are one
decision, §11 row 8, and neither mechanism ships until its consumer answers it.

Pairing the two mechanisms is the intended idiom: the facade's read results are the
natural gated values, minted by the facade's owning module, so "this value came from a
pinned read" becomes checkable at the type.

## 6. Tests

- Gate: a battery over every construction path — `__init__`, `model_validate`,
  `model_validate_json`, `model_copy` with and without `update`, and `model_construct`
  (asserting it *does* bypass, so the ceiling claim stays true in CI). Concurrency leg:
  two tasks minting through one gate must not observe each other's open window
  (`ContextVar` isolation). Anti-vacuity per RFC 0040's house rule: the battery asserts
  the refusal *kind*, not bare `raises`.
- Scope: `resolve` called exactly once per scope; a facade built in one scope refuses
  use after exit only if the first consumer needs it (delegated, §11 row 8).

## 7. Docs

One page under `pages/docs/in-depth/`, written with the threat-model caveat in the first
paragraph: the gate is provenance hygiene, not a security boundary, and the
`model_construct` ceiling is stated verbatim. The misreading this defends against —
"forze has capability security" — is named in §9.

## 8. Out of scope

- Retrofitting the gate onto existing types (`QuiesceReport`, import receipts): each is
  its own change; this RFC only makes the mechanism available.
- A sync variant of the pinned scope; the first consumer is expected to be async, and a
  sync twin is mechanical if one appears.
- Cross-process provenance (signing minted values): the escape hatch when an actual
  trust boundary appears is `forze.base.crypto`, not more type machinery.

## 9. Risks

- **Read as security theater.** The gate looks like enforcement and is hygiene. Every
  doc surface leads with the ceiling; the test suite pins the bypass as *working*, so
  the claim cannot drift into overstatement. Accepted residual: someone will still
  over-trust it — the greppability of `model_construct` is the answer we can offer.
- **Zero consumers at ship time.** Building either mechanism speculatively contradicts
  the repo's own YAGNI posture — hence demand-gating (§10 Q1). The risk of *writing the
  RFC* is nil; the risk being avoided is shipping a museum piece.
- **ContextVar gates and exotic execution.** Code that constructs models on another
  thread from inside a mint callback would see a closed gate. Accepted: mint functions
  are documented and tested as validate-and-return.

## 10. Unresolved questions

- **Q1 (gates execution):** which in-tree consumer goes first? Candidates, in likelihood
  order: a portability import receipt (`forze_kits.integrations.portability`), an attested
  `QuiesceReport` handed between processes, a grant snapshot in `forze_identity`.
  Settled by whichever lands a need first; until then this RFC stays Draft.
- **Q2:** does the pinned facade generalize over document query ports (a generated
  wrapper) or stay a per-application Protocol? Settled by the first consumer's shape
  (§11 row 7 delegates it).

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The gate lives in `forze.base` (new `gating` module), not in `forze_kits` — it is a base primitive any framework package may apply, and kits must stay importable-optional. Changing this later means moving a public symbol. |
| 2 | `LOCKED` | The guard runs in `model_validator(mode="before")` so every pydantic construction path shares one gate; `model_copy(update=...)` is refused separately. A guard on `__init__` alone is rejected as a guard on none. |
| 3 | `LOCKED` | `model_construct` stays open and is documented as the ceiling, with a test asserting the bypass works. Fighting it (patching, metaclass tricks) is rejected: it would claim a security property Python cannot deliver, and §9's misreading risk becomes a lie. |
| 4 | `LOCKED` | Neither mechanism is built until an in-tree consumer names itself (§10 Q1). This RFC is the design note item 07 called for, not a build order. |
| 5 | `ASSUMED` | The mint window is a per-gate `ContextVar` token, not a token-as-field (product's shape) — cleaner payloads, survives `model_validate_json`. If execution finds a pydantic path the ContextVar cannot cover, the token-as-field fallback is the recorded alternative. |
| 6 | `ASSUMED` | The pinned-read scope lives in `forze_kits/scopes/pinned.py` mirroring `DistributedLockScope`'s ergonomics; a `resolve`/`build` pair is enough surface. |
| 7 | `OPEN` | Facade shape: Protocol implemented per application vs. generated wrapper over a query port. The first consumer decides and logs the choice with its rationale. |
| 8 | `OPEN` | Post-exit lifetime, both halves: whether a facade refuses use after its scope exits (a closed-over "stale" flag vs a plain closure), and whether `resolve` may own a resource needing a close hook on exit. Decide against the first consumer's failure mode: if a leaked facade can read a torn-down pin, add the flag; if the pin holds a backend resource, add the hook. |
| 9 | `OPEN` | Second-scope policy per unit of work: refuse nesting with a ContextVar guard, or permit deliberate multi-pin reads. The structural guarantee is scope-wide (§5.2); this row is what would extend it request-wide. The first consumer decides and logs the choice. |

## 12. Phasing

- **P0 (this document):** design locked to the grades above. Nothing ships.
- **P1 (demand-gated):** the gate, with its construction-path battery, when Q1 names a
  consumer.
- **P2 (demand-gated, independent):** the pinned-read scope, when a pin-shaped read
  surface exists; P1 is not a prerequisite.
