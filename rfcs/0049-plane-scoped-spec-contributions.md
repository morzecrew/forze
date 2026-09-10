# RFC 0049 — Plane-scoped identity spec contributions

- **Status:** 📝 Draft — design locked, one PR. The coupling refusal in §5.2 is the only part worth arguing about.
- **Scope:** Give `forze_identity.spec_contributions()` a plane selection, so an application that wires authn alone can catalogue authn alone and still pass runtime reconciliation. Touches `src/forze_identity/inventory.py` and its tests; **no contract change** to `SpecRegistry`, no change to `build_runtime`'s reconciliation, and no change to what any plane's documents *are*. Deliberately does not weaken the cross-plane dependency the current all-or-nothing shape protects — §5.2 is how.
- **Related:** `src/forze_identity/inventory.py:170` (`spec_contributions`, and the docstring arguing for the current shape), `src/forze/application/execution/assemble.py:165` (reconciliation refuses a spec catalogued but never bound) and `:168` (`allow_unregistered` covers only the opposite direction), `identity_document_names(specs=…)` in the same module (the shipped selection helper whose empty-selection refusal this RFC mirrors).
- **Origin:** A production ERP backend migrating to 0.7.0. It wires authn and has no authz or tenancy plane; merging `spec_contributions()` catalogued twelve documents nothing binds, and assembly failed. The workaround was to skip the helper and hand-register the authn specs whose documents appear in the wiring.

---

## 1. Summary

`forze_identity.spec_contributions()` returns every document spec of all three identity
planes — authn, authz and tenancy — and takes no arguments. An application that wires
only authn therefore catalogues twelve documents it never binds, and
`build_runtime(specs=…)` refuses exactly that: a spec catalogued but never bound fails
assembly.

The escape hatch that exists for the mirror-image problem does not help. `allow_unregistered`
downgrades **bound but not catalogued** to a warning; this is **catalogued but not
bound**, and there is no flag for it — correctly, because that direction is usually a
real wiring mistake.

So the only way to use the reconciliation feature at all is to not use the helper. This
RFC gives the helper a plane selection, and keeps the reason it was all-or-nothing by
refusing the one selection that would break it.

## 2. Motivation

The reconciliation check is one of the better things `build_runtime` does: it catches a
spec that was catalogued and forgotten, and a route bound and never declared. An
application gets it by passing `specs=`.

For an application with a partial identity plane, the framework's own contribution helper
makes that check unusable. This is worth separating from an ordinary papercut: the
failure is not that something is harder than it should be, it is that **using two shipped
features together fails**, and the resolution an application reaches for is to stop using
one of them. That is the shape of defect that quietly reduces a feature's adoption to
zero without ever being reported.

The measured case: one application, twelve documents, assembly refused. The workaround
cost is small — a hand-written registration — and its real cost is that the application
now maintains a list the framework was maintaining for it, which will drift on the next
identity release.

## 3. Current state

```python
def spec_contributions() -> SpecRegistry:
    return SpecRegistry().register(
        *AUTHN_SPECS,
        *AUTHZ_SPECS,
        *TENANCY_SPECS,
        source=SpecSource.FRAMEWORK,
        identity=True,
    )
```

No parameters. The docstring states the rationale, and it is a real argument rather than
an oversight:

> All three planes come together deliberately: authn's dependencies already reach across
> into authz (its principal-eligibility check reads `authz_policy_principals`), so a
> per-subpackage helper would leak that coupling onto the app.

Two more verified facts shape the design:

- **`AuthnSpec` / `AuthzSpec` are not in the registry.** They are policy, carry no rows,
  and take an app-chosen route name. So a plane selection selects *documents*, and the
  policy specs stay outside it — there is nothing to filter there.
- **A selection helper already exists.** `identity_document_names(specs=…)` validates a
  selection of identity documents by name or spec, refuses an unknown name, and — notably
  — **refuses an empty selection** rather than treating it as success: "An empty selection
  binds nothing and would read as success; an emptied group constant or a bad
  comprehension should fail here, at the seam." That refusal is the precedent §5.1 copies.

## 4. Goals / Non-goals

**Goals**

- An application wiring one plane can catalogue one plane, through the helper, and pass
  reconciliation.
- The cross-plane dependency in the docstring cannot be broken silently by a selection.
- The change is additive: `spec_contributions()` with no arguments keeps returning all
  three planes.

**Non-goals**

- **Not a change to reconciliation.** `build_runtime` keeps refusing catalogued-but-unbound.
  Making the check laxer would trade a good error for a bad silence, and every other
  application would pay for this one's shape.
- **Not a `allow_uncatalogued` flag.** The mirror of `allow_unregistered` would let any
  catalogue drift pass, which is the check's whole purpose.
- **Not a re-litigation of the three-plane coupling.** The coupling is real; this RFC
  routes around the *catalogue* consequence of it, not the dependency itself.

## 5. Design

### 5.1 The selection

```python
IdentityPlane = Literal["authn", "authz", "tenancy"]

def spec_contributions(*, planes: Sequence[IdentityPlane] | None = None) -> SpecRegistry:
    """Every document spec the named identity planes bind (all three by default)."""
```

- `None` (the default) is all three planes — today's behaviour, unchanged, so no caller
  moves.
- An **empty sequence is refused**, not treated as "nothing": `planes=[]` is an emptied
  constant or a bad comprehension, and the same argument
  `identity_document_names` already makes applies verbatim. Reusing that reasoning is
  deliberate — two seams in one module disagreeing about whether empty means nothing
  would be worse than either answer.
- An unknown plane name is refused, naming the three.

### 5.2 The coupling refusal

This is the part that earns the RFC. The current shape protects a real dependency: authn's
principal-eligibility check reads `authz_policy_principals`. A plane selection is
therefore a way to catalogue authn while leaving that document uncatalogued — and if the
application has the eligibility check switched on, its authn plane binds a document it
never declared, which is the *other* reconciliation failure and a genuinely confusing one.

So `planes=["authn"]` is not unconditionally legal. Two candidate rules:

- **Refuse the selection when the check is on.** Requires the helper to see the
  `AuthnSpec`'s eligibility setting, which it does not today — the helper takes no spec.
- **Let reconciliation say it.** Bind the document, fail the check, and make the error
  name the plane selection as the likely cause.

The second is preferred: it needs no new coupling between the helper and the policy spec,
it fires only when the dependency is actually exercised, and the message can be exact.
The origin application is the evidence that this is the common case rather than the rare
one — it runs with `eligibility="allow_all"`, precisely *because* it has no authz plane,
so its authn plane reads no authz document and `planes=["authn"]` is sound for it.

The one thing this must not do is stay silent. An application that selects `["authn"]`,
leaves the eligibility check on, and gets a bound-but-uncatalogued failure with no hint
about the selection has been handed a worse error than the one this RFC removes.

### Alternatives considered

**A — per-subpackage helpers** (`forze_identity.authn.spec_contributions()`, …). The
shape the docstring rejects, and the rejection holds: three helpers make the cross-plane
dependency invisible at exactly the call site that needs to know about it, and an
application would compose them without ever meeting the coupling. A single helper with a
selection keeps one place to document it.

**B — reconcile only the planes the deps registry binds.** Have `build_runtime` drop
catalogued entries whose plane binds nothing. Rejected: it silently repairs a catalogue
mistake for every application in order to serve one, and "catalogued but nothing binds
it" is a defect far more often than it is a partial plane. The trade-off is that it would
need no application change at all, which is genuinely attractive — and not worth the
check it dissolves.

**C — leave it; document the hand-registration.** Zero code, and the workaround already
works. Rejected because the framework then ships a list that applications must maintain
by hand and re-check on every identity release; the drift is silent and lands as a
reconciliation failure at the worst moment.

## 6. Tests

In `tests/unit/test_forze_identity/`, beside the existing `identity_document_names` cases:

- Default (`None`) returns the same registry as today — pinned by comparing the entry set,
  so a future plane addition is caught rather than assumed.
- Each single-plane selection returns exactly that plane's documents and nothing else.
- `planes=[]` is refused; an unknown plane name is refused and names the three.
- **The test that carries the motivation**: a runtime assembled with an authn-only deps
  module and `specs=[spec_contributions(planes=["authn"])]` reconciles. It fails against
  `main`, which is the point.
- The §5.2 case: an authn plane with the eligibility check on plus `planes=["authn"]`
  produces the bound-but-uncatalogued failure, and the message names the selection.

## 7. Docs

- `pages/docs/identity-tenancy-enc/identity.md` — the selection, and one sentence on when
  a partial plane is the right shape.
- The reconciliation paragraph wherever `build_runtime(specs=…)` is documented should say
  that a framework contribution helper may need narrowing; a reader hitting this failure
  currently has nothing to search for.
- The published skill's `authn` reference gains the selection in its wiring block.

## 8. Out of scope

- **`AuthnSpec` / `AuthzSpec` in the inventory.** They carry no rows (§3); nothing to
  catalogue. Named because a reader may reasonably expect a "plane selection" to cover them.
- **The same treatment for other contribution helpers** (`AggregateKit.spec_contributions`,
  `RealtimeTransport.spec_contributions`, `progress_spec_contributions`). None has shown
  the problem: they are per-instance and already scoped to what the application declared.
  What would change it: a helper that bundles planes an application can wire separately.
- **A tenancy-only or authz-only application.** Legal under §5.1 and untested beyond the
  unit case, because no such application has been seen.

## 9. Risks

- **A plane selection becomes a way to hide a wiring mistake.** An author who selects
  `["authn"]` to make an error go away, while genuinely wiring authz, has papered over a
  real gap. Mitigation: the reconciliation failure still fires from the bound side, and
  §5.2 requires its message to name the selection.
- **The default drifts.** If a fourth identity plane is added and `None` keeps meaning
  "all", every application using the default silently starts cataloguing it. That is
  today's behaviour too, and the §6 default-comparison test at least makes the change
  visible in a diff.

## 10. Unresolved questions

- **What exactly the bound-but-uncatalogued message should say**, given the helper cannot
  know whether a selection was used. Naming the selection as a *possible* cause when it
  may not be one is a hint that is sometimes wrong — acceptable, but the wording needs a
  pass and it is the only user-visible text this RFC adds.
- Whether `planes` should accept the plane's `StrEnum` if one exists elsewhere in
  `forze_identity`, rather than a bare `Literal`. Cosmetic, and worth one grep before
  execution.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The selection lives on **one helper**, not on three per-subpackage helpers. Keeps the cross-plane dependency documented in the single place a caller passes through; consequence is that `forze_identity.authn` never gains a contribution helper of its own, and asking for one later means re-arguing §5's alternative A. |
| 2 | `LOCKED` | `spec_contributions()` with **no arguments is unchanged** — all three planes. Additive only; no existing caller moves. |
| 3 | `LOCKED` | **`build_runtime`'s reconciliation is not touched**, and no `allow_uncatalogued` flag is added. Catalogued-but-unbound stays a hard failure for every application. |
| 4 | `LOCKED` | An **empty selection is refused**, mirroring `identity_document_names`. Two seams in one module must not disagree about whether empty means nothing. |
| 5 | `ASSUMED` | The authn↔authz coupling is enforced **from the reconciliation side** (§5.2's second option), not by the helper inspecting the policy spec. Believed right because it adds no coupling and fires only when the dependency is exercised; execution may depart if the message cannot be made specific enough to be useful, and should log why. |
| 6 | `OPEN` | Whether `planes` takes a `Literal` or an existing `StrEnum`. Execution decides after checking what the package already exports. |

## 12. Phasing

One PR. The helper's parameter, the guardrails, the §6 tests and the §7 docs land
together — there is no half of this that is independently useful, and the reconciliation
test is what proves the change did the thing it exists for.
