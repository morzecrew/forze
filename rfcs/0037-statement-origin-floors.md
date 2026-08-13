# RFC 0037 — Statement origin: the second axis of the tenancy floor

- **Status:** ✅ Complete — landed as one PR ahead of RFC 0015 P1, which now declares an origin on its routes instead of hard-coding guard 1. Execution answered three questions the design left open and diverged from §3's route list once; both are recorded in §9.
- **Scope:** One orthogonal axis added to the tenancy doctrine — *how the statement text reaching a backend came to exist* — plus a floor table pairing it with `TenantIsolationMode`. Amends [`contracts/tenancy/wiring.py`](../src/forze/application/contracts/tenancy/wiring.py) only: a `StatementOrigin` literal, a floor mapping, and a validator that reuses the existing `TierLattice`. **No new module, no new plane, no port change, no new dependency.** Also resolves a naming collision with RFC 0015's route-level `provenance` field by giving the two axes different names and stating which answers which question.
- **Related:** [`tenancy/wiring.py`](../src/forze/application/contracts/tenancy/wiring.py) — `TenantIsolationMode`, `_ISOLATION_LATTICE`, `isolation_satisfies`, `validate_module_tenancy`; [`contracts/tiers.py`](../src/forze/application/contracts/tiers.py) — `TierLattice.satisfies`/`.validate`, reused verbatim. RFC 0015 §2 (the three author-trust tiers) and §3.2 (the per-route guards this generalizes). RFC 0038 is the first consumer of the `compiled` rung. `GraphRawQueryPort` with `allow_raw_query=False` ([`forze_neo4j`](../src/forze_neo4j/execution/deps/configs/graph.py)) is the shipped instance of the `raw` rung.
- **Origin:** Reviewing a lakehouse / semantic-layer design (a compiler emitting per-request SQL against per-tenant gold marts) against the shipped tenancy doctrine. The ladder's docstring says `dedicated` is *"the only model safe for untrusted raw or self-scoping query paths"*, and compiler-authored SQL is neither raw nor structured. Forced to choose today, an author either over-provisions to `dedicated` — precisely the operational weight such an architecture exists to avoid — or runs a raw-ish path at `namespace` in silent violation of the framework's own written rule. The rung exists in practice and has no name.

---

## 1. The gap: a rung the ladder does not name

[`TenantIsolationMode`](../src/forze/application/contracts/tenancy/wiring.py) documents `none < tagged < namespace < dedicated` and assigns each tier a mechanism. The docstring then makes one claim about *statements* rather than about containers:

> `dedicated` — a separate instance/credentials per tenant (a routed client). The only model safe for **untrusted raw or self-scoping query paths**.

That sentence carries the framework's whole position on query text, and it recognises exactly two kinds: text the framework builds (safe anywhere) and raw text it cannot read (safe only at `dedicated`). A third kind is already in the tree's design surface and fits neither:

- RFC 0015's dynamic-read plane executes statements authored at runtime by a catalog or a compiler.
- RFC 0038 adds statements that arrive with a **declared read set the adapter can check against the parsed text**.

Neither is raw — the author is our own release artifact and the claims are checkable. Neither is structured — the framework did not build the text and cannot place its predicates. The gap is not theoretical: RFC 0015 §3.2 already hard-codes a floor for it (guard 1: *a tenant-aware dynamic-read route on the tagged tier is refused outright*), as a per-route check in one integration's config validator. That is the right floor discovered in the right place and written in the wrong one — the next plane to face the question re-derives it, and the two can drift.

## 2. Two axes, two names — the collision this RFC also fixes

RFC 0015 ships `provenance: Literal["trusted", "untrusted"]` as a **mandatory route-config field with no default**, answering *how much do we trust the author*. That is a threat-model axis: it drives whether a confining role or a routed client is required.

The axis this RFC adds answers a different question — *what kind of process produced the text* — and drives the isolation floor. Naming both "provenance" would put two unrelated meanings behind one word in adjacent config, which is a trap someone falls into exactly once and expensively.

So: **`StatementOrigin` for text origin, `provenance` stays as author trust.** They compose rather than compete, and the composition is the point:

| | `structured` | `compiled` | `raw` |
|---|---|---|---|
| **trusted author** | analytics named query | RFC 0038 compiled plan | `GraphRawQueryPort` on a reviewed template |
| **untrusted author** | (impossible — the framework built it) | LLM emitting into a declared surface | end-user SQL console |

The effective floor is the **strongest** of: the origin floor (this RFC), the module's declared `required_isolation`, and whatever the trust tier demands (RFC 0015's `untrusted` ⇒ role or routed client). Nothing here weakens an existing requirement; a second floor can only raise.

## 3. Design

All of it lands beside `_ISOLATION_LATTICE`, because it is read at the same moment, by the same validator, against the same lattice.

```python
StatementOrigin = Literal["structured", "compiled", "raw"]
"""Who authored the statement text reaching a backend — which, with the isolation tier,
decides whether a route is safe.

- ``structured`` — the framework builds the statement from typed spec elements (document,
  search, analytics named queries, procedures). The adapter places every predicate itself.
  Safe at any tier, which is why every existing plane is this and says nothing.
- ``compiled`` — the text is generated per request by a trusted compiler that declares
  what it reads, and the adapter checks that declaration against the statement before
  executing it. Stronger than ``raw`` because the claim is checkable; weaker than
  ``structured`` because the text is still not ours. Floor: ``namespace`` (§4).
- ``raw`` — an engine-specific string the framework can neither rewrite nor verify
  (``GraphRawQueryPort``). Unchanged doctrine: ``dedicated``.
"""

_ORIGIN_FLOORS: Final[dict[StatementOrigin, TenantIsolationMode]] = {
    "structured": "none",
    "compiled": "namespace",
    "raw": "dedicated",
}

if frozenset(get_args(StatementOrigin)) != _ORIGIN_FLOORS.keys():
    raise exc.internal(  # at import — a rung without a floor must never reach a wiring
        "StatementOrigin and _ORIGIN_FLOORS disagree: every origin needs a floor, and "
        "the missing one would otherwise default to whatever a KeyError does at call time.",
        code="origin_floors_incomplete",
    )


def required_isolation_for_origin(origin: StatementOrigin) -> TenantIsolationMode:
    """The weakest isolation tier at which *origin* is safe."""


def validate_origin_isolation(
    *,
    origin: StatementOrigin,
    derived: TenantIsolationMode,
    route: str,
    integration: str,
) -> None:
    """Raise ``exc.configuration`` when a route runs an origin its isolation cannot carry."""
```

`validate_origin_isolation` delegates the comparison to `isolation_satisfies` — it does not re-implement the ordering, and it does not introduce a second lattice. `TierLattice.validate` already produces the remediation sentence (*"Strengthen the wiring … or lower the declared requirement"*); the origin validator adds one clause naming the origin, so the message says which of the two floors bit.

**The completeness guard is not decoration.** A `dict[StatementOrigin, …]` literal missing a key is *not* a type error — mypy does not check dict-literal completeness against a `Literal` key type — so without the guard a fourth rung added without a floor would sail through every gate and surface as a `KeyError` at call time, which is precisely the failure mode §3's next paragraph forbids. The guard derives the key set from the literal via `get_args`, the shipped pattern in [`querying/guards.py`](../src/forze/application/contracts/querying/guards.py) (`_ELEMENT_QUANTIFIER_KEYS`). It **raises rather than asserts**: `assert` is stripped under `-O`, so an assertion here would silently stop protecting exactly the deployments that run optimized.

**Failure is at freeze, never at call time.** This is the `validate_module_tenancy` moment, alongside `required_isolation`. A route whose origin outruns its isolation must be unwireable, not merely observable — the CQRS read-only-guard defect on record (a guard that fires at call time rather than resolve time) is the evidence for why, not a preference.

**`structured` is the default and stays invisible.** No existing spec grows a field, no existing wiring changes, and no plane declares `structured` explicitly: absence means structured, because every shipped plane *is* structured and a migration that touches every integration to say "unchanged" is cost with no signal. Only a plane that generates or passes through text declares an origin — which today means the dynamic-read family and the graph raw hatch.

## 4. Why `compiled` sits at `namespace`, and why verification does not move it

`tagged` is the tempting answer, and it is wrong for a reason worth writing down because it will be re-proposed.

The lattice's own docstring rules `tagged` out by describing it: *"shared resource, tenant marker embedded that operations must filter on … Per-tenant table partitioning is this tier too — a forgotten predicate still scans every partition, so the guarantee is the same as a plain discriminator."*

For compiled SQL the predicate is injected by a compiler and asserted by the adapter — so the obvious argument is that verification earns the weaker tier back. It does not, and the distinction is the load-bearing one in this RFC:

**Verification raises confidence in a claim; it does not create a boundary.** The assertion is an AST check against generated text. A compiler bug, a dialect the parser renders imprecisely, or a construct the parser folds away all produce the same outcome: a check that passes over a statement that reads something it should not. At `tagged`, that outcome is a **silent cross-tenant read in a correctly-rendered dashboard** — the framework's canonical never-again failure (the Mongo history `$exists:false` leak, and RFC 0015 §2.1's central argument). At `namespace`, the identical bug either fails loudly with an undefined relation or stays inside the tenant's container. Same defect, harmless outcome.

So the floor is set by what happens **when the check is wrong**, not by how good the check is. That is the same reasoning RFC 0015 §2.1 used to refuse `tagged` for unverifiable statements, and it survives the statement becoming partly verifiable — because partly is not a boundary.

The consequence is architectural and belongs in any consumer's own docs: **`compiled` at `namespace` means per-tenant containers**, which makes whatever names those containers a security-relevant component rather than a readability one. A compiler that bakes namespace names into its artifacts is baking in an isolation boundary.

## 5. What this does not do

- **Not a capability model.** It says nothing about what an engine can enforce; that is `DynamicReadCapabilities` (RFC 0016). Origin is about the text, capabilities are about the server.
- **Not a per-statement check.** The origin is a wiring fact declared once per route and validated at freeze. Nothing inspects a statement to infer its origin — that would be the parser RFC 0015 decision 3 forecloses, pointed at the wrong question.
- **Not a relaxation.** No route that is legal today becomes illegal *by default*, because `structured` is the unspoken default and floors at `none`. The only routes this can refuse are ones that opt into a non-structured origin.
- **Not a fourth rung.** `compiled` is deliberately the only addition. "Reviewed-template LLM output" and similar shadings are trust-axis questions, answered by RFC 0015's `provenance`, not by inventing origins.

## 6. Acceptance battery

1. `required_isolation_for_origin` returns the floor for each of the three origins, and the §3 completeness guard makes a rung-without-a-floor an **import** failure rather than a call-time `KeyError` — asserted by reloading the module with a floor removed and observing the raise, since a dict literal's incompleteness is invisible to mypy and would otherwise be caught by nothing. *(unit)*
2. A `compiled` route deriving `tagged` fails `validate_module_tenancy` with an actionable message naming the route, the origin, and the tier it needs. *(unit)*
3. A `compiled` route deriving `namespace` and one deriving `dedicated` both pass. *(unit)*
4. A `raw` route deriving `namespace` fails; at `dedicated` it passes — the shipped `GraphRawQueryPort` doctrine, now expressed as data rather than as prose. *(unit)*
5. Composition: a module declaring `required_isolation="dedicated"` with a `compiled` route at `namespace` still fails on the *declared* floor — the stronger of the two wins, and the message says which. *(unit)*
6. A route that declares no origin behaves exactly as today: no new validation runs, no message changes. Pinned so the "structured is invisible" claim is a test rather than an intention. *(unit)*
7. `validate_origin_isolation` produces its verdict through `isolation_satisfies` — asserted by monkeypatching the lattice and observing the call, so a future refactor cannot quietly fork the ordering. *(unit)*

## 7. Phases

One PR. The literal, the floor map, the two functions, the wiring into `validate_module_tenancy`, the battery, and the tenancy-matrix docs row. RFC 0015 P1 then declares `compiled`/`structured` on its routes instead of hard-coding guard 1, and RFC 0038 inherits the floor rather than restating it.

## 8. Decision log

| # | Decision | State |
| --- | --- | --- |
| 1 | Origin is a **second axis**, orthogonal to author trust; the effective floor is the strongest of origin, declared `required_isolation`, and the trust tier's demand. A second floor can only raise — no existing route becomes legal that was not | locked |
| 2 | Named `StatementOrigin`, not `QueryProvenance`: RFC 0015 already ships `provenance` for author trust, and two meanings behind one word in adjacent config is a trap. Consequence: any consumer declaring both must name them distinctly in its own config too | locked |
| 3 | `compiled` floors at `namespace`, not `tagged`. Verification raises confidence in a claim, it does not create a boundary — the floor is chosen by what happens **when the check is wrong**, which at `tagged` is a silent cross-tenant read. Consequence: consumers run per-tenant containers, making container naming security-relevant | locked |
| 4 | `raw` stays `dedicated` and `structured` stays `none` — this RFC names the existing doctrine as data, it does not revise it | locked |
| 5 | `structured` is the unspoken default; no shipped spec or wiring changes. A migration that touches every integration to declare "unchanged" is cost without signal | locked |
| 6 | Enforcement is at freeze, in `validate_module_tenancy`, never at call time — the recorded call-time-guard defect is the evidence | locked |
| 7 | Reuses `_ISOLATION_LATTICE` / `isolation_satisfies`; no second ordering is introduced. Battery item 7 pins this against a future fork | locked |
| 8 | Exactly three rungs. Finer shadings of author trust belong on RFC 0015's `provenance` axis; adding origins to express trust would collapse the two axes this RFC exists to separate | locked |
| 9 | No statement is ever inspected to *infer* an origin — that would be RFC 0015 decision 3's foreclosed parser aimed at the wrong question | locked |

## 9. Execution notes

Four things the design did not settle, decided while building it.

| # | Question | Answer | Why |
| --- | --- | --- | --- |
| E1 | **Divergence from §3's route list.** §3 says an origin is declared today by "the dynamic-read family *and the graph raw hatch*" — but wiring `allow_raw_query=True` to `origin="raw"` would refuse a Neo4j graph route that wires today on a non-routed client | The graph hatch was **not** wired. `StatementOrigin` ships as vocabulary plus enforcement; no shipped config declares an origin | Decision 5 (*locked*) says no shipped wiring changes, and §5 says no route legal today becomes illegal by default. §3's list contradicts both. Wiring the hatch is a real behavior change for existing deployments and needs its own change with a migration note — not a line in a vocabulary PR |
| E2 | `validate_origin_isolation`'s §3 signature carries no `code`, but every floor beside it takes a per-integration `validation_failed_code` | One fixed code, `ORIGIN_ISOLATION_FLOOR_CODE = "statement_origin_isolation_floor"`, exported from `contracts.tenancy` | The origin floor is a framework-wide rule with one remediation. An operator grepping for it is asking "where am I running unbuilt text without a container", which is not an integration-scoped question — and a per-integration code would make the fleet-wide search impossible |
| E3 | Order when **both** floors are unmet | The origin floor is reported first | A deployment can lower its own `required_isolation`; it cannot lower an origin's floor. Reporting the declared floor first would hand back a remediation that resolves nothing — the route fails again on the next boot. Pinned by a test, because the order is a diagnostic choice and not an accident of statement order |
| E4 | Reaching the floor needs each route's derived tier, which `validate_routed_client_tenancy_wiring` computed inline inside the `required_isolation` branch | Extracted `_route_isolation_mode(route, *, client_is_routed)` and used it for both floors | A second copy would have been the one place the two axes could disagree about what a route reaches — and the disagreement would surface as a route one floor allows and the other refuses, with nothing naming the cause |

One addition the self-audit produced: §3's guard keeps the *table* honest, but the origin itself arrives from a wiring-supplied callable (`TenancyRouteGroup.origin`) that nothing checks at runtime, so a typo reached `_ORIGIN_FLOORS[origin]` as a bare `KeyError` raised from inside a tenancy validator — naming neither the route nor the fix, for a value that decides an isolation floor. `required_isolation_for_origin` now refuses it as `statement_origin_unknown`. The guard also checks §3's `!=` in both directions rather than only for missing floors: a *stale* floor is harmless at runtime, which is why it survives, and it leaves a table asserting a tier for a rung nothing can declare.

The battery is `tests/unit/test_forze/application/contracts/tenancy/test_statement_origin.py`. Two additions beyond §6's seven items: the origin × tier grid is walked exhaustively (twelve cells) rather than sampled, so a floor table that drifted one rung would not survive; and the import-failure test is paired with a control that executes the unmodified source through the same fixture, without which it could pass because the probe is broken rather than because the guard fired.
