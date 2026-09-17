# RFC 0057 — Derived permissions and config-bound grants

- **Status:** 📝 Draft — execution-ready. Adopts two source proposals as one design, because the second rides the first's protocol; [RFC 0055](0055-scoped-disclosure.md) depends on the protocol too.
- **Scope:** A `PermissionProvider` protocol in `forze_identity.authz`: permissions derived per request from document state or from reviewed deployment config, unioned with catalog grants, with a deactivation rule that lets a derived denial mask a binding, plus a `ConfigGrants` provider for break-glass and external-recipient subjects. Touches `forze_identity.authz` (the grant resolver and the policy service) and adds a settings-shaped provider; **`EffectiveGrants` gains one field** (§5.2). No change to the catalog's documents or to the `AuthzRequest` shape.
- **Related:** [`src/forze_identity/authz/services/grants.py:172`](../src/forze_identity/authz/services/grants.py) (`resolve_effective_grants` — the union over principal, role-lineage and group bindings), [`src/forze_identity/authz/services/policy.py:65`](../src/forze_identity/authz/services/policy.py) (`decide`, `principal_active`, the owner-override keys), [`src/forze/application/contracts/authz/value_objects/grants.py:15`](../src/forze/application/contracts/authz/value_objects/grants.py) (`EffectiveGrants` = roles + permissions + `resolved_at`), [`src/forze/base/settings.py`](../src/forze/base/settings.py) (why the root settings class lives in the application), [`src/forze/application/execution/operations/wiring.py:123`](../src/forze/application/execution/operations/wiring.py) (`check_wiring`, where an unknown permission key must fail), [RFC 0059](0059-non-disclosing-denials.md) (the denial a capability check raises).
- **Origin:** A working-time ledger where per-request *capabilities* (its word; this RFC uses **permissions**, §5.1) are computed from document state — a mapped, active employee row yields four `OWN_*` capabilities, roles add bundles, a config allowlist adds one more, an inactive or unmapped row yields nothing even if roles exist — and every route gates on a permission, never on a role. Its own audit names the flaw this RFC has to answer: the config allowlist and the role table are two sources of truth with no reconciliation.

---

## 1. Summary

An app registers providers. Each provider maps a principal and a context to a set of permission
keys — from documents it reads (is this person an active member?) or from a typed settings
section (is this subject on the break-glass list?). The policy service unions them with catalog
grants, and a provider may also **deny**, which outranks a binding. Every permission key is
checked against the catalog at wiring, so a typo is a boot error rather than a permanent denial.

## 2. Motivation

"Active member of X" is the most common authorization fact in a line-of-business app, and it is
**state**, not a binding. Today an app has two options and both are bad:

- **Sync bindings from state.** A row changes, a job writes bindings, and the two drift — the
  window is unbounded and the failure is open (permissions that should be gone).
- **Check state in every handler.** Correct and unenforceable: nothing makes the next handler
  remember, and the check is invisible to the policy service, the introspection surface and the
  tests.

The second half is the same problem with a different source. Break-glass operators, one-off
migration roles and external recipients are better as reviewed deployment config than as rows an
admin can grant at runtime — and forze has no notion of a permission that comes from config, so
the origin application put it in config *and* in the role table, which its audit correctly calls
two sources of truth.

## 3. Current state

**Grants are closed over the catalog.** `resolve_effective_grants(principal_id, scope=...)` unions
principal-permission bindings, role-permission bindings across expanded role lineage, and
group-permission bindings for active member groups, then returns `EffectiveGrants(roles,
permissions, resolved_at)`. Every `PermissionRef` carries a catalog `permission_id`, so **a
permission that is not a catalog row has no representation** — the first design constraint.

**One state fact is already threaded through, and it is the precedent.**
`AuthzPolicyService.decide(grants, request, principal_active=True)` returns a denial for an
inactive principal *before* looking at any grant. So "state beats bindings" is already the
policy service's behaviour for exactly one fact, hardcoded. This RFC generalizes that one
parameter into a protocol.

**Resource-level authorization is one ABAC check.** When the request's resource carries an
`owner_id` attribute it must equal the subject, unless the principal holds `admin` or
`<resource_type>.admin`. There is no hook for "allowed because a document says so", which is what
[RFC 0055](0055-scoped-disclosure.md) needs.

**Settings belong to the application.** `forze.base.settings` is explicit: the root settings
class lives in the app because the env prefix, nesting and extra-key policy are deployment
decisions; `RuntimeSettings` is a plain `BaseModel` mounted as a field. So a config-grants section
is a model the framework offers and the app mounts — not an environment read by the framework.

## 4. Goals / Non-goals

**Goals**

- Derived permissions are part of the decision the policy service makes, not a check beside it.
- A derived **denial** masks a catalog binding, so deactivating a row is sufficient.
- Config-sourced permissions use the same mechanism, so there is one union and one place to
  inspect.
- An unknown permission key fails at wiring. A typo that silently denies forever is the worst
  outcome available.
- Every derived grant is visible to whatever inspects authorization, so "why can this principal
  do that" has one answer.

**Non-goals**

- **Not a policy language.** A provider is a Python callable, not a DSL. Rego-shaped evaluation is
  not approached.
- **Not ReBAC.** Relationship-graph authorization is out; per-object admission for one mechanism
  is [RFC 0055](0055-scoped-disclosure.md)'s narrow hook.
- **Not a cache.** A provider runs per decision. Memoization within one invocation is
  implementation's call; a cross-request cache reintroduces drift, which is the thing this
  replaces.
- **Not a replacement for roles.** Catalog grants stay the default; derivation is for facts that
  live in documents or in config.

## 5. Design

### 5.1 The protocol

```python
@runtime_checkable
class PermissionProvider(Protocol):
    name: str
    async def derive(self, principal: PrincipalRef, ctx: ExecutionContext) -> DerivedPermissions: ...

@attrs.define(frozen=True, slots=True)
class DerivedPermissions:
    granted: frozenset[str] = frozenset()
    denied: frozenset[str] = frozenset()      # outranks every source, including bindings
```

Providers are registered at wiring in declaration order; the result is the union of `granted`
minus the union of `denied`. `name` is what the introspection surface and the audit trail
attribute a grant to.

`PrincipalRef` is the shipped type
([`catalog.py:46`](../src/forze/application/contracts/authz/value_objects/catalog.py)), and it
already carries `is_active` — the same fact `decide` takes as `principal_active`, which is why
§5.3's rule is a generalization rather than a new axis. The principal a provider is called for comes
from the invocation context's shipped identity, `get_authn()` →
`AuthnIdentity(principal_id, actor)`
([`identity.py:11-32`](../src/forze/application/contracts/authn/value_objects/identity.py)), whose
`principal_id` the docstring states "aligns with `PrincipalRef`" — so a delegated call can be
resolved for the subject, the actor, or both, and the rule for which is §10's question rather than a
new channel.

**Permissions, not "capabilities".** The origin application says capabilities; in this codebase a
*capability* is an adapter's declared feature flag (the port capability model, `token_stream`,
capability-gated operators) and the authz vocabulary is `permission_key` / `PermissionRef` /
`freeze_permission_keys`. Borrowing the word would have collided with both.

### 5.2 Where derived grants live

`EffectiveGrants` gains one field:

```python
derived: frozenset[DerivedPermissionRef] = frozenset()   # (key, provider_name) — no catalog id
```

A separate field rather than smuggling synthetic `PermissionRef`s into `permissions`: a
`PermissionRef` promises a catalog row exists, and a derived permission has none (§3). Keeping
them apart is what lets an operator see which grants came from documents or config and which are
administered.

`decide` matches the action against `permissions` **or** `derived`, after checking the deny set.

### 5.3 Deactivation wins

The rule is stated once, in `decide`, and it generalizes the `principal_active` parameter that is
already there: a key in any provider's `denied` set is refused regardless of bindings, roles or
groups. That is what makes "mark the employee inactive" a complete authorization action rather
than a first step that a stale binding can outlive.

### 5.4 Config grants

```python
class ConfigGrants(BaseModel):
    grants: Mapping[str, tuple[str, ...]] = {}     # permission key -> exact subject identifiers
```

Mounted by the app on its own settings root (§3), wrapped by the shipped provider. Rules:

- **Empty means nobody.** No implicit wildcard, no "unset means all".
- **Exact subjects only.** No patterns, no domain matching: a pattern in a break-glass list is
  how a break-glass list becomes an access-control system nobody reviews.
- **Unknown permission key ⇒ wiring failure.** Checked inside `check_wiring` against the catalog,
  so the deployment that renamed a permission finds out at boot.
- **One source of truth, stated as a rule:** a permission key that appears in `ConfigGrants`
  **may not** also be granted through the catalog. The wiring check refuses the overlap. This is
  the origin audit's flaw, answered by refusing the configuration that creates it rather than by
  a precedence rule nobody remembers.

### 5.5 The boundary dependency

`require_permission("ledger.write")` as a FastAPI dependency, raising the denial
[RFC 0059](0059-non-disclosing-denials.md) defines so a missing permission is byte-identical to a
missing object where that matters.

### Alternatives considered

- **Sync bindings from state on a schedule.** The status quo alternative; drift window is
  unbounded and it fails open.
- **Let providers return `PermissionRef`s with synthetic ids.** No contract change, and it makes
  `permission_id` a lie — the introspection surface could no longer distinguish an administered
  grant from a derived one.
- **`ConfigGrants` read from the environment by the framework.** Convenient, and it contradicts
  the settings doctrine in §3 for one feature.
- **Precedence rule instead of refusing config/catalog overlap.** "Config wins" is one sentence
  and it leaves two places to look when a principal has a permission nobody meant to grant.

## 6. Tests

- Union: a provider's `granted` admits an action with no binding; a binding admits with no
  provider; both admit once.
- Deny: a key in `denied` refuses despite a direct binding, a role binding and a group binding —
  all three, because each takes a different path through the resolver.
- Order independence: two providers, registered either way round, produce the same decision.
- `EffectiveGrants` separation: a derived grant never appears in `permissions`, and carries its
  provider name.
- Config: empty grants deny; an exact subject is admitted; an unknown permission key fails
  `check_wiring`; a key present in both `ConfigGrants` and the catalog fails `check_wiring`.
- DST: `no_permission_after_deactivation` — a workload that deactivates a row mid-run, with a
  history invariant asserting no admitted operation for that principal after the deactivating
  write commits. Not a `SystemInvariant`: it ranges over the history, not over a read-set
  reduced to a number ([RFC 0052](0052-versioned-facts-correction-lineage.md) §5.5).
- **Not tested:** a provider's own correctness. That is the app's function; the battery pins the
  union, the deny precedence and the wiring refusals.

## 7. Docs

One section in the authz page: the protocol, the union table (catalog / derived / denied), the
`ConfigGrants` rules, and the sentence that decides how apps use it — **gate on permissions,
never on roles**, which is what makes a derived denial able to close a route.

## 8. Out of scope

- **Provider caching across requests.** Named because a document-reading provider costs a query
  per decision; a cache is the drift this design removes, so it needs its own invalidation
  argument.
- **Per-tenant config grants.** `ConfigGrants` is deployment-wide. A tenant-scoped break-glass
  list is a catalog concern.
- **Attribute-based conditions** ("may read rows in their own cost centre"). Providers return keys,
  not predicates. The resource-level hook stays [RFC 0055](0055-scoped-disclosure.md)'s narrow one.

## 9. Risks

- **A provider that reads documents makes authorization a query.** Latency and a new failure mode
  on every decision: a provider whose read fails must **deny**, not throw past the decision, or an
  outage becomes an authorization bypass. Pinned in decision 4.
- **`ConfigGrants` reads as a backdoor.** It is one, by design, for break-glass. Mitigation: exact
  subjects only, empty-means-nobody, wiring-checked keys, no overlap with the catalog, and visible
  to introspection.
- **Two fields on `EffectiveGrants` means two places to look.** Accepted: the alternative makes
  `permission_id` unreliable, and an operator asking "where did this come from" is exactly the
  question the split answers.

## 10. Unresolved questions

- **On a delegated call, which identity does a provider derive for?** `AuthnIdentity` carries the
  subject and the `actor` chain; deriving for the subject alone grants an agent everything the user
  has, deriving for the actor alone ignores the delegation. Leaning: derive for both and intersect,
  which is the conservative reading and needs a battery rather than a paragraph.
- **Does a provider see the whole `ExecutionContext` or a narrowed view?** The context is powerful
  (every port, the tenant, the invocation); a narrowed view is safer and may not be enough to read
  the documents a provider needs. Leaning: the context, with the docs saying a provider reads and
  never writes.
- **Is provider failure ever fail-open?** Decision 4 says deny. An app with a read-only public
  route may want otherwise, which would be a per-provider declaration.
- **Does the introspection surface ship here or later?** §4 wants derived grants visible; there is
  no authz introspection route in `src/forze_fastapi/routes/` today, so this RFC either adds one
  or defers the goal.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Derived permissions are part of `decide`'s input, not a check beside it. A check beside it is invisible to the policy service, the tests and whatever inspects authorization — which is the status quo this replaces. |
| 2 | `LOCKED` | A provider's `denied` set **outranks every other source**, generalizing the `principal_active` parameter already in `decide`. Deactivating a row must be a complete action, not one a stale binding outlives. |
| 3 | `LOCKED` | Derived grants live in their own `EffectiveGrants` field, never as synthetic `PermissionRef`s. A `PermissionRef` promises a catalog row, and an operator has to be able to tell an administered grant from a derived one. |
| 4 | `LOCKED` | A provider whose read **fails** produces a denial, never an exception past the decision. Otherwise a database outage is an authorization bypass. |
| 5 | `LOCKED` | A permission key in `ConfigGrants` may not also be granted through the catalog; the overlap is a **wiring refusal**. The origin audit's "two sources of truth" is answered by refusing the configuration, not by a precedence rule. |
| 6 | `ASSUMED` | `ConfigGrants` holds **exact subject identifiers**, empty means nobody, and unknown keys fail `check_wiring`. Patterns turn a reviewed break-glass list into an unreviewed access-control system. |
| 7 | `ASSUMED` | `ConfigGrants` is a model the app mounts on its own settings root, not an environment the framework reads — the settings doctrine in `forze.base.settings` holds for this feature too. |
| 8 | `OPEN` | Whether a provider receives the full `ExecutionContext` or a narrowed view, and whether fail-open is ever a per-provider declaration. |
| 9 | `LOCKED` | The vocabulary is **permissions**, matching `permission_key` / `PermissionRef`. *Capability* is taken: it is an adapter's declared feature flag across 100-plus files, and a second meaning inside authz would make "capability-gated" ambiguous in both directions. |
| 10 | `OPEN` | Whether the authz introspection surface (which does not exist today) ships in this RFC or is deferred, given §4 makes visibility a goal. |

## 12. Phasing

- **P1** — the protocol, the `EffectiveGrants` field, the union and deny precedence in `decide`,
  the wiring check for keys, batteries. Unblocks [RFC 0055](0055-scoped-disclosure.md).
- **P2** — `ConfigGrants` with its overlap refusal, and `require_permission` on the FastAPI
  adapter (needs [RFC 0059](0059-non-disclosing-denials.md) for the denial shape).
- **P3** — the DST leg for `no_permission_after_deactivation`.
- **P4** *(demand-gated)* — introspection, per-tenant config grants, provider caching.
