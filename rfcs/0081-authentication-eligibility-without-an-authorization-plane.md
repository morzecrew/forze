# RFC 0081 — Authentication eligibility without an authorization plane

- **Status:** 📝 Draft — three PRs (§12). §10 Q1 and Q2 were settled by the maintainer on 2026-11-04 (rows 7 and 9).
  - P1 has no core change and changes no deployment that works today.
  - P2 makes `allow_all` a posture finding, also with no core change. It is **breaking** under
    a production posture and waits on §10 Q2.
  - P3 adds a requirement check to `DepsRegistry.freeze()`, a core change. It is **breaking**
    for wiring that can never authenticate and waits on §10 Q1. If Q1 is refused, §5.8 A6
    ships instead.
- **Scope:** How an application authenticates without wiring the authz plane, and stays fail
  closed. Today the principal-eligibility gate offers two values. `policy_principal` reads the
  authz plane's `authz_policy_principals` document. `allow_all` checks nobody. The work:
  - `AuthnDepsModule.eligibility` also takes a gate factory, so a deployment can check the
    principal registry it really has;
  - `local_identity_deps` builds a gate over its own config where no policy-principal store is
    wired, and today's gate where one is. The preset stops depending on a plane it never had,
    and keeps deactivation wherever the plane exists;
  - `allow_all` reports a finding to the RFC 0061 production posture, so production needs an
    `Exempt` with a reason;
  - a provider can declare the routes it resolves, and `freeze()` refuses wiring where a
    declared route is unbound. A missing policy-principal store becomes a boot refusal instead
    of a 500 on every request.

  The eligibility port does not change, and the default stays `policy_principal`. It does
  **not** add a principal table to authn or a built-in credential-backed gate (§5.8 A3, A4).
  It does not let `principal_deactivation` run without `authz_route`, and it does not change
  the OIDC bootstrap preset. A gate other than the policy-principal one never takes the RFC
  0069 skips. The epoch raise's principal lock keeps today's rule. There is no new package.
  P3 adds one protocol, one record and one read-only `ProviderStore.providers()` accessor to
  `forze.application.contracts.deps`.
- **Related:**
  - [`src/forze_identity/authn/execution/deps/module.py`](../src/forze_identity/authn/execution/deps/module.py):
    the `eligibility` field (154-162), how the choice is registered (264-301), and the
    `authz_route` check for deactivation (312-316);
  - [`src/forze_identity/authn/execution/deps/deps.py`](../src/forze_identity/authn/execution/deps/deps.py):
    `_resolve_eligibility` (106-110), `_principals_to_lock` (143-167), `_api_key_skip`
    (203-232), `ConfigurablePolicyPrincipalEligibility` (589-619) and
    `ConfigurableAllowAllEligibility` (625-634);
  - [`src/forze_identity/authn/adapters/principal_eligibility.py`](../src/forze_identity/authn/adapters/principal_eligibility.py)
    holds the three adapters;
  - [`src/forze/application/contracts/authn/ports/eligibility.py`](../src/forze/application/contracts/authn/ports/eligibility.py)
    holds the port, and
    [`src/forze/application/contracts/authn/deps.py`](../src/forze/application/contracts/authn/deps.py)
    holds `PrincipalEligibilityDepPort` (92) and `PrincipalEligibilityDepKey` (103);
  - [`src/forze_identity/builtin/local/wiring.py`](../src/forze_identity/builtin/local/wiring.py)
    and [`src/forze_identity/builtin/idp/_oidc.py`](../src/forze_identity/builtin/idp/_oidc.py)
    are the two presets;
  - [`src/forze/application/execution/deps/registry.py`](../src/forze/application/execution/deps/registry.py):
    `freeze()` (265-329) and where it collects posture findings from modules (290-295);
  - [`src/forze/application/execution/assemble.py`](../src/forze/application/execution/assemble.py):
    `build_runtime` (73), which freezes the deps it assembles (224);
  - [`src/forze/application/execution/deps/frozen.py`](../src/forze/application/execution/deps/frozen.py):
    `resolvable` (413-422), the predicate the local preset detects with;
  - [`src/forze/application/contracts/deps/store.py`](../src/forze/application/contracts/deps/store.py):
    the public `plain_deps` (218) and `routed_deps` (224) fields, `exists` (355), and the raise
    for a missing dependency (284-296);
  - [`src/forze/application/execution/posture.py`](../src/forze/application/execution/posture.py)
    holds `PostureAware` (66) and `Exempt` (183), with
    [`src/forze_http/execution/deps/module.py`](../src/forze_http/execution/deps/module.py)
    (63-80) as the precedent finding;
  - [`examples/recipes/mock_server/app.py`](../examples/recipes/mock_server/app.py) (191-198)
    and [`examples/recipes/mock_server/served.py`](../examples/recipes/mock_server/served.py)
    (37-46) are forze's own workaround;
  - [RFC 0061](0061-production-posture.md) is the fail-closed posture this must not weaken.
    [RFC 0069](0069-session-revocation-fast-path.md) is the fast-path skip, whose soundness
    depends on the policy-principal gate. [RFC 0080](0080-shipped-schema-migrations-for-forze-owned-tables.md)
    is what a new authn-owned table would cost (§5.8 A4).
- **Origin:** Consumer pain verified in five consumer repositories on 2026-11-03 (§2):
  tb-chat-analysis hand-rebuilt the local preset, and bi-backend opted out with `allow_all`.

---

## 1. Summary

The eligibility gate asks whether a principal is still allowed to authenticate. forze answers it
from one place: the authz plane's policy-principal document. An application without that plane
has two choices today. It can turn the check off with `allow_all`, silently. Or it can get a 500
on every authenticated request, discovered only once traffic arrives. forze's own local preset
lands on the second, and forze's own examples seed authz rows to get around it.

This RFC keeps the default and keeps `allow_all`, and adds the missing middle.
- `eligibility=` accepts a gate factory, so a deployment checks the registry it has.
- Where no policy-principal store resolves, the local preset admits exactly the principals its
  config names. Where one resolves, it keeps today's gate.
- `allow_all` becomes a production posture finding, so a production deployment that wants it
  says why in an `Exempt`.
- A provider can declare the routes it will resolve, and `freeze()` refuses wiring where one is
  unbound. A policy-principal gate with no store to read then stops the boot, and the refusal
  names the three ways out.

A gate other than the policy-principal one always reads, so the RFC 0069 skips stay where their
proof holds.

## 2. Motivation

Of five consumers checked, two opted out of the gate. One of those two rebuilt a shipped preset
by hand. The other three run the authz plane and depend on the gate.

- **tb-chat-analysis** guards its API with one static key. It does not call `local_identity_deps`.
  It rebuilds the same `AuthnDepsModule` with `eligibility="allow_all"` (`app/src/wire/security.py:40-58`).
  The comment at 46-48 gives the reason: *"it keeps the default eligibility gate, which looks
  principals up in the authz plane's tables, and this service has no authz plane — every
  request presenting a key would 500."* The repository pins forze `<0.14`, but the preset on
  main is unchanged (`wiring.py:32-38`).
- **bi-backend** runs passwords, tokens and provisioning with no authz plane:
  `eligibility="allow_all"` (`app/src/wire/execution.py:270-282`). The reason is DECISIONS.md D6:
  *"No authorization plane yet: a member of a workspace may do anything in it."* That costs more
  than roles. `principal_deactivation` cannot be registered without `authz_route`
  (`module.py:312-316`), so the application has no forze way to switch a user off. bi-backend
  declares a `ProductionPosture` (`app/src/wire/settings/root.py:40-59`). Nothing in it noticed
  that every principal is admitted.
- **erp-backend** (`app/src/wire/execution.py:233-262`), **eis-backend**
  (`app/src/wire/execution.py:123-145`) and **hh-backend-v2**
  (`app/src/wire/execution.py:268-271`, `307-330`) wire the authz plane and keep the default.
  For them the gate carries weight. erp: *"a deactivated principal can neither log in nor
  refresh"*. hh: *"deactivating a principal stops token issuance immediately"*. hh's signup
  inserts the `authz_policy_principals` row itself (`app/src/wire/adapters/accounts.py:42-44`),
  so in practice that authz table is the principal registry. hh also records the coupling as a
  reason not to take authn alone (`execution.py:339-347`).
- **forze's own examples** seed a policy principal so the local preset's dev key can
  authenticate (`examples/recipes/mock_server/app.py:191-198`, `served.py:37-46`). The
  mock-server docs warn about it (`pages/docs/recipes/mock-server.md:84-90`).

Each consumer that hit the coupling solved it by turning the check off. None of them chose a
check over a registry they actually had, because no such choice exists. The failure arrives at
request time, as a 500 that names a document route rather than the gate.

## 3. Current state

All verified against the tree on 2026-11-03.

| Fact | Where |
| --- | --- |
| The gate is a two-value literal, `"policy_principal"` by default. An unknown string is refused at wiring. | `module.py:154-162`, `:281-289` |
| The choice is registered as one routed provider per wired route. A later registration on the same key and route conflicts, so the field is the only way to change the gate, and no other gate can be plugged in. | `module.py:264-301`; `store.py:441-451` (same-provenance overlap raises) |
| The default gate reads `authz_policy_principals`, the authz plane's document, and refuses a missing or inactive row with `authentication` ("Principal not found"). | `deps.py:603-619`; `principal_eligibility.py:40-56`; `authz/application/specs.py:58-59` |
| The gate is built per request at the boundary, not at assembly: `ctx.authn.authn(spec)` builds the orchestrator, which builds the gate, which resolves the document port. | `forze_fastapi/security/resolvers.py:155`, `:185`; `deps.py:493`, `:609`; docs `integrations/authn.md:331-333` ("on the first authentication or issue") |
| With no route and no plain fallback, as on Postgres-only wiring, resolution raises `configuration` ("Plain dependency 'document_query' not found (fallback from route 'authz_policy_principals')"), which HTTP maps to 500. | `store.py:284-296`; `base/exceptions/egress.py:123-132` |
| With a plain fallback, as on the mock, the gate reads an empty collection, and every unseeded principal gets 401 "Principal not found". That is the same message as an unknown credential. | `forze_mock/execution/module.py:511`; `principal_eligibility.py:55-56` |
| `check_wiring` does not reach the gate. It resolves operations. `AuthnRequired` reads a bound identity and builds no authn port. | `operations/wiring.py:141-200`; `hooks/authn/plans.py:24-30` |
| Authn owns no principal record. Its specs are credentials, sessions, invites, resets and an identity mapping. `register_with_password` gates the target principal before creating its first credential, so the principal must already be in the registry. | `deps.py:76-83`; `password_provisioning.py:262-268` |
| The gate runs on every authentication, every actor in a delegation chain, every issue (row-locked on Postgres) and every credential mutation. | `orchestrator.py:202`, `:253`, `:306`, `:334`; `token_lifecycle.py:126`, `:351`; `api_key_lifecycle.py:140`, `:180`, `:199`, `:253`, `:333`, `:405`; `password_lifecycle.py:119`; `password_provisioning.py:167`, `:258`, `:268`; `password_reset.py:197`, `:289` |
| `allow_all` is silent: no construction warning, no posture finding. `AuthnDepsModule` is not `PostureAware`. Even if it were, `freeze()` collects findings from modules only, so wiring passed as built `Deps`, as both presets and tb-chat-analysis pass it, would escape. | `module.py:279-280`; `registry.py:290-295` |
| The RFC 0069 skip keys on the factory class. The token skip wraps only the policy-principal gate. The API-key skip (hh request 8, CHANGELOG 0.16.0) tests for it positively. The epoch raise's principal lock tests negatively: anything but `allow_all` takes it. | `deps.py:616-619`; `deps.py:213-220`; `deps.py:159` |
| Both presets keep the default gate. `local_identity_deps` mints principals from a config file, so no policy-principal row exists unless the app seeds one. `oidc_bootstrap_identity_deps` maps IdP subjects to UUIDs. | `builtin/local/wiring.py:32-38`; `builtin/idp/_oidc.py:29-34` |
| The inventory helper documents the coupling and calls `planes=["authn"]` correct only with `allow_all`. | `inventory.py:199-210`; docs `identity-tenancy-enc/identity.md:424-430` |
| `ProviderStore` has no accessor that yields providers. It exposes `exists` and `registered_frames`. The frames carry key names, not keys or providers. The providers sit in its public `plain_deps` and `routed_deps` mappings. | `store.py:218-228`, `:355-379`; `frame.py:17-24` |
| History: the gate shipped in 0.3.0, `allow_all` in 0.5.1, the row-locked issue in 0.13.0, the token skip in 0.15.0 and the API-key skip in 0.16.0. | `CHANGELOG.md:2349`, `:1423`, `:786`, `:606`, `:492` |

## 4. Goals / Non-goals

**Goals**

- An application without the authz plane can authenticate behind a gate that refuses principals
  it does not know, reading a registry the application actually has.
- Where no policy-principal store is wired, `local_identity_deps` works out of the box, without
  seeded rows, and admits only the principals its config names. Where a store is wired, it keeps
  today's gate. No deployment that works today changes behaviour.
- `allow_all` stays available and explicit. Under a production posture it needs a written reason.
- A gate that cannot be built is refused at boot. Its message names the missing route and the
  three ways out. It no longer appears as a 500 on every request.

**Non-goals**

- Changing the default. It stays `policy_principal`, because three consumers depend on it and a
  quieter default would remove deactivation from every application that forgot to choose (§5.8 A1).
- A principal registry inside authn. Principals belong to authz or to the application (§5.8 A4).
- A built-in gate over credential rows. It cannot be both fail closed and usable for
  registration without a carve-out (§5.8 A3). It is demand-gated (§8).
- Changing `PrincipalEligibilityPort`. Every gate here fits its one method.
- A fast path for any other gate. RFC 0069's proof is about policy-principal epochs and Postgres
  row locks (row 3).

## 5. Design

### 5.1 The choices, and what each one trusts

| `eligibility=` | Reads | Admits | Trust statement |
| --- | --- | --- | --- |
| `"policy_principal"` (default) | `authz_policy_principals`, row-locked on issue | an active row | The authz plane is the registry. Deactivation takes effect on the next check. |
| a gate factory (new, W1) | whatever the factory reads | what the factory admits | The application names its registry. The gate must refuse principals it does not know. |
| `ConfigurableLocalPrincipalEligibility` (new, W2, the local preset's default) | today's policy-principal gate where that document resolves; otherwise the preset's frozen config | an active row; or, on the config, a principal named on an API-key entry | Where the authz store exists, as the default. Otherwise the config file is the registry, and removing a principal's last key and restarting deactivates it. |
| `"allow_all"` | nothing | everyone | Nobody is checked. Revocation rests on session and API-key lifecycle alone. Reported to the production posture (W4). |

### 5.2 W1: the gate seam

```python
# forze_identity.authn.execution.deps.module
EligibilityChoice = Literal["policy_principal", "allow_all"] | PrincipalEligibilityDepPort

eligibility: EligibilityChoice = attrs.field(default="policy_principal")
```

- **Validation.** A string other than the two literals is still refused (`module.py:281-289`).
  A value that is neither a string nor callable is refused in `__attrs_post_init__`
  (`configuration`, `authn_eligibility_invalid`). Both happen at construction, so neither can
  fall through to a permissive gate.
- **Registration.** A factory is registered for every wired route, exactly where the literals'
  factories are (`module.py:292-301`). There is no second field and no override path, so the
  conflict rule that made the default "non-overridable rather than merely default"
  (`module.py:266-271`) still holds.
- **The RFC 0069 skips key on the class, positively.** Both already do, and this RFC changes
  neither. The token skip wraps only inside `ConfigurablePolicyPrincipalEligibility`
  (`deps.py:616-619`), and `_api_key_skip` tests for that class (`deps.py:217-220`). A custom
  gate therefore reads on every check.
- **The lock keeps its negative test.** `_principals_to_lock` stays as it is (`deps.py:159`).
  Every gate except `allow_all` takes the epoch raise's principal lock, when the
  policy-principal document resolves and its locking read excludes. A custom gate that reads
  `authz_policy_principals` row-locked on issue then still orders its issue against an epoch
  raise, as the default gate does. A positive test there would drop that lock and let issue race
  a deactivation. For a gate that never reads the document, the lock costs one locking read and
  nothing else (row 3).

A gate over the application's own table is a few lines:

```python
@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableUserEligibility:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> PrincipalEligibilityPort:
        return UserEligibility(users=ctx.doc.query(user_spec))  # refuses unknown or inactive
```

The docs say what a gate owes. It refuses a principal it does not know. It reads tenant-unaware
state, because eligibility runs before a tenant is bound. It reads the row locked when asked
`for_update` inside a transaction, if it wants issue to serialize with its own deactivation.

### 5.3 W2: the local preset's gate

```python
# forze_identity.builtin.local
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableLocalPrincipalEligibility:
    """Today's policy-principal gate where its store resolves; the config-backed gate elsewhere."""

    config: LocalIdentityConfig

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> PrincipalEligibilityPort:
        if ctx.deps.resolvable(DocumentQueryDepKey, route=policy_principal_spec.name):
            return ConfigurablePolicyPrincipalEligibility()(ctx, spec)  # exactly today's gate

        return LocalPrincipalEligibility.of(self.config)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalPrincipalEligibility(PrincipalEligibilityPort):
    """Eligible while the config names the principal on an API key: the config is the registry."""

    principals: frozenset[UUID]

    async def require_authentication_allowed(
        self, principal_id: UUID, *, for_update: bool = False
    ) -> None:
        del for_update  # no row to lock (the port's "a port with no row to lock" case)

        if principal_id not in self.principals:
            raise exc.authentication("Principal not found")


def local_identity_deps(
    config: LocalIdentityConfig,
    *,
    authn_route: str = "main",
    tenancy_route: str = "main",
    eligibility: EligibilityChoice | None = None,  # None: ConfigurableLocalPrincipalEligibility
) -> Deps: ...
```

- **How the plane is detected.** When the gate is built, the factory asks
  `FrozenDepsRegistry.resolvable(DocumentQueryDepKey, route="authz_policy_principals")`
  (`frozen.py:413-422`). That predicate decides whether today's gate can be built at all. It
  holds when the route is registered, or a plain fallback is, and the spec inventory admits the
  route when one is declared. The frozen deps cannot change after `freeze()`, so the answer is
  fixed for the runtime's life.
- **Why this changes no working deployment.** Where the document resolves, the preset builds
  exactly today's gate. An application that seeded policy principals and deactivates them
  through the authz registry keeps that. Where the document does not resolve, today's preset
  fails every authentication (§3), so nothing that works today depends on that wiring. Only
  there does the config gate engage. Always switching the preset to the config gate would be
  the silent downgrade row 1 rejects (§5.8 A9).
- **The mock counts as wired.** Its plain document fallback resolves the route
  (`forze_mock/execution/module.py:511`). On the mock, therefore, the preset keeps today's gate
  and still needs a seeded principal. The mock-server examples keep their seed, and the docs
  warning stays (row 10).
- **An explicit choice wins.** `local_identity_deps(..., eligibility=...)` takes any
  `EligibilityChoice` and skips detection. Pass `"policy_principal"` to insist on the authz gate
  (refused at boot from P3 if its store is missing), a factory, or `"allow_all"`.
- **No fast path either way.** The preset's kernel is `AuthnKernelConfig()` (`wiring.py:33`),
  which carries no revocation states, so neither branch has a skip to take.
  `_principals_to_lock` treats the factory as any gate other than `allow_all`.
- **No requirement.** The factory declares none (§5.5). It never resolves the document unless
  the document resolves.
- **The config set.** `LocalPrincipalEligibility.of` computes the set once, from
  `entry.principal_id` over `config.api_keys`. A principal that appears only in
  `principal_tenants` holds no credential and is not eligible.

### 5.4 W4: `allow_all` is loud

`AuthnDepsModule` becomes `PostureAware` (`posture.py:66`), as `HttpDepsModule` is
(`forze_http/execution/deps/module.py:63-80`). With `eligibility="allow_all"` it reports one
finding per wired route:

```python
WiringFinding(
    source="forze_identity",
    target="authn_eligibility:main",
    rule="admits every principal: deactivating one does not stop its authentication",
)
```

- **P2 collects from the module, with no core change.** `freeze()` already collects findings
  from modules (`registry.py:290-295`). That covers an `AuthnDepsModule` passed as a module, as
  bi-backend passes it (`execution.py:270`). It misses one passed as built `Deps`, as
  tb-chat-analysis passes it (`security.py:49-58`). No preset selects `allow_all` (row 2), so no
  preset falls into that gap.
- **P3 closes the gap.** With W3's provider walk (§5.5), `freeze()` also collects findings from
  registered providers. `ConfigurableAllowAllEligibility` reports the same finding, and it is
  built one per route so its target names the route. Wiring passed as built `Deps` is then
  covered. When the module reports the same finding, the duplicate is dropped by `(target, rule)`
  (`posture.py:300`).
- **Without a posture.** `AuthnDepsModule.__attrs_post_init__` logs one warning when
  `eligibility="allow_all"`, as the HTTP module does for cleartext credentials
  (`forze_http/execution/deps/module.py:38-44`). Construction runs however the module is
  passed, so from P2 the warning reaches built-`Deps` wiring too.
- **Under a production posture.** The finding refuses the boot unless the deployment exempts
  it. That includes an unset environment (RFC 0061: silence means production):

  ```python
  ProductionPosture(
      settings=self,
      ...,
      exempt=(Exempt(target="authn_eligibility:main", reason="No authorization plane yet (D6)"),),
  )
  ```

### 5.5 W3: refusing an unbuildable gate at boot

A provider can say which `(key, route)` frames it resolves when called. `freeze()` checks those
frames against the merged store before it returns. `build_runtime` (`assemble.py:73`) freezes
the deps it assembles (`assemble.py:224`), so the check runs on every runtime build. It is not
an opt-in diagnostic.

```python
# forze.application.contracts.deps  (record and protocol names: row 12)
@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Requirement:
    key: DepKey[Any]
    route: StrKey
    remedy: str
    """What to wire instead. Names keys and choices, never values (RFC 0061)."""


@runtime_checkable
class DeclaresRequirements(Protocol):
    def requirements(self) -> Iterable[Requirement]: ...


# ProviderStore (store.py), new: a read-only walk over its public plain_deps (:218) and
# routed_deps (:224) mappings, which hold providers as registered.
def providers(self) -> Iterator[tuple[DepKey[Any], StrKey | None, Any]]: ...
```

```python
# DepsRegistry.freeze(), after ProviderStore.merge (registry.py:317)
for key, route, provider in store.providers():
    if isinstance(provider, DeclaresRequirements):
        for req in provider.requirements():
            if not (store.exists(req.key, route=req.route) or store.exists(req.key)):
                raise exc.configuration(..., code="dependency_requirement_unbound")
```

- **The accessor is new and in scope.** `ProviderStore` has no accessor that yields providers.
  `registered_frames()` (`store.py:370-379`) gives frames that carry key names only
  (`frame.py:17-24`), so they can neither be looked up nor type-checked. `providers()` is a few
  lines over the two public mappings. It adds no state, and the same walk serves W4's
  provider-level findings.
- **The predicate.** It is `FrozenDepsRegistry.resolvable`'s predicate (`frozen.py:413-422`),
  on the store: the route is registered (`store.exists`, routed only, `store.py:355-366`), or
  the plain fallback is. The inventory guard is left out, because it is attached after freeze.
  A route the inventory refuses is already reported by reconciliation (`runtime.py:360`).
- **Provider-level, not module-level.** Both presets and tb-chat-analysis pass built `Deps`
  (`wiring.py:32-38`, `_oidc.py:29-34`, `security.py:49-58`). A module-level declaration would
  never see them (§5.8 A8).
- **The default gate's declaration.** `ConfigurablePolicyPrincipalEligibility` declares
  `Requirement(key=DocumentQueryDepKey, route="authz_policy_principals", remedy=...)`. The
  refusal reads:

  ```text
  configuration (dependency_requirement_unbound): 'authn_principal_eligibility' on route 'main'
  resolves 'document_query' on route 'authz_policy_principals', which is not registered and has
  no plain fallback. Wire the authz plane's policy principal document, pass
  AuthnDepsModule(eligibility=...) a gate over your own principal registry, or declare
  eligibility="allow_all".
  ```

- **What it cannot see.** A plain fallback satisfies the check. On the mock, then, an unseeded
  principal still gets 401 "Principal not found" under the default gate.
- **If Q1 is refused.** P3 ships §5.8 A6 instead: a named `configuration` error raised when the
  default gate is built without its store. The refusal still comes per request, but the message
  explains itself.

### 5.6 Failure semantics

| Wiring | Today | After |
| --- | --- | --- |
| Default gate, policy-principal store wired | reads the row; missing or inactive → 401 | unchanged |
| Default gate, no store and no plain fallback (Postgres-only, no authz) | 500 on every authentication | refused at `freeze()`, before the runtime exists (P3). If Q1 is refused, a named `configuration` error per request (A6) |
| Default gate, plain fallback (mock), unseeded principal | 401 "Principal not found" | unchanged; docs warning stays |
| `local_identity_deps`, policy-principal document resolves (routed, or the mock's plain fallback) | today's gate | unchanged |
| `local_identity_deps`, document does not resolve | 500 on every authentication | eligible if and only if the config names the principal on a key (P1) |
| `oidc_bootstrap_identity_deps` without authz | 500 on every authentication | refused at `freeze()` (P3) |
| `allow_all` | silent | warning; under a production posture refused unless exempted (P2 for module-passed wiring, P3 for built `Deps`) |
| A gate factory | impossible | registered; reads on every check, never skips (P1) |
| Unknown string, or a non-callable | unknown string refused at wiring | both refused at construction |

### 5.7 Compatibility

- **P1 is additive and changes no working deployment.** The gate seam accepts a new type. The
  local preset's config gate engages only where today's preset cannot build its gate (§5.3).
- **P2 is breaking under a production posture.** There, `allow_all` needs an `Exempt`.
  bi-backend is the one known consumer affected: one line, citing D6.
- **P3 is breaking for wiring that could never authenticate.** The default gate with no store
  and no fallback is refused at freeze. A test that wires authn and never authenticates must add
  the store or choose a gate.
- **Beta.** These breaking changes are marked as such, with no migration code.
- **No port, spec or table change.** A third-party `PrincipalEligibilityPort` works unchanged as
  the adapter a factory returns.

### 5.8 Alternatives considered

- **A1: change the default, or pick it by whether authz is wired.** Either answers the pain with
  no new surface. Both make the dangerous outcome silent: an application that forgets to wire
  authz loses deactivation and is never told. RFC 0061's rule is that the dangerous default is
  the safe one. Rejected (row 1).
- **A2: an `eligibility="authn_only"` literal with a trust statement.** It behaves exactly like
  `allow_all`, under a name that says what is not needed instead of what is checked. A reader
  of the wiring would think something is checked. Rejected (row 2).
- **A3: a built-in `eligibility="credentials"` gate.** It admits a principal while the principal
  holds an active first-party credential, and reads only authn-owned rows. It is the obvious
  authn-only gate, and it fails closed, but it is not usable as written:
  - `register_with_password` gates the target before its first credential exists
    (`password_provisioning.py:268`). Every fresh registration is refused, including
    bi-backend's bootstrap, which mints `uuid4()` and then registers
    (`bi/platform/bootstrap.py:49-53`).
  - Fixing that needs a registration carve-out, a second predicate on a one-method port. The
    carve-out admits a fabricated id at registration, which is `allow_all` at exactly the call
    that creates principals.
  - A principal that holds only sessions, such as one created by the external bootstrap recipe,
    is refused on every refresh.
  - "Any active credential across families" and "per family" disagree after a partial revoke.
  - The issue lock moves to credential rows, which needs its own race test. No fast path is
    possible.

  The seam lets an application write this gate today with semantics it chooses. Demand-gated (§8).
- **A4: an authn-owned principal-standing document (`authn_principals`: id, is_active).** It
  gives every authn-only application a real switch. The cost: a new forze-owned table and its
  migration (RFC 0080's subject), a second principal registry beside authz's that must agree
  with it, and a write on every provisioning path, while hh already treats the authz table as
  the registry. Rejected (row 5).
- **A5: the local preset sets `allow_all`.** It is one line and needs no new gate. But it is a
  preset silently choosing `allow_all`, against row 2. With W4, every production use of the
  preset would also need an `Exempt` for a check that can in fact be made, because the config is
  the registry. Rejected.
- **A6: only name the request-time error.** One `if` in the factory, raising
  `authn_eligibility_unbound` with the remedy. The message would be clear, but the failure would
  still be a 500 per request in production. It is the fallback for P3 if W3's protocol is
  refused (§10 Q1, §12).
- **A7: `check_wiring` dry-runs every authn route.** It reuses an existing pass. But that pass is
  opt-in (only tests call it), and it builds ports against a throwaway context. A declaration
  builds nothing and runs on every build. Rejected.
- **A8: a module-level declaration, like module-level `PostureAware`.** It is simpler to collect.
  But it misses every wiring passed as built `Deps`: both presets and tb-chat-analysis
  (`wiring.py:32-38`, `_oidc.py:29-34`, `security.py:49-58`). Rejected for W3. For W4 it is the
  P2 step, and P3's provider walk closes the gap.
- **A9: the local preset always uses the config gate.** It is simpler: no detection. But an
  application that seeded policy principals and deactivates them through the authz registry
  would silently lose that deactivation on upgrade. That is the silent change row 1 rejects.
  Rejected in favour of detection (§5.3).

## 6. Tests

- **W1.** Every wired route registers the factory. A string other than the two literals, and a
  non-callable, are refused at construction. Take a custom gate with authz wired, revocation
  states set and `key_mac` set. It is not wrapped in `AdmittedTokenEligibilityAdapter`, and
  `_api_key_skip` returns `None`. On Postgres, `_principals_to_lock` still returns the principals
  for it (the lock is kept), and returns `None` for `allow_all`. These extend
  `tests/unit/test_forze_authn/test_authn_execution_deps.py`.
- **W2, detection.** The preset's factory builds today's gate when the document resolves
  routed, and when it resolves through the mock's plain fallback. A deactivated principal is
  refused through both. It builds the config gate when nothing resolves, including on a context
  with no document provider at all. An explicit `eligibility=` skips detection.
- **W2, the config gate.** A configured principal is admitted. One that appears only in
  `principal_tenants` is refused, and so is an unknown one, both with `authentication`. These go
  in `tests/unit/test_forze_identity/builtin/local/`. The mock-server examples are unchanged,
  and `tests/unit/test_examples/` keeps covering them.
- **W4 (P2).** Under a production posture, a module-passed `allow_all` refuses, an `Exempt` with
  a reason admits it, and a non-production environment only warns. There is one finding per
  route. The construction warning fires for built-`Deps` wiring too.
- **W3 (P3).** `ProviderStore.providers()` yields every plain and routed entry once. The default
  gate with no store and no fallback is refused at `freeze()`, and the message names both frames
  and no value. A plain fallback passes. A routed store passes. Wiring passed as built `Deps` is
  checked too. The OIDC preset without authz is refused at freeze. Built-`Deps` `allow_all`
  wiring reports its finding.
- **Not tested.** What an application's own gate admits; that is the application's code. No new
  real-Postgres suite: the policy-principal path is unchanged, and the existing
  `tests/integration/test_forze_authn/` suites cover it.

## 7. Docs

- `pages/docs/integrations/authn.md` (eligibility, 46-56): the four rows of §5.1 with their
  trust statements, what a custom gate owes (§5.2), the posture finding, and the boot refusal.
  The `allow_all` wording must say "nobody is checked", not "no authz plane".
- `pages/docs/recipes/local-identity.md`: the preset's detection rule, that the config is the
  registry where no store is wired, and the `eligibility=` passthrough.
- `pages/docs/recipes/mock-server.md:84-90`: the warning stays, and says why. The mock's plain
  fallback counts as a wired store, so the preset keeps the default gate there.
- `pages/docs/identity-tenancy-enc/identity.md:424-430` and the `inventory.py:199-210` docstring:
  `planes=["authn"]` pairs with any gate other than the policy-principal one, not only
  `allow_all`.
- `pages/docs/running-in-prod/production-posture.md` (97-118): `authn_eligibility:<route>` beside
  `http_service:<name>`.
- `skills/forze-skills/references/authn.md` and `fastapi-identity.md` (published app-author
  skills).
- `CHANGELOG.md`, one entry per phase:
  - P1: **Added**, for the gate seam and the local preset's config gate;
  - P2: **Changed**, for the `allow_all` posture finding (**Breaking** under a production
    posture);
  - P3: **Changed**, for the freeze-time refusal (**Breaking**).

## 8. Out of scope

- **A built-in credential-backed gate.** §5.8 A3. Build it when a consumer needs a
  deactivation switch without an authz plane and accepts the registration carve-out. The seam
  covers it until then.
- **`principal_deactivation` without `authz_route`** (`module.py:312-316`). The cascade's first
  write is the authz registry (`principal_deactivation.py:48-66`). An application with its own
  registry deactivates in its own table and calls `TokenLifecyclePort.revoke_tokens`. Making the
  cascade's principal leg pluggable waits for a consumer.
- **The OIDC bootstrap preset's gate.** Its resolver turns any IdP subject into a UUID with no
  registry behind it, so "admit every IdP user" would be `allow_all` under another name. It keeps
  the default, and W3 turns its missing store into a boot refusal. An `eligibility=` passthrough
  there is §10 Q4.
- **Other cross-plane requirements.** `ConfigurablePrincipalDeactivation` resolves the authz
  registry route (`deps.py:693`). `authn_revocation_states_mismatch` is also found only at first
  authentication (`deps.py:577-582`). The first fits W3's protocol (row 13). The second is a
  value comparison, not a frame, and is not covered.
- **A fast path for any other gate.** Row 3.

## 9. Risks

- **The preset no longer fails loudly when its store is forgotten.** An application that meant
  to use the authz gate, but did not wire the policy-principal store, gets the config gate
  instead of a 500. Accepted: without the store there was no authz deactivation to lose, and the
  config gate still admits only configured principals. The local-identity docs say how to insist
  (`eligibility="policy_principal"`). From P3 that choice is refused at boot when the store is
  missing.
- **Built-`Deps` `allow_all` wiring escapes the posture between P2 and P3.** It gets the
  warning only. Accepted: no preset selects `allow_all`. The one known consumer with a
  production posture, bi-backend, passes the module. tb-chat-analysis passes built `Deps` but
  declares no posture.
- **Freeze-time refusal breaks test contexts (P3).** Suites that wire `AuthnDepsModule` with the
  default gate and no store, and never authenticate, fail at freeze. forze's own suites and
  consumers' are at risk. Mitigation: a plain fallback (the mock) satisfies the check. P3 counts
  the churn in forze's suites before landing. The changelog marks it **Breaking**.
- **The local gate read as theater.** The local verifier only ever returns configured principals
  (`api_key_verifier.py:45-50`), so the config gate repeats a fact. That is the intent: it is the
  preset's registry check, made honestly where no other registry exists. It is not `allow_all`,
  and it stays correct if the verifier is swapped through `api_key_verifiers`.
- **A custom gate that admits everything is `allow_all` without the finding.** Accepted. The gate
  is reviewed application code, and the docs state what it owes. The finding exists for the
  framework's own `allow_all`, whose name says nothing about a reviewed decision.
- **bi-backend's production boot refuses after upgrading to P2.** Intended. The `Exempt` is one
  line and its reason is already written (D6).
- **The mock still answers 401 for unseeded principals under the default gate.** That covers the
  local preset there too. W3 cannot see an empty table behind a plain fallback. The docs warning
  stays.

## 10. Unresolved questions

Q2 must be settled before P2 counts as locked, and Q1 before P3. Q3 is free for implementation.
Q4 is demand-gated.

- **Q1 (maintainer, blocks P3).** P3 adds a provider-level requirement protocol and the
  `ProviderStore.providers()` accessor to `forze.application.contracts.deps`, which is a new core
  concept. If it is refused, P3 ships §5.8 A6 instead. The provider-level posture walk is then
  not built, and built-`Deps` `allow_all` wiring keeps the warning only.
- **Q2 (maintainer, blocks P2).** Is the `allow_all` finding acceptable as breaking under a
  production posture? The alternative is a warning-only finding that never refuses. That keeps
  bi-backend booting unchanged, but under it `allow_all` is no louder than a log line.
- **Q3 (implementation).** Where W3's check runs: `freeze()` (row 7) or
  `ExecutionRuntime.__attrs_post_init__` beside inventory reconciliation (`runtime.py:322-357`).
  `freeze()` also covers `context_from_modules`. Move it only if freeze proves to lack something
  the check needs.
- **Q4 (demand-gated).** Should `oidc_bootstrap_identity_deps` take an `eligibility=` passthrough?
  Add it when a consumer pairs the preset with its own registry.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The default stays `eligibility="policy_principal"`. erp-backend, eis-backend and hh-backend-v2 depend on it for deactivation. A default that varies by wiring, or a permissive one, would remove deactivation silently from any application that forgot to choose, against RFC 0061's rule that the dangerous default is the safe one. The same rule binds the presets (row 6). |
| 2 | `LOCKED` | `allow_all` keeps its meaning, every principal eligible, and only an application's explicit `eligibility="allow_all"` selects it. No preset, default or fallback chooses it, and no new literal aliases it (`authn_only` rejected, §5.8 A2). Reopening this would undo the posture finding's premise (row 9). |
| 3 | `LOCKED` | Only `ConfigurablePolicyPrincipalEligibility` takes the RFC 0069 skips: the token skip (`AdmittedTokenEligibilityAdapter`) and the API-key skip (`ApiKeyEligibilitySkip`). Both key on that class positively. The skip is sound because every deactivating write raises a policy-principal epoch and issue locks the Postgres row, and no other gate can vouch for either. The epoch raise's principal lock keeps its negative test (`_principals_to_lock`: every gate except `allow_all`). A custom gate that reads `authz_policy_principals` row-locked therefore still orders its issue against the raise. |
| 4 | `ASSUMED` | `AuthnDepsModule.eligibility` widens to `Literal["policy_principal", "allow_all"] \| PrincipalEligibilityDepPort`. It is one field, not a second one. A factory is registered routed exactly where the literals' factories are. A string other than the two literals, and a non-callable, are refused at construction (`authn_eligibility_invalid`). |
| 5 | `ASSUMED` | Authn gains no principal registry of its own: no table, no spec. The gate reads whatever registry the deployment has: the authz plane, the application's table, or a preset's config (§5.8 A4). |
| 6 | `ASSUMED` | `local_identity_deps` defaults to `ConfigurableLocalPrincipalEligibility`, which decides when the gate is built. If `FrozenDepsRegistry.resolvable(DocumentQueryDepKey, route="authz_policy_principals")` holds (routed, or a plain fallback such as the mock's), it builds today's policy-principal gate. Otherwise it admits exactly the principals `config.api_keys` names and refuses any other with `authentication` "Principal not found". The config gate therefore engages only where today's preset cannot authenticate at all, and no working deployment loses deactivation (§5.8 A9). An explicit `eligibility=` skips detection. Trust statement for the config gate: the config is the registry, and removing a principal's last key and restarting deactivates it. |
| 7 | `LOCKED` | The boot-time requirement check, accepted by the maintainer on 2026-11-04 (§10 Q1) and shipped in P3, after P1 and P2: a provider may declare `Requirement(key, route, remedy)` frames through `DeclaresRequirements`. A new read-only `ProviderStore.providers()` walks the public `plain_deps` and `routed_deps` mappings. `DepsRegistry.freeze()` refuses (`configuration`, `dependency_requirement_unbound`) a declared frame that has neither its route nor a plain fallback registered. The check is per provider, not per module, so wiring passed as built `Deps` is covered. Errors name keys and routes, never values. |
| 8 | `ASSUMED` | If row 7 lands, `ConfigurablePolicyPrincipalEligibility` declares `(DocumentQueryDepKey, "authz_policy_principals")`, with a remedy naming the three ways out: wire the authz document, pass a gate, or declare `allow_all`. The same refusal reaches `oidc_bootstrap_identity_deps` without authz, and that preset's gate is otherwise unchanged. The local preset's factory declares nothing (§5.3). |
| 9 | `LOCKED` | The `allow_all` posture finding refuses under a production posture, accepted by the maintainer on 2026-11-04 (§10 Q2) over warning-only. As designed, `AuthnDepsModule` is `PostureAware` and reports `authn_eligibility:<route>` (P2). Provider-level collection, with one `ConfigurableAllowAllEligibility` per route, covers built `Deps` (P3). There is one construction warning without a posture. Under a production posture, including an unset environment, the boot is refused unless `Exempt` covers it. That is breaking for bi-backend. |
| 10 | `ASSUMED` | The mock-server examples keep their policy-principal seed (`app.py:191-198`, `served.py:37-46`), and the mock-server docs warning stays. On the mock the plain fallback resolves the document, so the preset keeps today's gate there. |
| 11 | `ASSUMED` | `principal_deactivation` still requires `authz_route` (`module.py:312-316`). An application with its own registry deactivates there and revokes tokens itself (§8). |
| 12 | `OPEN` | The names and module of `Requirement` and `DeclaresRequirements`, and the remedy's shape (a string on the record, or a hint keyed by error code). The accessor is named and in scope (row 7). Settle these while writing P3, and log the choice. |
| 13 | `OPEN` | Whether `ConfigurablePrincipalDeactivation` declares its authz registry route in the same change. It is the same mechanism and additive. Decide by the size of P3's diff, and log it. |
| 14 | `ASSUMED` | No built-in credential-backed gate in this RFC. It is demand-gated on a consumer that needs a deactivation switch without an authz plane and accepts the registration carve-out (§5.8 A3). |
| 15 | `ASSUMED` | W4 ships ahead of W3, at module level, in its own phase (P2), with no core change. Built-`Deps` `allow_all` wiring gets only the warning until P3's provider walk. No preset selects `allow_all`, and the one known consumer with a production posture passes the module. |

## 12. Phasing

- **P1 = W1 + W2.** One PR in `forze_identity` only: the gate seam, the local preset's detecting
  gate and its passthrough, and the docs. It is additive, and no working deployment changes
  (§5.7). Once tb-chat-analysis upgrades, it can delete its hand-built wiring and call the
  preset. Its Postgres wiring has no policy-principal store, so the preset will use the config
  gate.
- **P2 = W4 at module level.** One PR in `forze_identity` only: `AuthnDepsModule` becomes
  `PostureAware`, with the `allow_all` construction warning and the docs. There is no core
  change. Gated on §10 Q2. It is **breaking** under a production posture, and the release notes
  name bi-backend's one-line `Exempt`.
- **P3 = W3, plus provider-level posture collection.** One PR across core and `forze_identity`:
  - `ProviderStore.providers()`, the record and the protocol;
  - the freeze check and the policy-principal declaration;
  - provider-level findings, with a per-route `ConfigurableAllowAllEligibility`.

  Gated on §10 Q1. It is **breaking** for wiring that could never authenticate. **If Q1 is
  refused, P3 ships §5.8 A6 instead.** That is a named `configuration` error
  (`authn_eligibility_unbound`), raised when the default gate is built without its store. It is
  still per request, but the message explains itself and names the three ways out.
- **Demand-gated:**
  - a built-in credential-backed gate (row 14);
  - a pluggable deactivation cascade (row 11);
  - the OIDC preset passthrough (§10 Q4).
