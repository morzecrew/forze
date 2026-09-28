# RFC 0061 — Fail-closed production posture

- **Status:** ✅ Complete — P1–P2 shipped (#466): `ProductionPosture` carrying the application's settings, the four rule kinds and the `_dev_only` marker default, `Exempt` with a required reason, evaluation at `check_wiring` (the `WiringReport.findings` channel) and at runtime construction, and the `PostureAware` escalation of the HTTP cleartext-credentials check. P3's CLI entry and further rules stay demand-gated; exemptions shipped early (row 16).
- **Scope:** A `ProductionPosture` the runtime evaluates at `check_wiring` and again at `build_runtime`: required-set fields, HTTPS-required fields, refused development values, and a default environment of "production" when nothing says otherwise. Errors name **keys, never values**. Touches `forze.application.execution` (assembly and the wiring report — including the findings channel three sibling RFCs also report through, §5.2) and gives the existing per-config safety *warnings* somewhere to escalate to. No settings class owned by the framework — §3 says why that stays the app's.
- **Related:** [`src/forze/application/execution/operations/wiring.py:123`](../src/forze/application/execution/operations/wiring.py) (`check_wiring`, the dry-run pass and its `WiringReport.raise_if_failed`), [`src/forze/application/execution/assemble.py:72`](../src/forze/application/execution/assemble.py) (`build_runtime`, the second gate), [`src/forze/base/settings.py`](../src/forze/base/settings.py) (the settings doctrine: the root class is the application's, `RuntimeSettings` is mounted as a field), [`src/forze_http/execution/deps/configs.py:202-241`](../src/forze_http/execution/deps/configs.py) (`_warn_if_credentials_travel_in_cleartext` — a shipped safety check that is a *warning* precisely because it cannot know whether the deployment is production; this RFC is what it escalates to), [RFC 0057](0057-derived-permissions-and-config-grants.md) (`ConfigGrants` is posture-checked the same way).
- **Origin:** A working-time ledger whose `production_settings_errors` returns the **names** of missing or unsafe keys and never their values, treats an unset environment as production, requires HTTPS origins, recognizes and refuses the dev database URL, the dev password and the dev port, refuses wildcard CORS, and refuses to start on any error — with protected routes answering 501 rather than 500 or 200 until OIDC is configured.

---

## 1. Summary

An application declares a posture: these fields must be set, these must be HTTPS, these patterns
are development-only. The runtime evaluates it as part of the wiring pass, and a violation is a
boot refusal naming the offending keys. An unset environment counts as production, so the
dangerous default is the safe one.

## 2. Motivation

Every deployment incident that starts with "it was running with the dev settings" is this check
missing. The framework already knows the individual hazards — it warns about credentials over
plaintext HTTP — and it has nowhere to turn a warning into a refusal, because the config that
warns cannot know whether it is looking at a laptop or at production.

That is stated, almost verbatim, inside the shipped warning:
*"a service mesh terminates TLS in a sidecar … refusing it would need an opt-out flag to stay
usable, which is a decision this config should not make on its own."* The posture is that
decision, made once, by the deployment, in one place.

## 3. Current state

**`check_wiring` dry-runs operations and nothing else.** It resolves every operation against a
throwaway context to catch missing or misrouted dependencies, and reports fallbacks. It has no
notion of an environment, a required field, or an unsafe value.

**There is no environment concept anywhere in `src/`.** Verified against `forze.base.settings` and
the lifecycle modules: no `ENVIRONMENT`, no `production`, no `debug` flag. `RuntimeSettings` carries
log level, render mode, access-log mode and telemetry choice, as a plain `BaseModel` mounted by the
application — the doctrine being that the env prefix, nesting and extra-key policy are deployment
decisions.

**Safety checks exist, at per-config granularity, and warn.** The HTTP service config's cleartext
check is the clearest example, and its docstring is the argument for this RFC: the check is right,
the escalation is impossible locally, and a flag per config would be worse.

**The posture cannot be a framework settings class.** §3's doctrine means the framework must take
a *declaration about* the app's settings — field paths and rules — rather than own the settings
object. That shapes the whole design: the posture names keys by path and reads them through the
app's own model.

## 4. Goals / Non-goals

**Goals**

- One declaration, evaluated at the two moments a deployment can still be stopped.
- Errors name keys, never values, so a boot log is not a credential leak.
- Unset environment ⇒ production. The safe reading of silence.
- The shipped per-config warnings escalate to refusals under the posture, instead of each growing
  its own opt-out.
- Development stays frictionless: the posture is inert unless the environment says production.

**Non-goals**

- **Not a settings framework.** The app owns its settings class (§3). The posture declares rules
  over paths into it.
- **Not a secrets manager.** Whether a secret came from a vault is the secrets plane's business;
  the posture asks whether it is *set*.
- **Not a security scanner.** A fixed, declared rule set — no heuristics over arbitrary config.
- **Not a runtime guard.** Evaluated at wiring and assembly, never per request. A per-request
  posture check is a latency cost for a value that cannot change after boot.

## 5. Design

### 5.1 The declaration

```python
ProductionPosture(
    env_default="production",
    require_set=("auth.oidc_issuer", "db.password"),
    require_https=("http.public_base_url", "identity.redirect_uri"),
    refuse_dev_values=(DevValue(pattern="localhost", fields=("db.dsn", "http.public_base_url")),
                       DevValue(pattern="_dev_only"),
                       DevValue(pattern="*", fields=("cors.allow_origins",))),
)
```

Rules are evaluated against the app's settings object by path. A path that does not resolve is
itself an error — a posture referring to a renamed field is a posture nobody is enforcing.

The shipped default set carries the origin's patterns (loopback hosts in a DSN or a public URL,
`_dev_only` markers, wildcard CORS, a missing secret where the auth method needs one), because a
posture whose rules every app writes from scratch is a posture most apps will not write.

### 5.2 Where it runs

- **`check_wiring`** — the posture's findings reach the caller through the `WiringReport`, so an
  app that already calls it in CI or at startup gets them with everything else. The report as it
  stands carries `checked`, `failures: tuple[WiringFailure, ...]` and `fallbacks`
  ([`wiring.py:79-97`](../src/forze/application/execution/operations/wiring.py)) — a posture
  violation is **not** an operation that failed to resolve, so either it arrives as a new
  `findings` channel or it is synthesized as `WiringFailure`s. The first keeps `ok` and
  `raise_if_failed` meaning what they mean today and is the leaning; §10 carries it.

  This RFC **owns that channel**, because it is not only the posture's: the storage-guarantee
  reconciliation ([RFC 0066](0066-storage-guarantees.md) decision 9), `ConfigGrants`' unknown-key
  check ([RFC 0057](0057-derived-permissions-and-config-grants.md) §5.4) and the audit allowlist's
  wiring check ([RFC 0060](0060-audit-spec.md) §5.3) all want the same thing — a declaration-time
  finding that reaches the caller with everything else instead of raising from wherever it was
  noticed. One channel, four reporters.
- **`build_runtime`** — evaluated again, because `check_wiring` is optional and an app that skips
  it must still not boot with dev settings in production.

Twice, deliberately: the first is the good citizen's early signal, the second is the floor.

### 5.3 Errors name keys

A violation renders as `configuration` listing paths and rule names — `db.password: required and
unset`, `http.public_base_url: must be https`, `cors.allow_origins: development-only value` — and
**never** the value. A boot failure is logged, shipped to a collector and pasted into a ticket; a
refusal that echoed the offending DSN would put a password in all three.

### 5.4 Escalating the existing warnings

A per-config safety check may declare itself posture-aware: warn when the posture is absent or
non-production, refuse when it is production. The cleartext-credentials check is the first
adopter, and it is the model — its service-mesh escape hatch becomes "declare the field exempt in
the posture", which is a reviewed line in the deployment's own declaration rather than a flag on
every config.

### 5.5 What "environment" means

One field, read from the app's settings at the declared path, with `env_default` applied when it
is unset or empty. Non-production values are the app's own vocabulary (`dev`, `test`, `staging`);
the framework only distinguishes "production" from "not", because a framework that enumerated
environments would be wrong about the fourth one.

### Alternatives considered

- **A framework-owned `Settings` base with the checks built in.** Much friendlier, and it
  contradicts the settings doctrine for one feature — and it would force an env prefix and
  extra-key policy on every app.
- **Refuse in each config, with an opt-out flag per config.** What the cleartext check rejected,
  for the reason it gives: N flags, each individually reasonable, none reviewable together.
- **A separate `just` check rather than a boot gate.** A CI check is skippable and does not see the
  deployment's own environment. Both is best; the boot gate is the part that cannot be skipped.
- **Warn-only in production.** A warning in a boot log nobody reads is how the incident in §2
  happens.

## 6. Tests

- Each rule kind: unset required field refuses; an `http://` URL in a `require_https` field
  refuses; a dev pattern in a covered field refuses; a wildcard CORS entry refuses.
- Key-only errors: the refusal message and the log record are asserted **not** to contain the
  offending values (the leg that matters — a `secret` value planted in the setting must not appear
  anywhere in the raised error or the captured log).
- Silence is production: with the environment unset, every rule applies.
- Non-production: with the environment `dev`, the posture is inert and the shipped warnings still
  warn.
- Both gates: a violation is reported by `check_wiring` and, independently, refuses
  `build_runtime` when `check_wiring` was never called.
- Unresolvable path: a rule naming a field the settings model does not declare refuses, naming the
  path.
- Escalation: the cleartext-credentials case warns without the posture and refuses with it.
- **Not tested:** that the rule set is complete. It is a declared list, and §9 says so.

## 7. Docs

A running-in-prod page section: the declaration, the shipped rule set as a table, the two
evaluation points, and the sentence that is the whole feature — **an unset environment is
production.** Plus the migration note: adopting the posture can refuse a deployment that starts
today, which is the point, and the way to find out is `check_wiring` in CI first.

## 8. Out of scope

- **Checking the *contents* of a secret** (entropy, format, rotation age). The secrets plane's, and
  a posture that graded passwords would be a scanner.
- **Runtime drift** (a value changed after boot). Settings are read at assembly; a live-reload
  posture needs a reload story first.
- **Per-tenant posture.** Deployment-wide.
- **Recognising cloud metadata** to infer the environment. Magic, and wrong in every hybrid
  deployment.

## 9. Risks

- **A posture that refuses a currently-working deployment.** Adoption can break a boot that runs
  today — intended, and still a break. Mitigation: it is declared, so adoption is a choice; the
  docs route apps through `check_wiring` in CI first.
- **False confidence.** A green posture says "the declared rules passed", not "this deployment is
  safe". Mitigation: the docs frame it as a checklist the deployment writes, and the shipped set as
  a starting point.
- **Rule creep.** Every incident wants a new rule, and a hundred rules is a config nobody reads.
  Mitigation: the shipped set stays small and the app declares the rest.
- **Key paths drift with the settings model.** Answered by making an unresolvable path an error
  rather than a skip — a silence check that guesses is a silence check that approves.

## 10. Unresolved questions

- **Does the posture live in `forze.application.execution` or beside the settings module?** It reads
  settings and is evaluated by the runtime; the leaning is the runtime, with the value objects in
  `forze.base`.
- **Does `WiringReport` gain a `findings` channel, or do violations arrive as `WiringFailure`s?**
  (§5.2.) A new channel keeps `ok` and `raise_if_failed` meaning "every operation resolved"; reusing
  `WiringFailure` needs an operation name a declaration-time finding does not have. Answered once
  for all four reporters, not per RFC.
- **Is there a `forze` CLI entry (`forze check-posture`)?** Useful in a deploy pipeline before the
  app starts; adds a CLI surface for a check the app can already run.
- **How does a rule exempt a field** (the service-mesh case)? An exemption list per rule, or a
  narrower rule? Leaning: an exemption naming the field and requiring a reason string, so it reads
  as reviewed.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | An unset or empty environment is **production**. Silence must read as the strict case, or the check is absent exactly where it matters. |
| 2 | `LOCKED` | Violations name **keys and rule names, never values**. A boot refusal is logged, exported and pasted into tickets; echoing the offending DSN puts a password in all three. |
| 3 | `LOCKED` | The framework owns no settings class. The posture declares rules over paths into the app's own model, because the settings doctrine says the root class and its env policy are the deployment's. |
| 4 | `LOCKED` | Evaluated at **both** `check_wiring` and `build_runtime`: the first is the early signal for apps that call it, the second is the floor for apps that do not. |
| 5 | `ASSUMED` | A rule whose path does not resolve is an **error**, not a skip. A posture referring to a renamed field is one nobody is enforcing. |
| 6 | `ASSUMED` | Existing per-config safety checks become posture-aware — warn without it, refuse under it — rather than each growing an opt-out flag. The cleartext-credentials check is the first adopter and its docstring is the argument. |
| 7 | `ASSUMED` | The framework distinguishes only production from not-production; the app's other environment names are its own. Enumerating environments would be wrong about the fourth one. |
| 8 | `OPEN` | Whether `WiringReport` gains a **findings channel** or declaration-time violations are synthesized as `WiringFailure`s. The report has no such channel today, and a violation is not an unresolved operation. This RFC owns the answer for all four reporters — posture, storage guarantees, config grants and the audit allowlist. **Decided by rows 10–11.** |
| 9 | `OPEN` | Where the posture module lands, whether a CLI entry point ships with it, and how a field is exempted from a rule (exemption list with a reason, or narrower rules). **Decided by row 12.** |
| 10 | `ASSUMED` | Decides row 8: `WiringReport` gains a `findings` channel of `WiringFinding(source, target, rule)`, carrying no value, and — departing from §5.2's leaning — a finding makes `ok` false and `raise_if_failed` raise: a finding in the channel is a refusal (non-production findings never enter it), and a CI calling `raise_if_failed` would otherwise pass a deployment the runtime then refuses — see `logs/T-0061.md:29` (D-8, attempt 1). |
| 11 | `ASSUMED` | Narrows row 10 and row 8's "for all four reporters": a refusal that belongs to no operation (a posture rule, a module's reported finding) goes to `findings`; a storage-guarantee mismatch surfaces while an operation's spec is resolved and stays a `WiringFailure` naming that operation, as [RFC 0066](0066-storage-guarantees.md) row 10 decided. Row 10 was built without citing that row — see `logs/T-0061.md:254` (D-8, attempt 2). |
| 12 | `ASSUMED` | Decides row 9: one module, `forze.application.execution.posture`, value objects and evaluation together — §10's split into `forze.base` buys nothing for an object only the runtime evaluates; no CLI (P3); exemptions as row 16 — see `logs/T-0061.md:41` (D-9, attempt 1). |
| 13 | `ASSUMED` | The posture **carries** the application's settings object — `ProductionPosture(settings=…, environment=…, rules=…)` — one object handed to both gates, so the two cannot judge different settings. §5 never says how settings reach `check_wiring` or `build_runtime` — see `logs/T-0061.md:53` (unlisted, attempt 1). |
| 14 | `ASSUMED` | Departs from §5.1's `env_default` parameter to keep row 1: there is no default knob — an unset or empty environment is production, always. The application declares its `non_production` names, and every other value (`prod`, a typo, another case) is production — see `logs/T-0061.md:101` (D-1, attempt 1). |
| 15 | `ASSUMED` | Departs from §5.1's shipped default set, which mostly names fields only the application knows: the application points named rule kinds (`RequireSet`, `RequireHttps`, `LoopbackHost`, `DevValue`) at its own paths, and one path-free rule ships on by default — a `_dev_only` marker anywhere in the settings, secrets included. "A missing secret where the auth method needs one" is `RequireSet` over the application's field — see `logs/T-0061.md:77` (unlisted, attempt 1). |
| 16 | `ASSUMED` | Departs from §12, which gates exemptions to P3: `Exempt(target, reason)` ships now with a required non-blank reason, matching a settings path or a module finding's target, because the cleartext check's service-mesh case needs one the moment it can refuse — see `logs/T-0061.md:89` (unlisted, attempt 1). |
| 17 | `ASSUMED` | Elaborates row 5: an unresolvable path is reported in **every** environment, development included, and cannot be exempted — a declaration defect a laptop should find; only the value rules are inert outside production — see `logs/T-0061.md:113` (D-5, attempt 1). |
| 18 | `ASSUMED` | Elaborates row 6: a deps module implementing `PostureAware` reports its configs' safety findings, collected by `DepsRegistry.freeze()` onto the frozen registry both gates read — see `logs/T-0061.md:65` (D-6, attempt 1). |
| 19 | `ASSUMED` | Narrows row 6: only checks decidable at wiring escalate. A tenant-routed HTTP service learns each tenant's `base_url` from that tenant's secret after boot, so its cleartext check stays a per-tenant warning; escalating it would hand the posture to the routed client when a tenant's client is built — a runtime guard §4 excludes. Demand-gated — see `logs/T-0061.md:227` (D-6, attempt 1). |
| 20 | `ASSUMED` | `RequireHttps`, `LoopbackHost` and `DevValue` over a group (a nested model or a mapping) read every value under it; `RequireSet` over a group refuses only an unset or empty group — see `logs/T-0061.md:205` (unlisted, attempt 2). |
| 21 | `ASSUMED` | A URL-shaped value the parser refuses is judged, never raised — the parser's error can quote part of the value, against row 2: it is not https and names no loopback host — see `logs/T-0061.md:184` (unlisted, attempt 1). |

## 12. Phasing

- **P1** — the posture value objects, the four rule kinds, evaluation at both gates, key-only
  errors, batteries including the no-values leg.
- **P2** — the shipped default rule set and the posture-aware escalation of the cleartext check.
- **P3** *(demand-gated)* — a CLI entry point, exemptions with reasons, further rules as incidents
  name them.
