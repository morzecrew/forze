# RFC 0062 — Log redaction reach

- **Status:** ✅ Complete — **shipped 2026-11-03**: when `logger_names` narrows capture, `configure_logging` (and so `bootstrap_logging`) adds a scrubbing root handler at `warning`, or the configured level if higher, unless `sanitize_logs=False`; `redact_unnamed=False` declines it with a warning; the §6 battery and the docs sentences. The three readiness-gate decisions and both §10 questions were settled in execution and proposed back in [`logs/T-0062.md`](../logs/T-0062.md); since 2026-11-04, §11 records each as a decision row (rows 6–14) — read it before trusting §2's uvicorn case or §5.1's "at the same level". The prose below is left as designed.
- **Scope:** Keep redaction attached when an application narrows `logger_names`, and make the narrowing say so. Touches `forze.base.logging.configure` only — a handler-attachment rule plus a warning. **No new processor, no new policy, no scrubbing change.**
- **Related:** [`src/forze/base/logging/configure.py:209-241`](../src/forze/base/logging/configure.py) (`configure_logging`, `sanitize_logs=True` and `text_scrub=True` by default; the `logger_names` allowlist and its "when omitted or empty, the **root** logger is configured" rule at `:302`), `:318` (`attach_foreign_loggers`, which carries the same two flags), `:159` (`build_foreign_formatter`), [`src/forze/base/logging/processors.py:111-270`](../src/forze/base/logging/processors.py) (the scrub applied to extras **and** to `event_dict["event"]`), [`src/forze/base/scrubbing/policy.py:491`](../src/forze/base/scrubbing/policy.py) (`register_sensitive_patterns`, the extension point), [`examples/recipes/agent_tools_dst/app.py:69`](../examples/recipes/agent_tools_dst/app.py) (a narrowing call, in the framework's own examples), [RFC 0061](0061-production-posture.md) (where a "narrowed logging in production" rule would go).
- **Origin:** A working-time ledger with a stdlib `logging.Filter` on every root handler scrubbing bearer tokens, cookies, CSRF headers, passwords in connection strings and anything named token, secret or key — from both the message and the extras, including uvicorn's own access and error loggers, whose format strings the application does not control.

---

## 1. Summary

Forze already redacts by key and by pattern, in extras and in the message, on by default, for
every logger that reaches its handlers. The one hole is the `logger_names` allowlist: an app that
narrows capture to its own loggers leaves every third-party logger — uvicorn's access log among
them — attached to whatever handler Python gives it, unredacted. This RFC keeps a redacting
handler on the root in that case, and warns when an app declines it.

## 2. Motivation

The difference between a clean incident and a credential in the log pipeline is usually one
logger nobody configured. Uvicorn's access logger formats its own messages; a token in a query
string lands there whole, and no amount of care in application logging touches it.

The narrowing call is not hypothetical: the framework's own recipes call
`configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])`. That is the documented,
supported way to keep a noisy dependency quiet — and it silently opts the process out of the
redaction it just enabled for everything else.

## 3. Current state

**Redaction is shipped and on by default.** `configure_logging(sanitize_logs=True,
text_scrub=True)` installs processors that scrub sensitive keys from the event dict and, at
[`processors.py:270`](../src/forze/base/logging/processors.py), apply `scrub_log_string` to
`event_dict["event"]` — the message itself, not just the extras. `register_sensitive_patterns`
is the extension point the source proposal asked for. So "add a redaction processor with an
extension point" is **already done**, and this RFC does not redo it.

**Foreign loggers are covered — when attached.** `attach_foreign_loggers(names, …)` builds a
`ProcessorFormatter` through `build_foreign_formatter` with the same two flags, so uvicorn,
SQLAlchemy and friends render through the same scrub. `configure_core_logging` attaches the names
an app passes.

**The default is safe; the narrowing is not.** At
[`configure.py:302`](../src/forze/base/logging/configure.py): `names = _cast_logger_names(logger_names)
if logger_names else [""]` — omit the argument and the **root** logger gets the redacting handler,
so every propagating third-party logger is covered. Pass a list, and only those names are, which
is the gap.

**Nothing warns.** An app that narrows gets exactly what it asked for, with no signal that it also
gave up redaction of everything unnamed.

## 4. Goals / Non-goals

**Goals**

- Narrowing capture must not narrow redaction.
- An app that deliberately wants neither says so, once, and is warned rather than surprised.
- No behaviour change for the default path, which is already correct.

**Non-goals**

- **Not a new processor.** §3.
- **Not new patterns.** The base set and `register_sensitive_patterns` stay as they are.
- **Not a stdlib `logging.Filter`** like the origin's. Forze renders foreign loggers through a
  structlog `ProcessorFormatter`, which already runs the scrub; adding a filter would be a second
  mechanism for the same job.
- **Not redaction of what the framework does not see.** A dependency writing to stdout directly is
  outside every logging configuration, and this RFC does not pretend otherwise.

## 5. Design

### 5.1 The rule

When `logger_names` is non-empty, `configure_logging` additionally attaches a **redacting** handler
to the root logger, at the same level, unless the caller passes `redact_unnamed=False`.

Named handlers keep their formatter and level as they are; the root gains the scrubbing formatter
so anything not explicitly named still renders through it. Narrowing then means what an app
expects it to mean — "I do not want this logger's noise in my format" — without meaning "and I
accept unredacted output from everything I did not list".

### 5.2 The refusal path

`redact_unnamed=False` is the opt-out, and it warns once at configure time, naming the effect:
loggers outside the allowlist write through Python's default handling, unscrubbed. An app with a
deliberate custom handler chain wants this; it should be a line in its own code, not an accident.

Not a refusal: logging configuration must not fail a process. The posture in
[RFC 0061](0061-production-posture.md) is where "narrowed redaction in production" could become a
boot error if a deployment wants that, and this RFC's warning is what that rule would read.

### 5.3 Double-emission

A named logger that propagates would now render twice — once through its own handler, once through
the root's. The rule is therefore paired with the existing `propagate` handling:
`attach_foreign_loggers` already sets `propagate=False` by default, and the narrowing path does the
same for the names it attaches. This is the part that has to be tested rather than reasoned about
(§6), because it is behaviour, not intent.

### Alternatives considered

- **Refuse to narrow at all.** Removes a supported use case, and the framework's own recipes use
  it.
- **A stdlib filter on the root, as the origin does.** A second redaction mechanism with its own
  pattern set to drift from the first.
- **Make `sanitize_logs` imply root attachment silently.** No opt-out, and a surprising handler
  appearing on the root of a process that configured its own chain.
- **Leave it and document it.** The cheapest option, and it relies on every reader of the
  `logger_names` docstring inferring the consequence. The docstring already explains the mechanism
  correctly and the framework's own example still narrows.

## 6. Tests

- Default path unchanged: with `logger_names` omitted, root is configured and a token in a foreign
  logger's message is scrubbed (the regression leg — this is the behaviour that already works).
- Narrowed path: with `logger_names=["app"]`, a token logged through an **unnamed** logger is
  scrubbed after this change and is not before it. The "is not before it" half is what makes the
  test a proof rather than a restatement.
- Opt-out: `redact_unnamed=False` leaves the unnamed logger unscrubbed and emits exactly one
  warning naming the effect.
- No double emission: a named logger's record appears once.
- Message *and* extras: both legs, on the unnamed path, since the value of the change is the
  message the app does not format.
- **Not tested:** a dependency writing to stdout outside `logging`. Out of scope (§4).

## 7. Docs

Two sentences in the logging page, at the `logger_names` parameter: narrowing capture does not
narrow redaction, and `redact_unnamed=False` is how to decline that — with the consequence stated.
No new section; this is a parameter's semantics, not a feature.

## 8. Out of scope

- **A posture rule** forbidding `redact_unnamed=False` in production. It belongs to
  [RFC 0061](0061-production-posture.md), and this RFC's warning is the signal it would read.
- **Auditing what was redacted.** A count of scrubbed values per process would be interesting and
  is a metric, not a redaction feature.
- **Structured redaction of URLs** (scrubbing a query parameter while keeping the path). The
  pattern set handles the token; a URL-aware scrubber is a separate change with its own false
  positives.

## 9. Risks

- **A handler appearing on the root surprises an app with its own chain.** Mitigation: the opt-out,
  and the warning that names it.
- **Double emission if the propagate pairing is wrong.** It is the one real regression this change
  can cause, which is why §6 asserts single emission rather than trusting §5.3.
- **Reading this as "logs are now safe".** They are not: a dependency that prints, a crash dump, a
  traceback with locals — all outside this. Mitigation: the docs keep the claim to the mechanism.

## 10. Unresolved questions

- **Does the root handler in the narrowed case use the app's render mode or always JSON?** Matching
  the app's mode is consistent; JSON is what a collector wants for output the app did not plan for.
- **Should `attach_foreign_loggers` grow the same warning** when called with `sanitize_logs=False`?
  It is the symmetric hole, and it is explicit at the call site, which is weaker grounds for a
  warning.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The source proposal's redaction processor is **not built**: it ships, on by default, covering extras and the message, with `register_sensitive_patterns` as the extension point. This RFC is the residue, and the RFC says so rather than re-proposing a shipped feature. |
| 2 | `LOCKED` | Narrowing `logger_names` keeps a **redacting root handler** unless the caller declines it. Narrowing is about noise, and silently trading it for unredacted third-party output is not what any caller means. |
| 3 | `LOCKED` | Declining is a **warning, never a refusal**. Logging configuration must not fail a process; a production rule belongs to [RFC 0061](0061-production-posture.md). |
| 4 | `ASSUMED` | No stdlib `logging.Filter` is added. Foreign loggers already render through a structlog `ProcessorFormatter` that runs the scrub, and a second mechanism would drift from the first. |
| 5 | `ASSUMED` | The narrowed path pairs with `propagate=False` on the names it attaches, and single emission is asserted rather than assumed. |
| 6 | `ASSUMED` | The root handler in the narrowed case renders in the configured `render_mode`, and `attach_foreign_loggers(sanitize_logs=False)` stays silent, accepted by the maintainer on 2026-11-04 (§10; D-6, attempt 2). |
| 7 | `ASSUMED` | The narrowed root handler sits at WARNING or the configured level, whichever is higher, so an app configured above WARNING does not print an unlisted dependency's warnings it does not print for itself. It overrides the gate answer "at WARNING" recorded in [`logs/T-0062.md`](../logs/T-0062.md) (D-2, attempt 2). |
| 8 | `LOCKED` | Narrowing `logger_names` keeps a redacting root handler for every logger that propagates. A logger that sets `propagate=False` itself (`uvicorn.access`) is scrubbed only by naming it in `third_party` / `attach_foreign_loggers`, and the docs say so. (D-2, attempt 2: the maintainer's gate answer) |
| 9 | `ASSUMED` | The root handler is the one-call path's behaviour, not an exception to it: `bootstrap_logging` always narrows, so nearly every deployment gets it, a reach the maintainer accepted at the gate. The changelog marks it a behaviour change: an unlisted logger's warnings move from raw stderr to the rendered stream, or stop printing above WARNING (row 7). (unlisted, attempt 2) |
| 10 | `ASSUMED` | The narrowed path adds one root handler it owns and replaces only that one on reconfigure; a later `redact_unnamed=False` call removes it. It attaches beside handlers the app already put on the root, failing closed: a line printed twice beats one printed only raw. `logger_names` and `redact_unnamed` document the double lines, and an app that manages the root itself passes `redact_unnamed=False`. A call that names the root itself (`""` or `"root"`) gets no second handler. (unlisted, attempt 2, as amended in review) |
| 11 | `ASSUMED` | Declining raises a `UserWarning` per narrowed call, which Python's default filter shows once per call site. It is attributed to the first frame outside `forze.base.logging`, the application's own call, for `configure_logging` and `bootstrap_logging` alike. Under `sanitize_logs=False` there is no redaction to decline, so neither the root handler nor the warning appears. (unlisted, attempt 2, as amended in review) |
| 12 | `ASSUMED` | A stdlib record's `extra` stays unrendered on every path, pinned by a test, so the scrub is never asked to cover it. §6's extras leg is asserted on a structlog-origin logger outside the allowlist. (unlisted, attempt 2, as amended in review) |
| 13 | `ASSUMED` | The docs and the `third_party` docstring say to name both `uvicorn` and `uvicorn.access`: a parent's name does not cover a child that stops propagation, and `uvicorn.error` propagates only as far as `uvicorn`. (unlisted, attempt 2, as amended in review) |
| 14 | `ASSUMED` | A `logging.basicConfig()` after the narrowed path does nothing, since the root has a handler. It is documented, not worked around: call `basicConfig(force=True)`, or call it before `bootstrap_logging`. (unlisted, review round) |

Rows 7 and 9 to 14 were added by execution and accepted by the maintainer on 2026-11-04. Row 8
records the maintainer's own answer at the readiness gate, given before attempt 2. Each row cites
its entry in [`logs/T-0062.md`](../logs/T-0062.md), and the sections above are left as designed.

## 12. Phasing

One PR: the attachment rule, the flag, the warning, the four-leg battery, and the two doc
sentences.
