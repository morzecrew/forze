# T-fields-migration-hint — `$fields` misuse should name the migration, not the symptom

Executing the maintainer's specification (message-only; same kind, same control flow, no new
API) on `fix/fields-migration-hint`. The specification carries no graded decision table, so its
decisions are treated as `LOCKED`.

## Plan

- `querying/types.py`: `ALL_VALUE_OPS` lifted beside the `Op` literals it is derived from;
  `capabilities.py` re-exports it, so every existing import keeps working.
- `querying/internal/parse.py`: one shared suffix constant, appended by the three `$fields`
  refusals — a value operator used as a compare operator (a new message), a non-string
  right-hand side, and an invalid map value (existing messages plus the suffix).
- Tests in `test_query_internal.py`: every value-only operator names `$values`; every compare
  operator with a non-string right-hand side names `$values`; an unknown operator keeps the plain
  message; a valid compare and the shortcut still parse unchanged.
- Release note in `[Unreleased]`: the migration pointer, and the shapes that do not raise at
  parse.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T08:50:00Z
attempt: 1
claim: the specification says a parse → capabilities import introduces no cycle; it does not today only because `querying/__init__` imports `builder` (which loads `internal.parse`) before `capabilities` — `internal/__init__` imports `parse`, so were `capabilities` ever loaded first, `parse` would meet it half-initialized, before `ALL_VALUE_OPS` is bound
evidence: src/forze/application/contracts/querying/internal/__init__.py:24
action: decided
proposal: ASSUMED — take the specification's alternative: `ALL_VALUE_OPS` is derived in `types.py` from the literals it names (the `Op` set without the separately gated hierarchy operators), and `capabilities` re-exports it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T08:50:00Z
attempt: 1
claim: the non-string right-hand-side check shares its branch with an empty or blank string; the specification asks for the pointer on a non-string right-hand side, and an empty string is as likely a mistyped field path as pre-0.7 code
evidence: src/forze/application/contracts/querying/internal/parse.py:309
action: decided
proposal: ASSUMED — the pointer fires on a non-string right-hand side only; an empty or blank string keeps the plain message, so the pointer stays specific.
```

```divergence
decision: unlisted
grade: LOCKED
class: spec-gap
at: 2026-09-27T08:50:00Z
attempt: 1
claim: "row 4 gets no message, because no message is possible" holds only when the string names a real field. A `$fields` right-hand side is validated as a field path against the read model, so `{"status": "archived"}` — and a `StrEnum` value, which is a `str` on both the shortcut and the right-hand side — parses and is then refused as `field_not_on_read_model` ("Filter field(s) ['archived'] are not on the read model"), a fifth outcome the specification's table omits, naming the symptom and never the migration. A string enum is the common case of the evidence's "enum"
evidence: src/forze/application/contracts/querying/field_policy.py:69-71
action: halted
proposal: LOCKED — a message is possible there: when an unknown root is the right-hand side of a `$fields` compare, `validate_runtime_filter_fields` appends the same suffix. Message-only (same kind, same `field_not_on_read_model` code, same control flow), one site every backend's filter validation routes through. Needs the maintainer's decision because it widens the change past the three sites the specification names.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-09-27T08:50:00Z
attempt: 1
claim: the specification leaves the codemod / `forze.testing` reflection gate for sizing first; its one data point (0 of 59 row-4 entries in erp-backend) says the release note carries most of the value, and the halted entry above would cover the string values that name no field
evidence: src/forze/application/contracts/querying/guards.py:113-118
action: decided
proposal: not built — the release note names the silent shape; the gate stays a follow-up if a second data point shows row-4 entries in the wild.
```

**Drift count: 0**

```divergence
decision: message-only
grade: LOCKED
kind: resolved
at: 2026-09-27T09:40:00Z
attempt: 1
claim: every refusal keeps its kind (`precondition`), its code (`core.precondition`) and its place in the control flow; only the text changes, and a valid compare parses to the same node
evidence: src/forze/application/contracts/querying/internal/parse.py:316
action: decided
```

## Self-audit — 2026-09-27

Scope: the branch, 1 commit, 6 files (+121 −33: 33 source, 84 test, 8 docs/changelog).

No findings beyond the decisions logged before code. Checked:

- **Sabotage, 6 mutants, all killed by an assertion:** the value-operator pointer removed; the
  pointer firing on any unknown operator (caught by the typo leg — the specificity the
  specification asked to pin); the literal-right-hand-side pointer removed; the pointer firing on
  a blank string; the map-value pointer removed; the `ALL_VALUE_OPS` derivation dropping a
  literal family. The last one matters because the value-operator leg takes its cases from
  `ALL_VALUE_OPS` itself and would shrink with it; the mock's DSL parity corpus kills it
  independently, once `$like` leaves the default capabilities.
- **Patch coverage:** 100% (10/10 added source lines).
- **Spec conformance:** message 1 is the specification's text verbatim; 2 and 3 are the existing
  text plus the one shared suffix; the four required legs are present, the typo leg asserting the
  whole message.
- `_validate_fields_op` has no caller but the `$fields` parser, and `$having` parses through
  the same code, so the pointer never lands outside a `$fields` refusal.

Residue: the halted entry (a string right-hand side naming no field is refused by field
validation with no pointer) waits on the maintainer. The query-syntax page gained a two-sentence
note for readers migrating from before 0.7 — an extra the specification did not ask for.

**Drift count: 0**

## Maintainer decision — 2026-09-27

```divergence
decision: unlisted
grade: LOCKED
kind: resolved
at: 2026-09-27T10:05:00Z
attempt: 2
claim: the maintainer accepted the halted proposal — a string right-hand side of a `$fields` compare that names no field on the read model is refused by field validation with the same suffix, after a clause naming it as a field where a value was meant; any other unknown field (a `$values` key, a `$fields` left-hand side) keeps the plain message
evidence: src/forze/application/contracts/querying/field_policy.py:161
action: decided
proposal: LOCKED — a `$fields` right-hand side naming no field carries the migration suffix at field validation; the one silent shape left is a string that names a real field.
```

**Drift count: 0**

## Self-audit, round 2 — 2026-09-27

Scope: the field-validation addition (`e79fda517`). No findings.

- **Sabotage, 4 mutants, all killed:** the pointer removed; the pointer on every unknown field
  (caught by the `$values`-key and left-hand-side legs); the right-hand side not tracked; the
  left-hand side tracked in its place.
- **Wired, not only callable:** a leg through the mock document port carries the pointer, and
  fails without the change.
- **Patch coverage:** 100% (19/19 added source lines across the branch). `just quality -s`: all
  19 gates.

**Drift count: 0**
