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
