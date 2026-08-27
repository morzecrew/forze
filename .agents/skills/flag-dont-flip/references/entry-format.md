# The entry, field by field

Every field, what it must carry, and the two rules about the file it goes in.
`SKILL.md` carries the grades, the plan gate and the classes; this is the shape
of what you write when one of them fires.

## Write the entry before you act

Not after. An entry written afterwards is a rationalisation of a decision
already taken, and reads like one. Deviations reconstructed at the end are
reconstructed *from the code*, which means they describe what was built rather
than what was decided.

Append to `logs/<task-id>.md`. **One file per task** — never a shared log, which
is a write hotspot the moment two workers run at once. Append-only: never edit
or delete an existing entry, including your own from an earlier attempt. If you
were wrong, append a new entry saying so.

````markdown
**Drift count: 1.**

```divergence
decision: D-3
grade: LOCKED
class: spec-gap
at: 2026-08-20T11:04:12Z
attempt: 2
claim: sessions cannot live in Redis; this deployment has no Redis service
evidence: infra/compose.yaml:1-40
action: halted
proposal: LOCKED — sessions live in Postgres until a Redis service is provisioned
```
````

| Field | Rule |
|---|---|
| `decision` | The identifier from the spec's decision table, or `unlisted`. Never invent one. |
| `grade` | Copied **from the task as it stands now**. Do not look it up in the current spec — the grade may have moved since, and the log records what was in force when you acted. |
| `class` | `discovery` · `spec-gap` · `drift` · `irreducible`. See below. One of `class` / `kind` must be present; a divergence normally carries `class`. |
| `kind` | `contradicted` · `departed` · `resolved` · `blocked` — what happened to the decision, where `class` says what it reveals about the design process. The axes are orthogonal; carry both when both are known — except on a close-out, where `class` is refused. `resolved` and `blocked` change what `action` is legal — see the close-out section below. |
| `at`, `attempt` | UTC RFC 3339, and which attempt at this task this is. |
| `claim` | One sentence: what reality says that the decision does not. Not what you did about it. |
| `evidence` | `path:line`, `path:start-end`, or a backticked command with its output. Someone else must be able to locate it. |
| `action` | `halted` · `departed` · `decided`. Must be legal for the grade. |
| `proposal` | The row this proposes back to the spec, written as it would appear. Required when `decision: unlisted`. |

Prose outside the blocks is allowed and ignored by the checker.

**Rationale is a mechanism, not a preference.** "Cleaner" and "more idiomatic"
are not reasons; they are the sound of drift being written down as discovery.

## Evidence must be checkable

The checker locates it. An entry whose evidence cannot be found is discarded,
and a discarded entry counts as none.

Evidence: `infra/compose.yaml:1-40` · `` `pnpm test tests/api/auth.spec.ts` — 3 failed, ECONNREFUSED 127.0.0.1:6379 ``

Not evidence: "Redis isn't available in this environment" · "the current
architecture makes this impractical". Those are claims, and `claim` is where
claims go.

## Never amend the spec from inside a task

Do not edit the design document's prose, decision table, or grades while
executing against it. That launders the flip and destroys the record that a
decision changed at all. Your entry **is** the amendment proposal: someone reads
the log and updates the spec under fresh review. Acceptance and refusal are
recorded there, by the author, not back in the append-only log.

Departures that change a **contract** — error kinds, retry semantics, ordering
guarantees, public surface — are logged even when they look like implementation
detail. If another implementation of the same port would now behave differently,
the shared conformance battery is re-run rather than assumed still valid
(`reading-isnt-proof`).

## Close-outs: compliance in a touched LOCKED area

The silence check demands an entry whenever a diff touches an area a `LOCKED`
decision declares — it cannot tell honored-quietly from worked-around-quietly,
so both owe a report. When the work complies, the report is a **close-out**:
`kind: resolved`, `action: decided`, no `class` (nothing diverged, so there is
nothing to classify), `claim` stating the decision was honored, `evidence`
locating the compliant implementation.

````markdown
```divergence
decision: D-3
grade: LOCKED
kind: resolved
at: 2026-08-21T13:00:00Z
attempt: 1
claim: touched the governed area; sessions stayed in Redis as decided
evidence: src/session/store.py:12-40
action: decided
```
````

`class` on a close-out is refused, not merely discouraged. `class` classifies
a departure and a close-out is not one, and left legal the pair is a route
around the grade table: `resolved` skips it, so `class: drift` would record a
contradiction and take the attesting exemption in the same entry.

Legality with `kind` present: `resolved` licenses `decided` or `departed` (the
grade table does not apply — a close-out attests, it does not contradict);
`blocked` licenses only `halted`; `contradicted` and `departed` follow the
grade table unchanged. `kind: resolved` with `action: decided` is also the
shape for recording that an `OPEN` question got settled.

## Recording a departure you did not make

Where the design was followed **and the alternative is worth naming**, say so in
prose beside the entries — the checker reads only the fenced blocks and ignores
the rest:

> **Deliberately not applied:** RFC 0014 §5.3 sketches a write-through cache in
> front of the session store. Not built: the store is already behind the
> repository seam, so the sketch would add a second cache with no measured
> pressure. The existing code stands.

An illustrative sketch you chose not to implement is a decision. Without the
note, the reader who finds the code disagreeing with the sketch cannot tell
whether it was seen and declined or simply missed, which is the same ambiguity
an unlogged departure creates.
