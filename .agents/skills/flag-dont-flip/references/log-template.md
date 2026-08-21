# The task log and the task file

Two skeletons: the log a task appends to, and the optional task file that lets
`scripts/log_check.py` run its silence check.

## `logs/<task-id>.md`

Created by the first entry, or by the task's completion — whichever comes first.
A task that departed from nothing still executed, and `Drift count: 0` is the
claim that says so. A log that only appears once something goes wrong cannot
tell a clean run from an unexamined one, which is the distinction it exists for.

The classes table is reproduced *inside* the log on purpose. The log is read by
people who are executing nothing — reviewers, the spec's author, whoever picks
the work up in six months — and a class name whose test lives in a skill file
they do not have is a label they cannot check.

````markdown
# T-0142 · Session storage

Executed RFC 0014 §5 against branch `feat/session-store`.

**Drift count: 0.**

Where building this disagreed with the design for it, written at the moment it
happened. Nothing here is revised afterwards to agree with what was later
settled, and nothing here has been folded back into the spec's own text. The
rows proposed below are put forward for the author to accept or refuse;
execution does not write them into a decision table itself.

| Class | Test | Meaning |
|---|---|---|
| `discovery` | Could not have been known before code existed | Healthy — the spec was right to be silent |
| `spec-gap` | Could have been known; the spec was silent or at the wrong altitude | The design process missed something |
| `drift` | The spec covered it and it was built otherwise anyway | **A defect** |
| `irreducible` | No amount of design settles it | Stop and spike |

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

Halted here. The alternative is a one-line config change and that is exactly
why it is not mine to make.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-08-20T11:31:02Z
attempt: 2
claim: nothing in the table says whether a session survives a password change
evidence: `rg -n "password" rfcs/0014-session-storage.md` — no match
action: decided
proposal: ASSUMED — a password change invalidates every session for that user
```
````

Notes that are easy to get wrong:

- **`Built:` becomes `Found:` in the `claim` when nothing was built.** Some departures are discoveries about what already existed — the spec said a module does not exist and it does.
- **Never delete, and never edit — including the drift count.** An entry that turns out to have been wrong gets a later entry saying so; a count that turns out to be wrong gets a new count line appended below the entry that changed it. The checker reads the last count in the file, so both stay readable:

  ```markdown
  **Drift count: 0.**
  ...entries...
  **Drift count: 1.** D-7 was drift against unit 1, found here.
  ```

- **`attempt` is what distinguishes a correction from a repeat.** Two entries citing one decision are a second look, not a duplicate.
- **`decision` cites the spec's identifier**, not a per-log number. Log entries have no identifiers of their own, so there is nothing to renumber and nothing for a citation to lose.
- **Prose between blocks is free.** The checker reads the fenced blocks and the drift count, and ignores everything else.

## `tasks/<task-id>.json`

Optional. Without it the silence check reports every decision as skipped, which
is not the same as passing.

```json
{
  "id": "T-0142",
  "decisions": [
    {"id": "D-3", "grade": "LOCKED", "paths": ["infra/**", "src/session/**"]},
    {"id": "D-4", "grade": "ASSUMED", "paths": ["src/db/**"]},
    {"id": "D-5", "grade": "OPEN"}
  ]
}
```

`paths` is what makes the check possible: it declares the area a decision
governs, so a diff touching that area with no matching entry is the silence
worth catching. `D-5` above has none and is reported as skipped — the checker
never guesses an area, because a guessed one produces both false silences and
false clean runs.

The same file is accepted as YAML, in a **restricted subset**: a top-level
mapping, scalar values, block lists, block lists of mappings, and inline flow
lists of scalars. Anything else — anchors, multi-document files, block scalars,
nested flow mappings — is refused with the line that caused it, rather than
parsed approximately. A task file the checker half-understood is a silence check
that passes for the wrong reason.

```yaml
id: T-0142
decisions:
  - id: D-3
    grade: LOCKED
    paths: ["infra/**", "src/session/**"]
  - id: D-4
    grade: ASSUMED
    paths:
      - src/db/**
```
