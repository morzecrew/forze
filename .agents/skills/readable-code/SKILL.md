---
name: readable-code
description: Use when naming or renaming anything, when code is deeply nested or arrow-shaped, when a comment restates what the code says, or when reviewing a diff for readability. Not when a name or a structure is dictated by an external contract or an ecosystem convention.
roles: [implement, review]
gate: none
gate_reason: a linter enforces casing and depth; whether a name means what it says, and whether a comment earns its place, is a read
---

# Readable Code

Three rules, one each for the three things a reader has to decode. They fire
together on almost every review, which is why they live in one place.

- **A name's length scales with its scope, and it must not lie.** `i` in a
  three-line loop is fine; an exported name carries full meaning on its own. A
  `get_user()` that creates the user is worse than a vague name, because the
  reader trusts it.
- **Each level of indentation is one more condition the reader holds as true.**
  Cost compounds rather than adding up: four guard clauses cost 4, the same
  logic nested costs 10. Three levels is the soft ceiling.
- **A comment at the same level of abstraction as the code duplicates it.**
  Refactor until the code says it, then delete the comment. A comment that is
  *more precise* (units, bounds, invariants) or *more abstract* (intent,
  contract, rationale) carries what code cannot, and stays.

## Where to look

| What you are seeing | Read |
|---|---|
| An identifier that reads unclear, abbreviated, generic, or misleading | [references/naming.md](references/naming.md) |
| `Utils`, `Manager`, `Data`, `Base`, numbered variants, near-twin names | [references/naming.md](references/naming.md) |
| A missing unit on a number, or a negated boolean | [references/naming.md](references/naming.md) |
| Arrow-shaped `if`/`for`/`try`, a happy path several indents deep | [references/nesting.md](references/nesting.md) |
| A `try`/`catch` around every individual call | [references/nesting.md](references/nesting.md) |
| A nested `if`/`else` chain selecting among cases | [references/nesting.md](references/nesting.md) |
| A comment restating the line under it | [references/comments.md](references/comments.md) |
| A comment decoding a condition or a magic number | [references/comments.md](references/comments.md) |
| A public function with no caller-level summary | [references/comments.md](references/comments.md) |

## Where they meet

The three rules constrain each other, and the interaction is where most of the
value is:

- **Extraction needs a name.** A block you cannot name is not a coherent unit yet, so it is not ready to extract — flatten it with a guard clause instead.
- **A precise name is what makes a "what" comment deletable.** Reach for the name first; a comment kept because the name is bad is a fix in the wrong place.
- **A name that will not come is design feedback.** A function you can only name `process_and_update` is telling you it does two things; a class you can only name `DataManager` is telling you its responsibility has not been decided. Neither is proof on its own — it is the signal that sends you to look at the structure rather than the vocabulary.
- **A misleading comment is fixed by fixing the name, then deleting the comment.** Correcting a bad name in prose leaves both.

## Checklist

- Would this name still be clear at its farthest point of use?
- Does every boolean read as a positive true/false claim at the `if` site?
- Does every unit-bearing number carry its unit in its name or its type?
- Does the name promise exactly what the code does — no hidden creation, mutation, or I/O?
- Any function past ~3 levels? Can edge cases become guards?
- Is any branch about to become a guard actually normal behavior? Then keep `if`/`else`.
- Does an early return skip manual cleanup? Use the language's cleanup idiom first.
- Could a reader reconstruct this comment from the code alone? Delete it — refactoring first if they could not.
- Does a declaration leave units, bounds, or invariants open? That comment stays.

## Related skills

- `less-code-same-behavior` — flattening often reveals duplicate branches worth merging. Absent it, still note the duplicates rather than flattening around them.
- `python-google-docstrings` / `python-rest-docstrings` — formats for the interface documentation this skill says to keep.
