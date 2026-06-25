---
name: self-documenting-code
description: Make code explain itself instead of relying on comments - name sub-expressions, extract complex conditions into well-named functions, lean on the type system, and reserve comments for the "why" the code cannot express. Use when writing or reviewing comments, refactoring hard-to-read logic, deciding whether a comment is needed, or when the user mentions comments, self-documenting code, or code readability.
---

# Self-Documenting Code

Prefer code that explains itself over comments that explain the code. The guiding
test: **if a piece of code is complex enough to need a comment, first try to
refactor the code so the comment becomes unnecessary.** Most "what" comments are a
missed opportunity to make the code itself clearer.

This is not "never write comments." It's a priority order — make the code human
first, and keep the comments that carry information the code genuinely cannot.

## Use this skill when

- Writing or reviewing comments in a diff.
- A comment is needed to explain *what* a line or condition does.
- Refactoring dense logic that is hard to follow.
- The user mentions comments, self-documenting code, or readability.

## Do not use this skill when

- The user explicitly wants explanatory/teaching comments (tutorials, examples, learning codebases).
- A comment documents *why* (rationale, trade-off, workaround) — those are valuable; keep them.

## Techniques to make code self-documenting

### 1. Name sub-expressions with variables

A condition that needs a comment to decode can usually be rewritten so it reads
like that comment.

```python
# user can check out only if active and cart isn't empty and not banned
if user.active and len(cart.items) > 0 and not user.banned:
    ...
```

```python
is_active = user.active
has_items = len(cart.items) > 0
can_check_out = is_active and has_items and not user.banned
if can_check_out:
    ...
```

### 2. Extract a complex condition into a function

When the expression is large or reused, give it a name by moving it into a
predicate function. The call site now reads as plain intent.

```python
def can_check_out(user, cart):
    return user.active and len(cart.items) > 0 and not user.banned

if can_check_out(user, cart):
    ...
```

### 3. Let types carry the information

A type can state a fact a comment would otherwise assert — ownership, nullability,
units, allowed values. Encode it in the type and the reader can trust it, because
unlike a comment the compiler/checker keeps it honest. (For unit-bearing values,
also put the unit in the name — see `naming-things`.)

## Why lean away from comments

Comments are not checked by anything, so they drift: people change the code and
forget the comment, and now it actively misleads. **Comments can lie; code cannot.**
A "what" comment duplicates the code and creates a second thing to keep in sync.

## Comments vs. documentation

These are different and the distinction matters:

- **Comments** describe the *internals* — how a block works. These are the ones to
  minimize by improving the code.
- **Documentation** describes the *external usage* of a public API for its callers.
  This is worth writing — and the better and simpler your API, the more concise and
  accurate the docs can be. (See the docstring skills for format.)

## When a comment IS the right tool

Some intent cannot be expressed in code, and there a comment earns its place:

- **Why, not what** — the rationale, constraint, or trade-off behind a decision.
- **Non-obvious performance** — code that looks strange because it's tuned for speed; explain why it looks that way.
- **Workarounds** — "this odd line works around bug X in library Y", ideally with a link.

The rule of thumb: if you reach for human language to describe *what* the code does,
try to make the code more human instead; if you reach for it to capture *why*, write
the comment.

## Quick checklist

- Does a comment restate what the next line does? Try to delete it by clarifying the code.
- Is there a commented complex condition? Name its parts or extract a predicate.
- Could a type state the fact the comment asserts? Encode it in the type.
- Does the comment explain *why* / a workaround / a perf hack? Keep it.
- Is it public API usage? Prefer real documentation over an inline comment.

## Related skills

- `naming-things` — clear names are the foundation that makes comments redundant.
- `never-nesting` — extracting named functions both flattens code and documents intent.
- `python-rest-docstrings` / `python-google-docstrings` — how to write the API documentation this skill says to keep.
