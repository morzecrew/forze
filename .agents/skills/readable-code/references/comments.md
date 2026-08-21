# Comments

What to refactor away, and what no rewrite can express.

The test for any comment: **does it say something at the same level of
abstraction as the code next to it?** If yes, it duplicates the code — refactor
so the code says it, then delete the comment. If it operates at a *different*
level — more precise (units, ranges, invariants) or more abstract (intent,
contract, rationale) — it carries information code cannot, and it stays.

This is not "never comment." Ousterhout's *A Philosophy of Software Design* is
the sharp counterpoint to comment-minimalism: "comments should describe things
that aren't obvious from the code" — and for interfaces, plenty isn't. The
skill is telling redundant comments from load-bearing ones.

## Refactor the "what" into the code

### 1. Name sub-expressions

A condition that needs a comment to decode can be rewritten to read like the
comment:

```python
# user can check out only if active, cart non-empty, not banned
if user.active and len(cart.items) > 0 and not user.banned: ...
```

```python
has_items = len(cart.items) > 0
can_check_out = user.active and has_items and not user.banned
if can_check_out: ...
```

### 2. Extract a predicate

When the expression is large or reused, move it into a function whose name is
the comment. The call site becomes plain intent, and the name — unlike the
comment — can't be skipped by the next editor:

```python
def can_check_out(user, cart):
    return user.active and len(cart.items) > 0 and not user.banned
```

### 3. Name magic values

```python
if attempts > 5: ...          # why 5? (comment coming...)

MAX_LOGIN_ATTEMPTS = 5
if attempts > MAX_LOGIN_ATTEMPTS: ...
```

### 4. Let types state the fact

Nullability, ownership, allowed values, units — encode them in the type
(`Optional[User]`, an enum, `Duration`) and the checker keeps the claim honest.
A comment asserting the same thing is unenforced. (Unit-bearing names:
see `naming-things`.)

## Why redundant comments are worse than none

Nothing checks a comment, so it drifts: the code changes, the comment doesn't,
and now it actively misleads a reader who trusted it. A comment that repeats
the adjacent code adds no information today and a maintenance liability
forever. Ousterhout's first red flag is exactly this: *comment repeats code* —
if a reader could write the comment from the code alone, delete it.

## The comments code cannot replace

Two directions, per Ousterhout — both are legitimately about "what," and both
survive the refactor-first test because no rewrite of the code can express
them:

**More precise than the code** — pin down what the declaration leaves open:

```python
retry_interval = 200   # what does 200 mean? is it fixed?

# Milliseconds. Doubles per attempt, capped at 30_000.
# Invariant: > 0; 0 would spin-loop the scheduler.
retry_interval_ms = 200
```

Units (when the type can't carry them), inclusive vs exclusive bounds, null
semantics, ownership and lifetime, invariants, thread-safety expectations.

**More abstract than the code** — interface comments. A caller should
understand what a function does, its parameters' meaning, side effects, and
error behavior *without reading its body*. That summary is "what"
documentation and it is required for any non-trivial public interface — this
is where "comments only explain why" tips from principle into dogma. Keep
interface comments free of implementation detail (Ousterhout's second red
flag: implementation contaminating the interface); the docstring skills cover
format.

**Why-comments** — always code-inexpressible:

- Rationale and trade-offs: why this approach over the obvious one.
- Performance oddities: code that looks wrong because it's tuned; say so, with the measurement.
- Workarounds: "sidesteps bug X in library Y", with a link.
- Cross-module obligations: "if you change this, update Z."

## Comment-first as a design probe

When you do write an interface comment, try writing it *before* the body. If
the comment comes out long, conditional, or hedged ("does X, but also Y if
flag Z..."), the abstraction is muddy — fix the interface, not the wording.
A clean one-sentence comment is evidence of a clean abstraction.

## Decision table

| Comment says... | Verdict |
| --- | --- |
| What the next line visibly does | Delete; refactor code if it wasn't visible |
| What a complex condition means | Extract predicate / name sub-expressions |
| What a literal means | Named constant |
| A fact a type could enforce | Move into the type |
| Units, ranges, invariants, null/ownership semantics | Keep — precision the code lacks |
| What a public function does for callers | Keep — interface contract |
| Why this design / workaround / tuning | Keep — rationale |
| Corrects a misleading name | Fix the name, then delete |
