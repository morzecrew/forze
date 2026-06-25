---
name: never-nesting
description: Keep code flat and readable by limiting indentation depth, using guard clauses with early returns, and extracting nested blocks into well-named functions. Use when writing or refactoring code with deep nesting, pyramid-shaped if/else, arrow code, or when the user mentions nesting, indentation, guard clauses, early returns, extracting functions, cyclomatic complexity, or making code more readable.
---

# Never Nesting

A style for keeping code flat instead of letting it drift into ever-deeper inner
blocks. Each level of indentation forces the reader to hold one more condition in
their head at once, so reducing depth directly reduces the cognitive load of
reading a function.

The guideline (popularized by the Linux kernel coding style, which famously
warns that "if you need more than three levels of indentation, you're screwed
anyway and should fix your program") is to treat **three levels of indentation as
a soft ceiling**. When a function pushes past that, it is usually a signal to
restructure rather than to indent further.

There are two complementary techniques for flattening code: **inversion** (guard
clauses with early returns) and **extraction** (pulling a block into its own
function). Most messy functions improve by applying both.

## Use this skill when

- Writing a new function that is starting to grow nested `if`/`for`/`try` blocks.
- Refactoring "arrow code" or pyramid-shaped conditionals (`if { if { if { ... }}}`).
- The happy path is buried several indents deep while error/edge cases wrap around it.
- Reviewing code and flagging readability or complexity problems.
- The user mentions nesting, indentation, guard clauses, early returns, flattening, or extracting functions.

## Do not use this skill when

- The user explicitly wants the existing structure preserved.
- Flattening would fight a language idiom that genuinely reads better nested (see Nuance).

## Why depth hurts

Counting one level of indentation per open block, a plain function is 1 deep, one
`if` makes it 2, a loop inside that makes it 3. Every extra level adds another
condition the reader must keep simultaneously true in their mind to understand the
innermost line. Flattening lets the reader mentally discard each condition as they
pass it and focus on the core logic.

Flattening is not just cosmetic. The pressure to stay shallow naturally pushes you
toward small, single-responsibility functions instead of one large function that
does many things at once.

## Technique 1: Inversion (guard clauses + early return)

When the happy path is wrapped in nested conditions, **invert each condition,
handle the unhappy case first, and return early**. The error/edge cases collect at
the top as a "gatekeeping" section that declares the function's requirements, and
the real work drops to the bottom at the lowest indentation.

**Before** — happy path buried, reader holds 3 conditions at once:

```python
def save(user, payload):
    if user is not None:
        if user.is_active:
            if payload.is_valid():
                record = build_record(payload)
                store(record)
                return record

            else:
                raise InvalidPayload()

        else:
            raise InactiveUser()

    else:
        raise MissingUser()
```

**After** — invert the conditions, fail fast, happy path is flat:

```python
def save(user, payload):
    if user is None:
        raise MissingUser()

    if not user.is_active:
        raise InactiveUser()

    if not payload.is_valid():
        raise InvalidPayload()

    record = build_record(payload)
    store(record)

    return record
```

The mechanical steps:

1. Flip the outer condition to its negation and `return`/`raise`/`continue` early.
2. Because the function exits in that branch, the `else` is now redundant — promote
   its body up one level (delete the `else`).
3. Repeat for the next condition until the happy path sits at the base indent.

Inside loops the same move uses `continue` (or `break`) instead of `return`:

```python
for item in items:
    if item.skip:
        continue

    process(item)
```

## Technique 2: Extraction

When a nested block is a coherent unit of work, **pull it into its own
well-named function**. This removes a level of indentation at the call site and, just
as importantly, gives the block a name — which often documents intent better than a
comment would.

**Before** — the loop body is a deep, unnamed blob:

```python
def process_downloads(downloads):
    for d in downloads:
        if d.state == "in_progress":
            result = d.process()

            if result.is_error():
                if result.retriable and d.retries < 3:
                    d.retries += 1
                    d.state = "pending"
                    
                else:
                    fail(d)
```

**After** — extract the per-item handling; the loop reads as a summary:

```python
def process_downloads(downloads):
    for d in downloads:
        if d.state == "in_progress":
            process_in_progress(d)


def process_in_progress(d):
    result = d.process()
    if not result.is_error():
        return
    if result.retriable and d.retries < 3:
        d.retries += 1
        d.state = "pending"
        return
    fail(d)
```

Extraction shines on large functions with several distinct phases: lift each phase
into its own function so the top-level function becomes a readable outline of the
high-level steps, and any single function you drill into stays concise.

## Combine both

Real refactors alternate the two: extract a chunk to drop a level, then invert the
conditions inside the extracted function so its happy path is flat too. Apply
repeatedly until no function exceeds the ~3-level ceiling and each does one thing.

## Nuance — don't flatten dogmatically

The three-level rule is a smell detector, not a law. Keep judgment in the loop:

- **Don't extract a function used exactly once if the name adds no information** and
  the block is trivial. A guard clause is often enough on its own.
- **Don't invert when it inverts the meaning.** If the natural reading is "do X only
  when all of these hold", a single combined condition (`if a and b and c:`) can be
  clearer than three separate guards — pick whichever makes intent obvious.
- **Respect language idioms.** Context managers (`with`), pattern matching, and
  comprehensions remove nesting more naturally than guard clauses in some languages;
  reach for those first where they fit.
- **Early returns vs. single-exit.** Guard-clause early returns are the point of
  inversion. If a codebase enforces single-exit, prefer extraction over inversion.

The goal is reduced cognitive load and clear single-responsibility functions — not a
zero-indentation contest.

## Quick checklist

- Is any function deeper than ~3 levels? If so, it is a candidate.
- Can the happy path be moved to the lowest indent by handling edge cases first with early returns?
- Is there a nested block that forms a coherent, nameable unit? Extract it.
- After refactoring, does each function read as doing one thing, with a clear name?
