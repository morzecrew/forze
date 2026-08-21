# Nesting

How to flatten, and the cases where flattening makes code worse.

Each level of indentation is one more condition the reader must hold as true to
understand the innermost line. SonarSource's cognitive-complexity metric
formalizes this: every control structure costs +1, **plus +1 for each level of
nesting it sits under**. Four sequential guard clauses cost 4; the same logic as
four nested `if`s costs 1+2+3+4 = 10. Nesting doesn't add difficulty linearly —
it compounds. The Linux kernel style guide puts it bluntly: "if you need more
than 3 levels of indentation, you're screwed anyway, and should fix your
program."

Treat **three levels as a soft ceiling** and a fourth as a refactoring signal,
not a formatting problem. The moves below flatten code; the last section covers
the cases where flattening makes code worse.

## The move-set

| Move | Use when | Effect |
| --- | --- | --- |
| Guard clause (invert + early return) | edge/error cases wrap the happy path | happy path drops to base indent |
| `continue` / `break` | per-item conditions nest a loop body | loop body flattens |
| Extract function | a nested block is a coherent, nameable unit | resets nesting to zero; names intent |
| Define errors out of existence | every caller must check or catch | the error branch disappears entirely |
| Aggregate error handling | `try`/`catch` wraps each individual call | one handler at one level |
| Dispatch table / pattern match | nested `if`/`else` selects among cases | branching becomes data |

## Guard clauses: invert, fail fast, return early

Fowler's "Replace Nested Conditional with Guard Clauses": handle each unusual
case first with an immediate `return`/`raise`, so the remaining code needs no
`else` and the real work sits at base indentation. The guards read as a
declaration of the function's preconditions.

```python
# Before: reader holds 3 conditions to understand the core 3 lines
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

# After: each condition is discharged, then forgotten
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

Mechanics: (1) negate the outermost condition and exit early in that branch;
(2) the `else` is now dead — delete it and promote its body one level;
(3) repeat inward until the happy path is flat. Inside loops, the same inversion
uses `continue` (or `break`) instead of `return`:

```python
for item in items:
    if item.skip:
        continue
    process(item)
```

If several guards share the same failure response, one combined condition
(`if not (a and b and c): raise ...`) can beat three separate guards — choose
whichever states the requirement most directly.

## Extraction: name the block, reset the depth

When a nested block does one coherent thing, pull it into a function. This
removes indentation at the call site, gives the block a name that documents
intent, and — because complexity metrics assess each function separately —
resets the nesting count to zero inside the new function.

```python
# Before: the interesting logic starts 4 levels deep
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

# After: the loop is a summary; the extracted function is guard-flat
def process_downloads(downloads):
    for d in downloads:
        if d.state == "in_progress":
            handle_result(d, d.process())

def handle_result(d, result):
    if not result.is_error():
        return
    if result.retriable and d.retries < 3:
        d.retries += 1
        d.state = "pending"
        return
    fail(d)
```

For a long function with distinct phases, extract each phase so the top level
reads as an outline. Real refactors alternate the moves: extract to drop a
level, then invert inside the extraction.

## Flatten error handling at the source

Nesting is often not a control-flow problem but an API-design problem. Two
moves from Ousterhout's *A Philosophy of Software Design*:

- **Define errors out of existence.** Redesign the operation so the "error"
  case is normal behavior and the branch vanishes for every caller. Deleting a
  missing key throws → make deletion idempotent (succeed if already absent).
  Out-of-range substring throws (Java) → clamp to the valid range (Python
  slicing). One API change deletes a `try`/`except` from every call site.
- **Aggregate exception handling.** Instead of wrapping each call in its own
  `try`/`catch` (pyramids of handlers), let exceptions propagate to a single
  handler at the level that can actually respond — a request-level error
  handler, a per-item `try` around the loop body, a top-level retry loop.

## Replace conditional trees with data or idioms

- A nested `if`/`else` chain that maps a value to behavior is a **dispatch
  table**: `handlers[kind](payload)` — branching becomes a data lookup.
- Prefer built-in flat forms where the language has them: pattern matching
  (`match`/`switch` with cases), comprehensions and `filter`/`map` for
  filter-inside-loop, `with`/`using` for acquire-release nesting.

## When not to flatten

- **Symmetric branches.** A guard clause signals "this branch is not what the
  function is about." Fowler's rule: use guards when one branch is the unusual
  case; when **both branches are normal behavior** (`days = 366 if leap else
  365`, buy vs sell), keep `if`/`else` (or a conditional expression) so both
  get equal emphasis — a guard would falsely mark one as an error path.
- **Manual cleanup languages.** Early return is safe only when cleanup runs
  automatically: RAII destructors (C++/Rust), `defer` (Go), context managers
  (`with`), `try`/`finally`, `using` (C#). In C-style code with manual
  `free`/`unlock`, extra returns leak resources; the flat idiom there is a
  single `goto err` cleanup chain (the Linux kernel's own pattern) — don't
  graft early returns onto it.
- **Trivial one-off extraction.** Don't extract a two-line block used once if
  the name adds nothing; a guard clause alone is often enough. Each extraction
  also adds a definition the reader may have to chase.
- **Single-exit codebases.** If the style guide bans early returns, flatten by
  extraction and dispatch tables instead of inversion.

The goal is fewer conditions held in the reader's head, not a zero-indent
contest.
