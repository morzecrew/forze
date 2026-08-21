# Naming

A name is read far more often than its definition. Its job is to tell the
reader what something *is* and *means* at the point of use, without a jump to
the declaration.

A name is read far more often than its definition. Its job is to tell the
reader what something *is* and *means* at the point of use, without a jump to
the declaration. Most good naming is achievable by following a few positive
rules and refusing a known catalog of anti-patterns — and when a good name
still won't come, that difficulty is design feedback: the **structure**, not
the vocabulary, is usually the real problem.

## Positive rules

### Name length proportional to scope

The farther from its declaration a name is used, the more it must explain.
`i` is fine in a three-line loop where declaration and every use are visible at
once; a module-level or exported name must carry full meaning on its own
(`activeSessionCount`, not `cnt`). Code Complete cites Gorla, Benander &
Benander: debugging effort was lowest with variable names averaging 10–16
characters — a calibration point, not a quota. Short names aren't wrong;
short names in **wide scopes** are.

### Problem-domain over solution-domain

Name things for what they mean in the problem, not how the program handles
them: `employeeRoster` not `inputRecord`, `printerReady` not `bitFlag`,
`overdueInvoices` not `filteredList`. A domain expert reading the code should
recognize their own vocabulary. Solution-domain names describe plumbing that
the reader can already see; problem-domain names carry the intent they can't.

### Boolean names assert a fact

Name booleans so `if <name>` reads as a true/false claim: `is_active`,
`has_children`, `can_retry`, `should_flush`, or bare truth words (`done`,
`found`, `error`). Keep the positive form — negated names force double
negatives at the use site:

```python
if not user.is_not_verified: ...   # brain-twister
if user.is_verified: ...
```

The same applies to flags: `disable_cache=False` reads worse than
`enable_cache=True`.

### Units in the name — or better, the type

A bare number with an implicit unit is a bug generator (`sleep(delay)` —
seconds or milliseconds?). Put the unit in the name: `delay_ms`, `size_bytes`,
`weight_kg`, `timeout_s`. Where the language allows, a dedicated type
(`Duration`, `Bytes`) is stronger still — the checker enforces what a name only
suggests.

### One word per concept, consistent pairs

Pick one verb per concept across the codebase — not `fetchUser`, `getAccount`,
`retrieveOrder` for the same kind of operation. Keep pairs symmetric:
`open/close`, `begin/end`, `add/remove`, `create/destroy`. Inconsistency makes
readers hunt for distinctions that don't exist.

### Names must be honest

The name is a promise. A `get_user()` that creates the user, a `check_quota()`
that mutates state, an `is_valid()` that performs I/O — each lies, and a lying
name is worse than a vague one because the reader trusts it. If behavior grows
beyond the name, rename (`get_or_create_user`) or split.

## Anti-pattern catalog

| Anti-pattern | Example | Fix |
| --- | --- | --- |
| Single letter in non-trivial scope | `for d in downloads` | `for download in downloads` |
| Abbreviations | `cnt`, `usr`, `calcAmt` | `count`, `user`, `calculate_amount` |
| Type baked into name (Hungarian) | `users_array`, `name_str` | name the role: `users`, `name` |
| Missing unit | `delay`, `size` | `delay_ms`, `size_bytes` (or a unit type) |
| Negated boolean | `not_found`, `disable_ssl` | `found`, `enable_ssl` |
| Vague agent/filler words | `DataManager`, `InfoProcessor`, `RequestHandler2` | say what it does: `SessionCache`, `InvoiceParser` |
| `Base` / `Abstract` prefix | `BaseTruck` → `Truck` | rename the **child** to be specific instead |
| `Utils` / `Helpers` grab-bag | `utils.py` with 40 strays | real homes: `currency.py`, `url.py`, `time_format.py` |
| Numbered variants | `data1`, `data2`, `processV2` | name the difference: `raw_rows`, `deduped_rows` |
| Near-twin names | `userInfo` vs `userData` in one scope | make the distinction explicit or merge them |

Notes on the subtler entries:

- **Abbreviations** save the writer keystrokes and cost every reader a decode.
  Editors autocomplete; brains don't. Exceptions: abbreviations more
  recognizable than the expansion (`id`, `url`, `max`, `db`).
- **Type-in-name** goes stale the moment the type changes, and the type system
  already answers that question. Name the role the value plays.
- **`Base`/`Abstract`** describes mechanics, not domain — a `BaseTruck` is
  still a truck. If the parent is hard to name, the child is misnamed: let the
  parent own the general concept (`Truck`) and make children precise
  (`TrailerTruck`, `DumpTruck`).
- **Vague words** (`Manager`, `Processor`, `Handler`, `Data`, `Info`,
  `Object`, `Impl`) are hedges — they admit you haven't decided what the thing
  is. Kevlin Henney's test: if you delete the word, does the name lose any
  information? Then the whole name is padding.
- **`Utils` grab-bags** grow without bound precisely because the name accepts
  anything. Each function has a real home; the bucket is the symptom of not
  finding it.

## Naming as a structural signal

If no honest, specific name comes after real effort, don't force a mushy one —
read the difficulty as feedback. A function you can only call
`process_and_update` does two things: split it. A class you can only call
`DataManager` has no single responsibility: reshape it. A variable you can
only call `temp` or `result2` marks a computation that should be its own named
step or function.
