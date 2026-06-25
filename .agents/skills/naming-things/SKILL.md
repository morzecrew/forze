---
name: naming-things
description: Name variables, functions, classes, and modules well by avoiding known anti-patterns - single letters, abbreviations, types baked into names, missing units, "Base"/"Abstract" class names, and "Utils"/"Helper" grab-bags. Use when naming or renaming code, reviewing names in a diff, or when the user mentions naming, identifiers, variable names, readability, or that a name feels off.
---

# Naming Things in Code

Good names are hard to invent but easy to get wrong. You can reach roughly 80% of
good naming just by recognizing and avoiding a handful of anti-patterns — each one,
when you catch yourself doing it, pushes you toward a clearer name. A name's job is
to tell the reader what something *is* and *means* at the point they read it,
without forcing them to jump to its definition.

A recurring theme: when a good name is genuinely hard to find, that difficulty is
often a signal that the **code structure**, not the name, is the real problem.

## Use this skill when

- Naming new variables, functions, classes, types, or modules.
- Renaming during a refactor or cleanup.
- Reviewing a diff and a name reads unclear, abbreviated, or generic.
- The user mentions naming, identifiers, readability, or "what should I call this".

## Do not use this skill when

- A name is dictated by an external contract (serialized field, API schema, protocol) you cannot change.
- A convention is enforced by the language/ecosystem (e.g. `i`/`j` in a tight numeric loop, `T` for a generic type parameter).

## Anti-patterns to avoid

### 1. Single-letter names

`d`, `x`, `t` carry no meaning and force the reader to infer the purpose from
usage. Prefer a descriptive word. The conventional exceptions are tiny,
well-understood scopes — a loop counter `i`, a coordinate `x`/`y`, a generic type
`T` — where the meaning is unambiguous and the scope is a few lines.

```python
for d in downloads: ...      # what is d?
for download in downloads: ...
```

### 2. Abbreviations

Abbreviations save a few keystrokes and cost every future reader a moment of
decoding. Spell words out: `cnt` → `count`, `usr` → `user`, `calcAmt` →
`calculate_amount`. Editors autocomplete; brains do not.

### 3. Type baked into the name

Don't encode the type in the identifier (`users_array`, `name_str`,
`is_valid_bool`). The type system or the value already tells you that, and the name
goes stale the moment the type changes. Name by *role*, not representation:
`users`, `name`, `is_valid`.

### 4. Missing units (the opposite rule — DO add these)

Where a number carries a unit, the unit belongs **in the name**. `delay`, `size`,
`weight` are ambiguous; `delay_ms`, `size_bytes`, `weight_kg` are not. This removes
a whole class of bugs (seconds vs milliseconds) without a comment.

```python
sleep(delay)        # seconds? milliseconds?
sleep(delay_ms)
```

### 5. Classes named "Base" or "Abstract"

`BaseTruck` / `AbstractTruck` describe the code's mechanics, not the domain — and a
`BaseTruck` still *is* a truck, so the prefix adds nothing for users of the class.
If you extract a parent and struggle to name it, that usually means the **child**
is mis-named: name the general concept `Truck`, and make the specific subclass more
precise (`TrailerTruck`, `DumpTruck`). When you can't name the parent, rename the
child instead.

### 6. "Utils" / "Helper" grab-bags

A `utils` or `helpers` module is where functions go when no one decided where they
belong. It grows without bound and tells the reader nothing about what is inside.
Before reaching for it, ask where each function *actually* belongs — most can be
sorted into modules with real, domain-meaningful names (`time_format`, `currency`,
`url`). The grab-bag name is a symptom of a missing home.

## Naming as a structural signal

If no good name comes after real effort, resist forcing a bad one. Difficulty
naming a function often means it does more than one thing (split it); difficulty
naming a class often means its responsibilities are muddled (reshape it). Use the
struggle as design feedback, not just a vocabulary problem.

## Quick checklist

- Any single-letter names outside a tiny conventional scope? Expand them.
- Any abbreviations a newcomer would have to decode? Spell them out.
- Any type info encoded in a name? Drop it and name by role.
- Any unit-bearing number without its unit? Add the unit.
- Any `Base`/`Abstract` prefix? Rename the child to be specific instead.
- Any `utils`/`helper` bucket? Find each function's real home.
- Still can't name it well? Reconsider the structure.

## Related skills

- `never-nesting` — extracting and naming functions to flatten code; naming difficulty often signals a function doing too much.
- `self-documenting-code` — good names are what make comments redundant.
