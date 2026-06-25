---
name: measure-before-optimizing
description: Optimize code only after a real, measured performance problem - distinguish macro (design-level) from micro (fine-tuned) performance, reach for data-structure and algorithm wins before micro-tweaks, profile to find real hotspots, and don't trade readability or adaptability for unmeasured speed. Use when optimizing code, reviewing performance suggestions, choosing "faster" constructs, or when the user mentions performance, optimization, speed, or efficiency.
---

# Measure Before Optimizing

"Premature optimization is the root of all evil" — and in practice premature
optimization almost always means **micro-optimization done on a hunch**, without a
measured problem. The discipline is simple: confirm there's a real performance
problem, measure it, fix the highest-leverage cause, and measure again. Speed you
can't measure isn't speed you can trust.

## Use this skill when

- About to optimize code, or asked to "make this faster".
- Reviewing a change/suggestion justified as "X is faster than Y".
- Choosing between constructs primarily on performance grounds.
- The user mentions performance, optimization, speed, latency, or efficiency.

## Do not use this skill when

- There is a measured, reproduced performance problem and you're following the loop below (then it's not premature — proceed).
- A hard real-time/throughput requirement is an explicit, stated constraint of the task.

## Macro vs. micro performance

- **Macro (design-level)** — system-wide structure: algorithms, data structures,
  I/O patterns, caching, query/network shape. This is where the large wins live.
- **Micro (fine-tuned)** — line-level tweaks: swapping operators, manual loop
  tricks, avoiding a function call. Tiny payoff, and where premature optimization
  usually happens.

The classic case: a code-review comment says "use X instead of Y because X is
faster." But computers are fast, and you are writing code to solve a real-world
problem — reaching a correct, clear solution sooner is usually worth more than a
slower-to-write solution made of faster code.

## Adaptability vs. performance

Adaptable code (abstraction, indirection) tends to cost some performance, and that
trade-off is usually worth it for the flexibility it buys as requirements change.
Don't strip away clarity or extensibility for speed you haven't shown you need.

## The optimization loop

There is really only one reliable way to optimize: **measure, change one thing,
measure again.** When you have a confirmed problem, work in leverage order:

1. **Have a real performance problem.** Reproduce it; don't optimize on suspicion.
2. **Measure it.** Get a baseline number so you can tell whether a change helps.
3. **Make the 80% moves.** Pick a better **data structure** (by far the highest-
   leverage lever) or a well-known faster **algorithm**. Most real wins are here.
4. **Profile and find hotspots.** Let the profiler point at where time actually
   goes — it's frequently not where you guessed.
5. **Micro-optimize last.** Only as a worst case, reason about what the code does
   under the hood — and keep measuring each change.

## Quick checklist

- Is there a measured, reproduced problem? If not, stop — don't optimize yet.
- Do you have a baseline number to compare against?
- Could a different data structure or algorithm fix it before any micro-tweak?
- Did you profile to confirm the real hotspot rather than guessing?
- Are you trading away readability/adaptability for speed you haven't measured?

## Related skills

- `never-nesting`, `naming-things`, `self-documenting-code` — keep code clear first; optimize the measured hotspots second.
- `composition-over-inheritance` — the adaptability whose small performance cost is usually worth paying.
