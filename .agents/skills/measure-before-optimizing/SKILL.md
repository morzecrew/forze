---
name: measure-before-optimizing
description: Optimize only what measurement proves matters - carry both halves of Knuth's rule (skip the 97%, do not pass up the critical 3%), profile first because Amdahl's law caps every speedup, benchmark without warmup/noise/mean-vs-percentile traps, take data-structure and algorithm wins before micro-tweaks, and make architectural performance calls (query shape, data layout) at design time because they can't be profiled in later. Use when optimizing or profiling code, reviewing "X is faster than Y" claims, benchmarking, or when the user mentions performance, slow code, bottlenecks, latency, throughput, speed, or efficiency.
---

# Measure Before Optimizing

Knuth's rule is almost always quoted at half strength. The full sentence, from
"Structured Programming with go to Statements" (1974): "We should forget about
small efficiencies, say about 97% of the time: premature optimization is the root
of all evil. **Yet we should not pass up our opportunities in that critical 3%.**"
That's two obligations, not one — don't tune noncritical code on a hunch, *and*
find and fix the critical fraction. Knuth adds how to tell them apart: it's "often
a mistake to make a priori judgments" about what's critical, because programmers'
intuitive guesses fail once measurement tools are applied. Hence the discipline:
reproduce, measure, fix the highest-leverage cause, measure again.

## Use this skill when

- About to optimize code, or asked to "make this faster".
- Reviewing a change or suggestion justified as "X is faster than Y".
- Writing or interpreting a benchmark.
- Making design decisions with performance consequences (query shape, data layout, API granularity).

## Do not use this skill when

- There is a measured, reproduced problem and you're inside the loop below — that's not premature, proceed.
- A hard real-time or throughput budget is an explicit, stated constraint of the task.

## Why profile first: Amdahl's law

Total speedup is capped by the fraction of runtime the code you're touching
actually occupies: `speedup = 1 / ((1 − p) + p/s)` for a fraction `p` made `s`
times faster.

| You optimize... | Local speedup | Whole-program gain |
|---|---|---|
| 4% of runtime | infinite | ≤ 1.042× |
| 20% of runtime | 2× | 1.11× |
| 60% of runtime | 2× | 1.43× |

Profiling is how you learn `p`. Without it, optimization is a lottery where most
tickets are printed on the 4% row — effort spent making irrelevant code fast.

## Macro vs. micro

| Level | Examples | Payoff |
|---|---|---|
| **Macro** (design) | algorithms, data structures, I/O and query shape, caching, batching | Orders of magnitude; where real wins live |
| **Micro** (line) | operator swaps, loop tricks, avoiding a call | Constant factors; only worth it on a profiler-confirmed hotspot |

A complexity-class fix beats any constant-factor tweak:

```python
# WRONG: hunch-driven micro-tweak on unprofiled code
seen = []
for x in items:
    if x not in seen:      # O(n) scan — the actual cost
        seen.append(x)
# ...while the "optimization" applied was hoisting len() out of a loop.

# RIGHT: profile shows membership testing dominates; fix the data structure
seen = set()               # O(1) membership; O(n^2) loop becomes O(n)
```

## The optimization loop

1. **Reproduce the problem.** No symptom, no optimization.
2. **Baseline.** Record a number under stated conditions, or you can't tell whether a change helped.
3. **Take the macro wins.** Better data structure (the highest-leverage lever), better algorithm, fewer round trips.
4. **Profile for hotspots.** Let the profiler assign `p`; it is frequently not where you guessed.
5. **Micro-optimize last**, only on confirmed hotspots — one change at a time, measuring each, and stop when the budget is met.

## Benchmark honestly

Bad measurement is worse than none — it justifies the wrong change with a number.

- **Warmup / JIT**: early iterations measure compilation and cold caches, not your code. Use a harness that handles this (JMH, pyperf, criterion.rs, BenchmarkDotNet) or discard warmup runs.
- **Dead-code elimination**: optimizers delete work whose result is unused, yielding impossible speeds. Consume results (blackhole/sink).
- **Unrealistic inputs**: constant-folded arguments, cache-hot microloops, and toy data sizes flatter code that is memory- or I/O-bound in production. Benchmark with production-shaped inputs.
- **Noise**: CPU frequency scaling, thermal throttling, and co-tenant processes swamp small effects. Repeat runs; compare distributions, not two single numbers.
- **Mean vs. percentiles**: latency distributions are skewed, so report p50/p95/p99 — a healthy mean can hide a tail that every user hits once per session.

## The counterpoint: some performance can't be profiled in later

Performance is a feature, and the measure-first rule applies to code you can still
cheaply change. Some decisions set the performance *ceiling* and cost a rewrite to
retrofit: data layout and schema, N+1 vs. batched query shape, chatty vs. coarse
APIs, sync vs. streaming, runtime choice for a hot service. Decide these at design
time by estimating with known costs — round trips, disk vs. memory, serialization
— not by shipping and hoping the profiler saves you.

```python
# WRONG: "we'll optimize later" — this shape ships 1+N queries and no
# hotspot-level fix removes them; the loop *is* the design.
for order in orders:
    customer = db.get_customer(order.customer_id)

# RIGHT: choose the query shape up front — 2 queries regardless of N
customers = db.get_customers({o.customer_id for o in orders})
```

The boundary: this licenses **design-level reasoning about known asymptotics and
I/O counts**, made when the decision is cheap. It does not license line-level
tweaks before measurement — those still wait for the profiler.

## Quick checklist

- Is there a reproduced problem and a baseline number? If not, stop.
- What fraction of runtime does the target code occupy (Amdahl)? Profiled, not guessed?
- Could a data structure or algorithm change fix it before any micro-tweak?
- Does the benchmark survive warmup, dead-code, input-realism, and noise scrutiny — and does it report percentiles where latency matters?
- Is this actually an architectural decision (query shape, data layout)? Then estimate now; don't defer it to a future profiler.
- Are you trading readability or adaptability for speed you haven't measured?

## Related skills

- `never-nesting`, `naming-things`, `self-documenting-code` — keep code clear first; optimize measured hotspots second.
- `composition-over-inheritance` — the adaptability whose small indirection cost is usually worth paying.
- `reading-isnt-proof` — a benchmark you ran beats a speedup you inferred from reading the code.
