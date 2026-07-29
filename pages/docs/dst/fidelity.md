---
title: Fidelity
icon: lucide/scale
summary: Does the mock behave like the real engine — the anomaly agreement matrices, the bug-transfer differential, and what the numbers do and don't license
---

# Fidelity

Every invariant DST proves is proven against the in-memory mock — so the honest question is
always *"and does the mock behave like the real engine?"*. This page is the measured answer, in
two layers: the **anomaly matrix** (does the mock agree with real Postgres and Mongo on classic
isolation phenomena, per level?) and the **bug transfer** (do the mock's verdicts on known bugs
hold on the real engine?). Both are regenerated artifacts, never prose claims — `just
dst-fidelity` re-runs the differentials and rewrites the tables below.

--8<-- "dst/_generated/fidelity.md"

## Bugs the harness found in Forze itself

The strongest evidence is provenance. A git-history audit found three confirmed harness-found
bugs — all surfaced by the **conformance differential**, which is exactly the component this
page measures:

- **The mock's write-through dirty read.** The default mock transaction manager let a concurrent
  transaction read another's uncommitted, later-rolled-back write — weaker than real
  read-committed. Found by the `dirty_read` battery case; fixed by buffering every write through
  the MVCC overlay. Pinned forever by the battery.
- **The revision-conflict kind divergence.** On a stale-revision write the mock raised a
  different exception contract than every real adapter — an app's optimistic-concurrency
  handling would behave differently on the mock than in production. Found by the real-Postgres
  differential doing precisely its job; the mock (the outlier) was fixed to match.
- **The rev-guarded double charge.** Making read-committed faithful exposed that two concurrent
  rev-guarded updates could both commit — a double charge. Fixed with first-committer-wins
  write-write rejection; the payments example's oracle keeps it caught.

The framing matters: these are differential catches, not seed-search catches — which is exactly
what the tables above are for. Claiming more would be the overreach this page exists to prevent.

## What these numbers license — and what they don't

- A green matrix licenses trusting the mock **for the phenomena, levels, and backends tested**
  — the isolation family on Postgres and Mongo. It says nothing about untested planes.
- The transfer table is the *direct* evidence, at found-bug granularity — and its N is what it
  is: every transferable corpus instance, currently against Postgres.
- Logic **below the port** — triggers, generated columns, database views — is outside every
  oracle here. An invariant maintained by a trigger must be covered by an integration test, not
  by simulation.
- A divergence is admissible only through the reviewed catalogs above; an unexplained one fails
  the build. There is deliberately no single "fidelity score" anywhere on this page — the two
  divergence directions have opposite costs, and averaging them would hide exactly the one that
  ships bugs.
