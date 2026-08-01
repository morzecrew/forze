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
dst-fidelity` and `just dst-transfer` re-run the differentials and rewrite the tables below.

--8<-- "dst/_generated/fidelity.md"

--8<-- "dst/_generated/transfer.md"

--8<-- "dst/_generated/predictor.md"

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

## Which planes have a leg

A differential is only evidence for the plane it runs on, so the next question after "does the
mock agree?" is "agree about *what*?". That answer is a manifest rather than a paragraph:
`[tool.conformance_manifest]` in `pyproject.toml` names every plane, the scenario it runs, the
contract ports it covers and the engines it must run against. `just conformance-check` (part of
`just quality`) enforces it, and `just conformance` runs the legs themselves.

The manifest exists because the previous answer was prose, and prose does not fail a build. A
plane could gain a backend whose only test ran against that backend alone, with nothing
comparing it to anything, and CI stayed green. So the checker derives which packages actually
*register* each plane's ports — from the integration packages, not from a list somebody
maintains — and fails when one of them runs no leg. Adding a backend now means adding its leg or
writing down, in the manifest, why there isn't one.

That check runs before the suite does, so it can only prove a leg **exists**. Existing is not
running: a leg whose engine never starts, whose optional extra is missing, or whose suite is
absent from CI's matrix skips quietly, and a skipped test looks exactly like a passing one in a
green pipeline. So each CI shard also records what it actually ran, per leg, and a final job
unions those records and fails on any manifested leg that passed nothing anywhere. That gate
earned its place immediately: it found that two of the four inference legs, and the whole
portability suite, had never run in CI at all. Individual skips *inside* a leg stay allowed —
a check that cannot apply to an engine should skip with a reason naming it — but they are
reported, so a leg quietly hollowing out one check at a time stays visible.

Uncovered planes are declared rather than omitted, which is the part worth reading:

| Gap | Engines with no differential | Why it matters |
| --- | --- | --- |
| `queue` | RabbitMQ, SQS | two brokers, no comparison of publish/consume/nack/redelivery |
| `analytics` | BigQuery, ClickHouse, DuckDB, Postgres | four engines answer the same ports |
| `durable_function` | Postgres, Inngest | replay determinism and step idempotency uncompared |
| `kms` | Vault (plus the AWS/GCP/Yandex/local backends) | wrap/unwrap/rotate under one AAD |

Read that table as the honest complement to the green matrices above: those planes are tested,
they are simply not *compared*.

Deriving the requirement from the code is what keeps that table honest, and one plane needed
help to manage it. Field encryption's ports are *resolved* by the adapters that seal values
rather than registered by a backend, so the census sees no providers for them — and a
derivation that finds nothing requires nothing, which is a ratchet that cannot fail. The
manifest lets such a plane name a proxy instead (`derive_from`), and here the proxy is exact
rather than approximate: the battery is document-plane specific, so "which backends need a
field-encryption leg" *is* "which backends provide a document port". A misspelled proxy key is
a hard error for the same reason — it would derive from nothing and look like success.

Known differences that are real and expected live in `forze_dst.conformance.catalog` as data, not
prose. Each row records what each engine did, what was done about it (unified, normalized, or
declared), and a `probe` naming the test that asserts it — a link the checker resolves against
real pytest collection, so a catalog entry cannot outlive the test behind it.

One row is worth reading for a different reason: it records a case where the oracle could not
be *wrong*, because it could not model the outcome at all. The storage port lets a caller ask
`list(missing_ok=False)` so a **vanished** bucket can be told from an **emptied** one — the
distinction the re-encryption sweep is built on. The mock reached its bucket through
`setdefault`, so the bucket existed the instant anything looked at it and the parameter was
documented as a no-op. That is a stronger failure than "untested": no test written against
that oracle could have failed, so every mock-backed test of the contract was green without
exercising anything. Closing it meant giving the mock the concept first — reads never
provision, the documented write paths do — and the sweep's own test then had to say which
state it meant, having asserted "empty" while exercising "absent".

A third row records the one interaction none of this page's other machinery can reach. The
offline mailbox bounds a reconnect replay by a retention cap, and clients ack **cumulatively**
— "I have everything up to this id". Each is fine alone; together, a replay that delivered an
incomplete window plus an ack on a live frame lets the cursor jump the gap, and the trim floor
then deletes signals that were never sent. Nothing raises. Simulation cannot find it either,
because the race lives in document-port code rather than stream code, so no schedule the
explorer drives will interleave it. The leg drives that interleaving directly against the mock,
Postgres and Mongo, and its controls reconstruct the fault by truncating the replay — so the
check is known to fail when the guarantee does, rather than merely passing today.

One divergence there is worth reading as a pattern rather than an entry. The inference port is
**declarative**: each adapter publishes an `InferenceCapabilities` and the shared validators gate
requests against it, so "does this call get through?" is a property of the declaration, not of the
model behind it. A mock told nothing advertises the full surface — unbounded batches, streaming,
async jobs — which is true of the mock and truer than any backend it stands in for, so a
capability gate that passes against the oracle can still refuse in production. The fix is a
wiring obligation (`MockInferenceRegistry.on(..., capabilities=…)`) and it cannot be defaulted
away, because the mock is also used where no backend is being mirrored at all. So the differential
carries it instead: the battery compares a backend's gates against a mock built from that
backend's own declaration, and asserts that an *untold* oracle disagrees — which is what makes
forgetting the wiring fail at authoring time rather than in production.

## What these numbers license — and what they don't

- A green matrix licenses trusting the mock **for the phenomena, levels, and backends tested**
  — the isolation family on Postgres and Mongo. It says nothing about untested planes, and
  [the gap table](#which-planes-have-a-leg) is where those are named.
- The transfer table is the *direct* evidence, at found-bug granularity — and its N is what it
  is: every transferable corpus instance, currently against Postgres.
- The pre-registered predictor analysis is currently uninformative by construction (zero
  divergence on either plane), so the battery is **not** certified as a substitute for the
  transfer run — and for the corpus's non-isolation half it could never be one.
- Logic **below the port** — triggers, generated columns, database views — is outside every
  oracle here. An invariant maintained by a trigger must be covered by an integration test, not
  by simulation — and that exclusion is now *checkable*, not just prose: declare it with a
  `HorizonDeclaration` naming the covering test, and per-invariant accounting keeps the clean
  verdict scoped to invariants the harness has been shown able to catch (see
  [what a green run doesn't say](invariants.md#what-a-green-run-doesnt-say)).
- A divergence is admissible only through the reviewed catalogs above; an unexplained one fails
  the build. There is deliberately no single "fidelity score" anywhere on this page — the two
  divergence directions have opposite costs, and averaging them would hide exactly the one that
  ships bugs.
