# RFC 0033 — Pipeline backend breadth: Airbyte, Dagster, managed variants

- **Status:** 📝 Draft (gated on RFC 0031 P2; each backend demand-gated separately — the 0016 shape)
- **Scope:** Extend RFC 0031's pipeline plane beyond Airflow **by doctrine, not uniformly by code**: every candidate backend is triaged by what its API can actually promise, ships with a published fail-closed capability matrix, and turns its own hard limits into wiring refusals rather than documentation. **(W1)** Airbyte — the parameterless backend, whose single missing feature cascades into a mandatory tenancy floor. **(W2)** Dagster — the triple-selector backend with a server-assigned run id. **(W3)** managed Airflow (Astronomer, Cloud Composer, MWAA) — argued as *client configuration, not adapters*. Plus §5's re-verification protocol, because every fact in this RFC has a version attached.
- **Related:** RFC 0031 (contract, capability properties, taxonomy — all reused verbatim). RFC 0016 and RFC 0010 are the doctrine-triage precedent: an honest enforcement matrix beats a uniform abstraction. RFC 0032 §3–§4 depends on this RFC's idempotency column — the compensating control exists *because* two of three backends land in the "no" column. RFC 0013's deliberate-thinness posture governs how much of each backend's model gets mapped.
- **Origin:** The source specification's per-backend validation, which is the part of it with the shortest half-life. Airflow moved its REST surface from `api/v1` to `api/v2` between major versions; a Dagster release that lets a caller assign a run id would flip a capability and delete a whole mitigation from RFC 0063. Breadth work in this plane is therefore not "more adapters" — it is keeping a matrix true.

---

## 1. The matrix, and what it is for

Facts confirmed against current documentation at drafting (2026-08); §5 is the protocol for keeping that true.

| | Airflow 3.x | Airbyte v1 | Dagster |
|---|---|---|---|
| `accepts_parameters` | ✅ `conf` | ❌ **none** | ✅ `runConfigData` |
| `supports_idempotency_key` | ✅ `dag_run_id` | ❌ | ❌ (§3) |
| `tenant_aware` ceiling | `namespace` / `dedicated` | **`namespace` minimum, forced** | `namespace` / `dedicated` |
| `CANCELLED` distinguishable | ❌ (cancel lands in `failed`) | ✅ | ✅ (`CANCELING`/`CANCELED`) |
| Catalog protocol | ✅ `GET /api/v2/dags` | ✅ `GET /v1/connections` | ✅ `repositoryOrError` |
| Log protocol | ✅ per task instance | ⚠️ verify (§5) | ⚠️ verify (§5) |
| §2.1 schedule attestation | ✅ DAG timetable readable | ✅ `scheduleType` readable | ⚠️ best-effort (sensors) |
| `OBSERVE` mode viable (0031 §4.1) | ✅ | ✅ | ✅ — attestation only records |

The matrix is a **published, queryable artifact**, not a table in prose — the same decision RFC 0016 made for dynamic-read enforcement. Its consumers are real: RFC 0032 branches on the idempotency column, a caller can assert a capability to fail closed, and the conformance battery asserts that each adapter's declared capabilities match its observed behaviour, so a matrix that drifts from reality fails a test rather than misleading a reader.

Airflow is the only backend where `supports_idempotency_key` is **native rather than emulated**, and that is worth stating plainly because it is the deciding factor for anything that must not run twice.

## 2. W1 — Airbyte: one missing feature, three consequences

| Method | Call |
|---|---|
| `trigger` | `POST /v1/jobs`, body `{connectionId, jobType: "sync" \| "reset"}` |
| `status` | `GET /v1/jobs/{jobId}` |
| `cancel` | job cancellation endpoint (§5) |
| `find_runs` | `GET /v1/jobs?connectionId=…` |
| `list_definitions` | `GET /v1/connections` |

`definition` = `connectionId`.

**There is nowhere to put parameters.** The trigger body accepts a connection id and a job type; arbitrary configuration has no channel. Hence `accepts_parameters = False`, and three consequences follow — the first two from the source specification, the third this RFC's addition:

1. **All parameterisation is baked into the connection, so a per-tenant definition is mandatory rather than optional.** `tagged` is unreachable in principle: a shared connection means shared credentials and a shared destination, which is not a weaker isolation tier but *no isolation at all*.
2. **The adapter must not modify the connection before triggering.** `PATCH` then `POST` is the tempting workaround and it is a race under concurrent triggers — two tenants, one connection, whichever `PATCH` landed last. The structural enforcement is to **ship no connection-mutating method on the adapter or its client port**: an API that does not exist cannot be reached for by a future maintainer under deadline.
3. **The tenancy floor is a wiring refusal, not a recommendation** *(added here)*. Since `tagged` is not merely discouraged but unsafe by construction, an Airbyte pipeline route configured below `namespace` fails at freeze with its code. RFC 0031 §6 recommends `namespace` across the plane; for this backend the recommendation is a gate, on the same reasoning that made RFC 0031 §2.1 a gate — a precondition that only lives in prose is one deployment away from being violated.

**Idempotency is absent.** The Airflow provider's own Airbyte documentation warns that re-triggering is not idempotent. Partial protection exists at platform level — at most one sync per connection at a time, with a newly queued run superseding the previous — but that is a concurrency property, not an idempotency key, and RFC 0032 §4's compensating control exists exactly because it cannot be relied on.

**States:** Airbyte has a distinct cancellation status, so all five `RunState` values are representable — the only backend of the three where that is true.

**Platform floor:** Airbyte Cloud enforces a minimum sync interval. A schedule violating it is a Forze-side validation concern (RFC 0032 §6), not something the adapter should discover at trigger time.

## 3. W2 — Dagster

| Method | Call |
|---|---|
| `trigger` | `launchRun(executionParams: {selector, runConfigData})` → `LaunchRunSuccess { run { runId } }` |
| `status` | `runOrError(runId:)` |
| `cancel` | `terminateRun(runId:)` |
| `find_runs` | `runsOrError(filter: {statuses: […]})` |
| `list_definitions` | `repositoryOrError(…) { jobs { name } }` |

**Addressing** is a triple — `repositoryLocationName` / `repositoryName` / `jobName` — and this is the case that settles RFC 0031 §3.2: `definition` must be an opaque adapter-encoded string, because "the name of the DAG" is not a shape every backend has.

**Idempotency: the run id is assigned by the server.** Deduplication via run tags is expressible (`executionMetadata.tags` accepts `ExecutionTag` entries, confirmed at drafting) but amounts to read-check-launch with a race between the read and the launch. Declaring `supports_idempotency_key = False` is more useful than emulating it unsafely: a caller that knows the truth can compensate, a caller that trusts a false `True` cannot. §5 item 4 is the check that would change this.

**States:** `CANCELING`/`CANCELED` → `CANCELLED`; `QUEUED`/`NOT_STARTED`/`STARTING` → `PENDING`; `STARTED` → `RUNNING`.

**Partitions are deliberately not mapped.** Dagster's partition model is richer than anything the other two backends have, and pulling it into the main port would export one vendor's concepts under a portable name — the failure RFC 0031 §5 refuses for DAG structure. If it is ever needed it is a **separate optional protocol**, the way `PipelineCatalogPort` and `PipelineLogPort` already are, never an extension of the run port.

**Attestation is best-effort here** (RFC 0031 §2.1): schedules attached to a job are enumerable, but a *sensor* can launch runs on conditions the API describes only partly. The capability says best-effort rather than claiming a guarantee, and the docstring names sensors as the residual.

## 4. W3 — Managed Airflow is configuration, not an adapter

Astronomer, Cloud Composer and MWAA are managed Airflow speaking the same REST API; what differs is the base URL and the authentication method — that is **client configuration**. Google's own Composer 3 examples use exactly `api/v2/dags/{dag_id}/dagRuns`.

The practical conclusion: **do not create modules per managed platform until an API divergence is actually found.** Differences live where they are expected — auth and network — and a speculative `forze_astronomer` would be a package whose entire content is a credentials adapter.

One honesty constraint attached: the plane may claim support only for what a battery has run against. A managed platform without a test fixture is documented as *"the same adapter; configuration differs; not exercised by our suite"* — which is a true statement, unlike an unqualified support claim.

## 5. The re-verification protocol

Every fact above has a version attached, so re-verification is a **gate before each adapter merges**, not a wish. Each item names what it would change, because a checklist without consequences does not get run:

| # | Check | What it changes if it has moved |
|---|---|---|
| 1 | Airflow REST path (`api/v2` in 3.x vs `api/v1` in 2.x) and the exact run-state set in the target minor | The adapter's routes and the `RunState` mapping — a silent mismatch produces wrong states, not errors |
| 2 | Airflow behaviour on a duplicate `dag_run_id`: status code and error shape | Whether `DuplicateRun` is detectable, i.e. whether the one native idempotency guarantee in the plane holds |
| 3 | Full Airbyte job-status list and the exact shape of `GET /v1/jobs` | The state mapping and `find_runs` pagination |
| 4 | **Whether a caller can assign `runId` via `executionParams` in the target Dagster version** | Flips `supports_idempotency_key` to `True`, changes the matrix, and **removes the need for RFC 0032's lock on this backend** — the highest-leverage item in the table |
| 5 | Shape of `runsOrError` with pagination | The port assumes paged output; without it, `find_runs` needs a different strategy |
| 6 | Airbyte's cancellation endpoint and Cloud's current minimum interval | `cancel` wiring and RFC 0032 §6's floor |
| 7 | Airflow multi-team status, and whether its documentation still declines to call it multi-tenancy | RFC 0031 §6's tenancy ceiling argument |

Item 4 deserves its emphasis: a single upstream change would delete a mitigation, simplify a kit and improve a guarantee. It should be re-checked at every pickup, not only before the Dagster adapter.

## 6. Acceptance battery (per shipped backend)

1. The RFC 0031 battery, re-run wholesale against each backend — the contract's portability is proven by test reuse, not asserted. Items the backend cannot satisfy are **explicit skips with a declared capability reason**, never quiet passes.
2. **Declared capabilities match observed behaviour**: for each property, a test that exercises the real API and fails if the flag lies. This is what keeps §1 from drifting into fiction.
3. Airbyte: a route below `namespace` isolation fails at freeze; supplying params to the port raises `CapabilityNotSupported` at wiring; the client port exposes **no** connection-mutating method (asserted structurally, not by convention).
4. Airbyte: two triggers in flight — the platform's supersede behaviour is pinned as observed reality, with a comment that it is not an idempotency guarantee.
5. Dagster: the triple selector round-trips through the opaque `definition`; a run launched with tags is found again; the read-check-launch race is pinned as a documented limitation rather than mitigated in the adapter.
6. Dagster: full state mapping including both cancellation states.
7. Attestation per backend: a self-scheduling definition is refused; for Dagster, a sensor-triggered job is documented as the residual the check cannot see.
8. Managed Airflow: where a fixture exists, the Airflow battery runs unchanged against it; where none exists, the support claim is absent from the docs rather than unqualified.

## 7. Phases

Each workstream ships against a named consumer, not speculatively.

- **W1 Airbyte** — trigger on ingestion-shaped work (a connection that must run on Forze's schedule). Highest likelihood of being first.
- **W2 Dagster** — trigger on a deployment that already runs Dagster.
- **W3 Managed** — no build; a documentation and configuration note, plus a fixture if a deployment provides one.

## 8. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Per-backend triage with a **published, queryable, test-asserted** capability matrix (the 0016 shape); adapters never emulate a capability the backend lacks | locked |
| 2 | Airbyte `accepts_parameters=False` ⇒ per-tenant definitions mandatory ⇒ **`tagged` refused at freeze**, not merely discouraged | **locked** (upgrade of the source specification; promoted 2026-08-03 with RFC 0031 decision 2, same principle — a precondition that only lives in prose is one deployment from being violated) |
| 3 | No connection-mutating method ships on the Airbyte adapter or its client port — the `PATCH`-then-`POST` race is prevented structurally, by absence | locked |
| 4 | Dagster declares `supports_idempotency_key=False` rather than emulating via tags; honest refusal beats an unsafe `True` | locked |
| 5 | Dagster partitions are not mapped into the run port; if ever needed, a separate optional protocol | locked |
| 6 | Managed Airflow variants are client configuration, not adapters, until a real API divergence appears; support is claimed only where a battery has run | locked |
| 7 | §5 is a merge gate with per-item consequences, not a checklist; item 4 (Dagster `runId`) is re-checked at every pickup because it would delete a mitigation in RFC 0032 | locked |
| 8 | Every fact here is drafting-time (2026-08) and carries a version; the matrix is maintained, not written once | recorded |
