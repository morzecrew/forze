# RFC 0031 — Pipeline plane: governed runs on externally-defined pipelines

- **Status:** 📝 Draft (family head: 0032 = scheduling & run-tracking kit, 0033 = backend breadth)
- **Scope:** A plane for **triggering and observing pipelines whose definition lives somewhere else**: `contracts/pipeline/` (`PipelineSpec`, `RunHandle`/`RunStatus`/`RunState`, a run port, two optional protocols, three capability properties, an error taxonomy) + an in-memory mock with a real run lifecycle and fault injection + one real adapter (Airflow 3.x) so the plane is born with a differential leg rather than a tautology. The load-bearing premise, inherited from the source specification: **Forze owns the schedule; the orchestrator is an executor triggered from outside.** §2 works through the four consequences of that premise, two of which *shrink* the contract, and one of which this RFC upgrades from a documented precondition into a startup gate.
- **Related:** RFC 0013 (`Temporal deliberate thinness`) is the governing precedent — forze wraps an external execution engine thinly and does not reimplement it. The self-hosted durable tier supplies the durable cron the schedule half rides; the shipped cooperative-cancel doctrine shapes `cancel`; the operation-progress job read model is the sibling for in-forze work. RFC 0030 §5 (`the section that has to say no`) is reconciled head-on in §1 — that RFC refused to *build* an orchestrator, this one *drives* one. RFC 0015's freeze-time attestation and RFC 0029's `verify` mode are the pattern §2.1 borrows to make the double-run hazard enforceable. The `tenant_aware` property precedent and `TenantIsolationMode` floor govern §6.
- **Origin:** A source specification (v1.0-draft, validated against Airflow 3.x REST `api/v2`, Airbyte API v1 and Dagster GraphQL) proposing a `pipeline` contract, together with the DWH work in RFCs 0028–0030. The immediate consumer: a warehouse-shaped backend whose transforms partly live in an existing orchestrator rather than in forze, and which today would reach them through raw HTTP calls in handler code.

---

## 1. Why a port, when forze already has durable execution and cron

The obvious objection deserves the first section, because it is the one that decides whether this plane should exist at all: forze can already schedule (durable cron), execute durably, report progress and cancel. Why drive Airflow?

Because **the definitions are not ours**. A pipeline reached through this port is one forze did not write and cannot host: an existing DAG owned by data engineering, an Airbyte connection encoding credentials and a destination, a Dagster job that needs a Spark or dbt runtime a Python library has no business running in-process. RFC 0028 §6.2 already reached the matching conclusion from the other direction — the Databricks transform half was *declined* precisely because competing with Spark/DLT is not a seam. Triggering it is.

**Reconciliation with RFC 0030 §5, which said no to exactly this word.** That section refused to build an orchestrator: no scheduling semantics of its own beyond durable cron, no cross-service graph, no lineage inference, no per-node policy. Nothing here reverses it. This plane contains **no graph, no task composition, no dependency edges, no retry policy** — it is a client port with four methods against a remote system, in the same family as the HTTP, inference and sandbox seams. The test from that section still answers correctly: *would a data team adopt this instead of Airflow?* No — it is how you **call** Airflow.

The value is the usual seam value, and it is not small: run parameters become a validated Pydantic model instead of Airflow's untyped `conf` blob, definitions resolve per tenant through the existing resolver vocabulary instead of by hand-formatted DAG names, failures land in a shared taxonomy, capability differences are declared rather than discovered, and the whole surface is mockable — which is what makes domain tests possible without a running orchestrator, and what opens a DST contour over lost runs, hung states and cancellations that never arrived.

## 2. The premise and its consequences

**Forze owns the schedule.** Cron configuration is domain data in tenant-aware storage; the orchestrator knows nothing about time. Four things follow.

### 2.1 Remote definitions must carry no schedule of their own — and this is a gate, not a warning

| Backend | How the remote schedule is disabled |
|---|---|
| Airflow | `schedule=None` on the DAG |
| Airbyte | `scheduleType: manual` on the connection |
| Dagster | job with no attached schedules or sensors |

Violating this produces **double runs** — one on forze's schedule, one on the orchestrator's — and the source specification names the failure mode exactly right: it is discovered a week later, as duplicated data.

The source treats this as a deployment precondition. **This RFC upgrades it to a startup attestation**, because a precondition that only lives in prose is the standing "built the mechanism, not the gate" finding, and because all three backends expose the fact over the same APIs the adapter already speaks: Airflow's DAG detail carries its timetable, Airbyte's connection carries `scheduleType`, Dagster can be asked for schedules and sensors attached to a job. So the adapter can ask, at startup, whether the definition it is bound to schedules itself — and refuse to wire when it does.

`schedule_attestation` ships **on**, with `pipeline_remote_schedule_present` as the refusal — in `MANAGE` mode. In `OBSERVE` mode (§4.1) the same check runs and its verdict inverts: a self-scheduling definition is expected there and is recorded rather than refused. Two honest limits, recorded rather than glossed: the check is a point-in-time observation, so a schedule added later needs re-attestation (a periodic re-check belongs to RFC 0032's tick, where it is nearly free); and a Dagster *sensor* can trigger a job on conditions the API describes only partially, so that backend's attestation is best-effort and says so in its capability rather than claiming a guarantee it cannot keep.

This single change is the highest-value edit this RFC makes to the source specification: it converts the most dangerous item in the design from something a runbook must remember into something a deployment cannot get wrong.

### 2.2 The incrementality window moves into `params`, and `logical_date` leaves the port

Airflow derives a data interval from the schedule. With `schedule=None` there is no meaningful interval — the documentation warns that for manually triggered runs `data_interval` depends on the timetable and the trigger path and may diverge from `logical_date`. So window boundaries must be passed **explicitly, as parameters**.

Once they are, `logical_date` has no place on the port: Airbyte has no analogue at all, and Dagster models the same idea as partitions with different semantics. Airflow 3 *does* require the `logical_date` key to be present in the trigger request body — it may be `null`, meaning "run now", but omitting it returns 422 (independently confirmed against current documentation). That is **the adapter's problem**, and it substitutes the key on every call. It is not the port's surface.

**Removed from the source draft, and this RFC agrees: the `logical_date` parameter.**

### 2.3 Backfill is not an entity

Backfill in Airflow is derived from the schedule — re-running missed intervals — and is inapplicable at `schedule=None`. Dagster's backfill is partition-shaped; Airbyte has none. Three irreconcilable models, and a contract that tried to unify them would be inventing a fourth.

But once the window is a parameter, **backfill is N calls to `trigger` with different windows** — a loop in domain code. The irreducibility stops being a problem because the concept dissolves. This is the same move RFC 0030 made when it declared backfill "a partition parameter on an idempotent publish, never a subsystem"; two independent designs converging on that answer is worth noting.

**Removed: `PipelineBackfillPort`.** A batch `trigger_many` is also declined: partial-failure semantics over N triggers are unpleasant to specify and worse to consume, and parallelism is a setting on the orchestrator side, not a parameter of a call.

### 2.4 The schedule needs a home in Forze

```
schedules (document, tenant_aware)   — cron, pipeline name, params, enabled
        │
        ▼
scheduled tick (durable cron)        — select matured, call trigger
        │
        ▼
PipelineRunPort.trigger(...)
        │
        ▼
runs (document, tenant_aware)        — handle, state, updated from inbox
```

Schedules are ordinary documents, so tenant isolation comes free instead of resting on DAG file naming — which was the original argument for this division of labour and remains its strongest. The whole of this box is **RFC 0032**; this RFC only fixes the shape.

## 3. Model

### 3.1 `RunState` — and a vocabulary alignment this RFC insists on

```python
class RunState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"      # source draft: SUCCEEDED — see below
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool: ...
```

Five states cover what a domain needs. Richness like `up_for_retry` or `upstream_failed` is dropped deliberately — that is what the orchestrator's own UI is for, and a port that mirrored it would be re-exporting Airflow's model under a portable name.

**The one substantive change to the source draft.** The draft spells the success state `SUCCEEDED`. The codebase already has two run-state vocabularies and **both spell it `COMPLETED`**: `DurableRunStatus` (`PENDING`/`RUNNING`/`COMPLETED`/`FAILED`/`FORWARD_INCOMPLETE`/`CANCELLED`/`TIMED_OUT`) and `DurableWorkflowRunStatus` (`RUNNING`/`COMPLETED`/`FAILED`/`CANCELLED`/`TERMINATED`/`CONTINUED_AS_NEW`/`TIMED_OUT`). Durable run control exists *because* the asymmetry between those two under one umbrella is a real cost; introducing a third vocabulary that renames the most common terminal state would deepen exactly the problem it was written to reduce.

A **separate enum is still correct** — each plane has states the others cannot have (`FORWARD_INCOMPLETE` is a saga fact, `CONTINUED_AS_NEW` is a workflow fact, and neither is expressible for a remote DAG). Separate enums, aligned spellings: with the rename, this plane's set is a clean union of states both existing enums already know, and nobody has to remember which of three words means the same thing.

`CANCELLED` is **not representable on every backend** — see §4 of RFC 0033, and §3.3 below for how the contract refuses to lie about it. *(The source draft cross-references "4.4" here and "4.3" in its capability matrix; neither section exists in it. The content lives in its per-backend section, and the references are corrected in this translation.)*

### 3.2 `RunHandle` — an opaque address

```python
class RunHandle(BaseModel):
    definition: str    # adapter-owned, already tenant-resolved; never parsed by callers
    run_id: str
```

Opacity is not pedantry. Airflow's address is a `dag_id`; Airbyte's is a connection UUID; Dagster's is a **triple** of `repositoryLocationName` / `repositoryName` / `jobName`. There is no single shape, so the port does not parse one — the adapter encodes whatever it needs and the caller passes it back unchanged. Dagster alone disproves the "it's just the DAG name" intuition.

### 3.3 `RunStatus`

```python
class RunStatus(BaseModel):
    handle: RunHandle
    state: RunState
    started_at: datetime | None
    finished_at: datetime | None
    attempt: int
    error: str | None
```

**Removed from the source draft, and this RFC agrees: the `outputs` field.** There is nothing to unify — Airflow's outputs are XCom entries requiring a query per task instance, Dagster's are asset materializations, and Airbyte has no result in that sense. The run's *result* is read from storage where the pipeline wrote it; the port observes only the lifecycle. `PipelineSpec` loses its second type parameter as a direct consequence.

**On backends that cannot distinguish cancellation from failure** (Airflow: interrupting a run lands it in `failed`), the adapter's obligation is explicit — either track the cancel intent on its own side or **honestly return `FAILED`**. Silently mapping a cancellation to `FAILED` without saying so in the docstring is a documented source of wrong alerts. The cleaner division, which RFC 0032 implements: the *adapter* never lies, and the *kit's* run record carries `cancel_requested_at`, so the read model can present the truth the port cannot.

### 3.4 `PipelineSpec`

```python
class PipelineSpec[P: BaseModel](BaseSpec):
    name: str                        # logical name; says nothing about backend or location
    params: type[P] | None = None    # None where the backend accepts no run parameters
```

Same value as `AnalyticsSpec`: Airflow's `conf` is untyped JSON, and here run parameters become a validated model in the handler's signature.

## 4. Ports

```python
class BasePipelinePort[P](Protocol):
    spec: PipelineSpec[P]

    @property
    def tenant_aware(self) -> bool: ...            # definitions resolve per tenant
    @property
    def accepts_parameters(self) -> bool: ...      # False for Airbyte — see 0033
    @property
    def supports_idempotency_key(self) -> bool: ...  # True only where the backend itself rejects duplicates
```

Three capability properties rather than one, because validation found three independent axes along which backends diverge — a backend can accept parameters and not idempotency keys, or neither. All three follow the shipped `tenant_aware` precedent: a descriptive property a caller can lean on to fail closed. The naming matches existing convention in the tree (`tenant_aware` bare, `supports_*` prefixed).

```python
@runtime_checkable
class PipelineRunPort(BasePipelinePort[P], Protocol[P]):
    def trigger(self, params: P | None = None, *,
                idempotency_key: str | None = None) -> Awaitable[RunHandle]: ...
    def status(self, handle: RunHandle) -> Awaitable[RunStatus]: ...
    def cancel(self, handle: RunHandle) -> Awaitable[None]: ...
    def find_runs(self, *, states: Sequence[RunState] | None = None,
                  since: datetime | None = None,
                  pagination: PaginationExpression | None = None,
                  ) -> Awaitable[CountlessPage[RunStatus]]: ...
```

`trigger` raises `CapabilityNotSupported` when an idempotency key is supplied to a backend that cannot honor it — **never a silent duplicate run** — and `DuplicateRun` when the key already exists. Same refusal when `params` are supplied and `accepts_parameters` is false; that one is caught at **wiring time**, not call time, because binding a non-empty params model to Airbyte is a configuration error and not a runtime condition.

`cancel` is best-effort and asynchronous **everywhere**: a successful call means the request was accepted, not that the run stopped. This is the same cooperative-cancellation doctrine durable run control argued for the durable tier — no hard kill, poll `status` for the outcome — and the consistency is deliberate.

### 4.1 Two modes, one plane

§2.1's precondition — the remote definition must carry no schedule — is a requirement of **triggering**. Observation needs no such thing: `status`, `find_runs` and `logs` cannot cause a double run because they cause no run at all, and are therefore safe against a pipeline someone else schedules and owns.

Bundling the two would make the whole plane unusable against an orchestrator forze does not control. Splitting them costs one protocol and one config field, and makes the plane serve both deployments:

```python
class PipelineObservePort(BasePipelinePort[P], Protocol[P]):   # no schedule precondition
    def status(...); def find_runs(...)

class PipelineRunPort(PipelineObservePort[P], Protocol[P]):     # adds trigger/cancel; attested
    def trigger(...); def cancel(...)


class PipelineMode(StrEnum):
    OBSERVE = "observe"    # someone else owns the schedule; forze watches
    MANAGE  = "manage"     # the §1 premise: forze owns the schedule
```

**Enforcement is at wiring, never at call time.** A route declaring `mode=OBSERVE` provides only the observe capability; a handler annotating `PipelineRunPort` against it is refused at freeze, naming the mode. The alternative — one port whose `trigger` raises `CapabilityNotSupported` at runtime in observe mode — is rejected on evidence rather than taste: the codebase already carries a recorded defect of exactly that shape, the CQRS read-only guard that fires at call time rather than resolve time, so a route's misuse surfaces when it runs rather than when it deploys. Repeating that pattern in a new plane would be choosing a known bug. `check_wiring`'s dry-run already resolves every operation at startup, which is the machinery this needs.

**The attestation becomes mode-conditional, and this is what makes two modes cheap.** The §2.1 check runs in both modes and its verdict inverts:

| | `MANAGE` | `OBSERVE` |
|---|---|---|
| Remote definition schedules itself | **refuse to wire** (`RemoteSchedulePresent`) | expected — recorded, not refused |
| Remote definition has no schedule | wire | recorded as "externally driven, currently idle" — worth knowing when asking why nothing has run |

One check, one implementation, opposite consequences. The mode does not duplicate machinery; it parameterises a check that has to exist anyway.

**Scope of the mode:** per route, not per tenant. A pipeline that is forze-owned for one tenant and externally owned for another has two owners for one logical pipeline, which is a modelling error rather than a configuration to support. Changing mode is a redeploy, because everything about it is freeze-time.

**The back-door double-run is closed in the kit:** RFC 0032 refuses to attach a forze schedule to an `OBSERVE` route, so the mode cannot be used to sidestep the attestation and then schedule the pipeline anyway.

**The payoff reaches beyond this plane.** RFC 0029 §2.1 asks how a service learns that a relation it did not build is stale; an `OBSERVE` route answers the *causal* half — is the upstream job that builds this table healthy, and when did it last succeed — for the same deployment where 0029's `verify` mode answers the schema half. That is the external-warehouse topology of RFC 0028 §1's coexistence clause, served by the same contract that serves the owned one.

### 4.2 Optional protocols

```python
class PipelineCatalogPort(Protocol):            # NOT spec-bound: catalogs are per-connection
    def list_definitions(self) -> Awaitable[Sequence[PipelineDefinition]]: ...

class PipelineLogPort(BasePipelinePort[P], Protocol[P]):
    def logs(self, handle: RunHandle) -> AsyncGenerator[str]: ...
```

## 5. Deliberately out of scope

| Not included | Why |
|---|---|
| Graph definition, task composition | Lives on the remote side. A port describing DAG structure stops being portable the moment a second backend exists |
| Task-level retry policy | Belongs to the definition, not to the call |
| Blocking wait for completion | A composition of `status` + sleep, and its home is durable-execution workflow code. Inside an adapter it holds a connection for hours |
| Receiving callbacks / webhooks | The reverse direction is already closed by the inbox contract; nothing to publish here |
| Schedule management on the orchestrator side | The premise is that Forze owns the schedule. An API that wrote schedules to the remote side would contradict §2.1's gate |

## 6. Tenancy

Definition names resolve through the resolver vocabulary already used for document routing:

```python
AirflowPipelineConfig(
    definition=lambda tid: f"cdm_build__{tid.hex[:8]}",   # namespace
    # definition="cdm_build",                             # tagged
)
```

| Tier | Mechanism | Caveat |
|---|---|---|
| `tagged` | shared definition, tenant in parameters | **Weaker here than for stores** — the definition's author on the remote side can reach anything the orchestrator can reach, including its own metadata database |
| `namespace` | per-tenant definition-name resolver | the working minimum where a tenant is someone else's business |
| `dedicated` | routed client resolving endpoint and credentials per tenant | separate instance or group |

The docstring obligation is to say plainly that **the `tagged` ceiling is lower for orchestrators than for stores**: Airflow's multi-team support is marked experimental and its own documentation declines to call it multi-tenancy, AWS states in the MWAA documentation that Airflow is not multi-tenant, and for Airbyte `tagged` is unreachable in principle (RFC 0033). Hence the default recommendation: `required_tenant_isolation="namespace"` for any pipeline deps module.

This is the same reasoning RFC 0015 used to refuse `tagged` for dynamic read and RFC 0018 used to *allow* it for lake tables — the criterion is verifiability, and here the framework cannot verify what a remote definition does.

## 7. Error taxonomy

| Exception | When |
|---|---|
| `PipelineDefinitionNotFound` | no such definition remotely, or the resolver produced a name that does not exist |
| `RunNotFound` | the handle points at an unknown run |
| `DuplicateRun` | a run with this idempotency key already exists |
| `CapabilityNotSupported` | a capability the backend lacks was requested (idempotency key, parameters) |
| `OrchestratorUnavailable` | backend unreachable or transport error |
| `RemoteSchedulePresent` | §2.1 attestation failed — the definition schedules itself |

Separating `DuplicateRun` from `CapabilityNotSupported` is essential and easy to get wrong: the first is the **normal, successful outcome** of an idempotent retry, the second is a wiring error to fix rather than retry. RFC 0025's disposition ladder is where their retryability is ultimately classified; `OrchestratorUnavailable` is the one that must not become permanently retryable infrastructure noise.

## 8. Acceptance battery

1. Full lifecycle: trigger → `PENDING` → `RUNNING` → `COMPLETED`, handle round-trips through `status`. *(mock ≡ real Airflow)*
2. Typed params validate and reach the run; a params model bound to a backend with `accepts_parameters=False` is refused **at wiring**. *(unit + real)*
3. Idempotency on Airflow: duplicate `dag_run_id` → `DuplicateRun` from the backend itself; on a backend without support, supplying a key raises `CapabilityNotSupported` and **no run starts** — asserted by run count, not by the exception alone. *(real + mock)*
4. **§2.1 attestation**: a DAG with a live schedule refuses to wire with `RemoteSchedulePresent`; the same DAG at `schedule=None` wires. The double-run failure this prevents is itself pinned — with attestation disabled, a scheduled remote definition plus a forze tick produces two runs. *(real Airflow — the executable form of the spec's warning)*
5. `logical_date` is always present in the Airflow request body; a run triggered with the key absent reproduces the documented 422, pinning why the adapter substitutes it. *(real Airflow)*
6. Cancellation: `cancel` returns after acceptance, not completion; polling reaches a terminal state; on Airflow the outcome is honestly `FAILED` with the inability to distinguish pinned as a documented-limitation test. *(real)*
7. `find_runs` filters by state and `since`, pages, newest first. *(mock ≡ real)*
8. Tenancy: two tenants resolve to different definitions; `tagged` wiring requires an explicit acknowledgement below the recommended floor. *(unit)*
9. Taxonomy: unknown definition, unknown run, backend down — each maps to its own code, and `OrchestratorUnavailable` is classified for retry the way RFC 0025 expects. *(real)*
10. DST: lost runs, states that never advance, cancellations that never arrive, and a status flap — the mock's fault-injection contour, which is the reason the mock is P1 and not "someday". *(mock)*
11. **Mode enforcement is freeze-time**: a handler annotating `PipelineRunPort` against an `OBSERVE` route fails `check_wiring` with a message naming the mode — and the negative form matters as much, so the test also asserts the route is never reachable at call time to raise there instead. *(unit)*
12. **Mode-conditional attestation**: the same self-scheduling DAG refuses to wire under `MANAGE` and wires under `OBSERVE` with the fact recorded; a schedule-free DAG does the converse. One check, both verdicts. *(real Airflow)*

## 9. Phases

- **P1** — contract + value objects + errors + **both protocols and `PipelineMode`** (§4.1) + **mock adapter** + battery 1–3, 7, 10–11. The protocol split is P1 rather than a later addition because retrofitting a narrower protocol under a wider one after adapters exist is a breaking change; doing it at the start costs one `Protocol` declaration. The mock is P1 deliberately: it makes domain tests possible without a running orchestrator, and for a framework whose promise is reproducibility that is the point, not a convenience.
- **P2** — Airflow 3.x adapter (`forze_airflow`) + §2.1 attestation in both modes + freeze-time mode enforcement + battery 4–6, 8–9, 12. One real leg lands with the head RFC per the mock-horizon rule; the rest is RFC 0033. **Airflow is confirmed as the first backend.**
- **P3** — optional `PipelineCatalogPort` / `PipelineLogPort`.

The contract ships marked **experimental for a couple of minor releases**, per the source specification's recommendation.

## 10. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Forze owns the schedule; the orchestrator is an externally-triggered executor. The premise is the whole design and every consequence below follows from it | locked |
| 2 | **§2.1 is a startup gate, not a documented precondition** — all three backends expose their own schedule over APIs the adapter already speaks, so the double-run hazard is refused at wiring rather than warned about. Dagster sensors make its attestation best-effort, and the capability says so. In `OBSERVE` mode the same check runs with an inverted verdict | **locked** (upgrade of the source draft, confirmed 2026-08-03) |
| 3 | `logical_date` removed from the port — the window is a parameter; Airflow's mandatory-key requirement is the adapter's problem | locked |
| 4 | `PipelineBackfillPort` removed and `trigger_many` declined — backfill is a loop of `trigger` calls once the window is a parameter (converging with RFC 0030's "backfill is a parameter, never a subsystem") | locked |
| 5 | `outputs` removed from `RunStatus`; results are read from storage, the port observes only lifecycle; `PipelineSpec` loses its second type parameter | locked |
| 6 | **`SUCCEEDED` → `COMPLETED`** — both existing run-state enums spell it that way, and durable run control exists because vocabulary asymmetry under one umbrella is a real cost. Separate enum, aligned spelling | **locked** (change to the source draft, confirmed 2026-08-03) |
| 7 | `definition` is an opaque adapter-encoded address, never parsed by callers — Dagster's triple selector settles it | locked |
| 8 | Three capability properties, not one; `CapabilityNotSupported` over silent degradation; params/backend mismatch caught at wiring | locked |
| 9 | Adapters never fake `CANCELLED` where the backend cannot express it; the kit's run record carries the intent instead (RFC 0032) | locked |
| 10 | `required_tenant_isolation="namespace"` recommended for pipeline modules; the `tagged` ceiling is lower for orchestrators than for stores and the docstrings must say so | locked |
| 11 | Mock ships in P1 with a fault-injection contour; one real adapter (Airflow) ships with the head RFC per the mock-horizon rule | locked |
| 12 | **`PipelineObservePort` splits from `PipelineRunPort`, selected by a per-route `PipelineMode`** — observation carries no schedule precondition, so the split makes the plane usable against orchestrators forze does not own, and feeds RFC 0029 §2.1's freshness question | **locked** (addition to the source draft, confirmed 2026-08-03) |
| 12a | Mode is enforced **at wiring, never at call time** — the recorded CQRS read-only-guard defect (fires at call time, not resolve time) is the evidence against the one-port-that-raises design; `check_wiring` already resolves every operation at startup | locked |
| 12b | Mode is **per route, not per tenant** — one logical pipeline with two owners is a modelling error, not a configuration; changing mode is a redeploy | locked |
| 13 | Backend facts (Airflow `api/v2` and the `logical_date` 422, Airbyte endpoints, Dagster's server-assigned `runId`) confirmed against current documentation at drafting; **re-verify at pickup** per RFC 0033's protocol | recorded |
