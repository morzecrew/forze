# RFC 0047 — Durable MockState — persistence for the in-memory backend

- **Status:** ✅ Complete — shipped 2026-09-09
- **Scope:** An opt-in snapshot/restore file for `MockState`, so an application wired on `forze_mock` keeps its data across a process restart. **No new package, no new adapter, no new implementation of any port** — the mock's semantics, capabilities and adapters are untouched, and nothing joins the conformance battery. What is added is a lifecycle step that loads a file at startup and writes it at shutdown, plus a three-way classification of the 46 state fields into *persist*, *reset* and *drop*. Explicitly **not** a database: single process, whole dataset in RAM, and everything since the last flush is lost on `kill -9`. Explicitly **not** the DST substrate's problem — persistence is off by default and simulation never enables it.
- **Related:** [`src/forze_mock/state.py`](../src/forze_mock/state.py) (the 46-field state and its existing `snapshot_tx_stores` / `restore_tx_stores`), [`src/forze_mock/execution/module.py`](../src/forze_mock/execution/module.py) (`MockDepsModule.state`), [`src/forze_mock/tenancy/routed.py`](../src/forze_mock/tenancy/routed.py) (`MockRoutedStateRegistry`, an LRU of per-tenant states), [`src/forze_dst/engines/crash_restart.py`](../src/forze_dst/engines/crash_restart.py) (already treats `MockState` as the store that survives a crash), [`src/forze/application/contracts/execution/value_objects.py`](../src/forze/application/contracts/execution/value_objects.py) (`LifecycleStep`), RFC 0012 (the demand gate this RFC must clear, and the reason it clears it differently).
- **Origin:** A question about adding SQLite (or DuckDB) document adapters to cover MVP persistence without a Docker Compose stack. The measurement pass that followed rejected both engines for that job and found the gap is durability, not a missing backend.

---

## 1. Summary

`forze_mock` is the framework's zero-infrastructure backend and it forgets everything on exit. That makes it correct for tests, where fresh state per test is the point, and unusable for *running* an application — an MVP restarts and its orders are gone.

This RFC adds durability to the backend that already exists, rather than a new backend that would be durable. A `MockStatePersistence` object serializes the state's data fields to one file on shutdown and reloads them on startup, wired through an ordinary `LifecycleStep`. Three of the 46 fields are live concurrency primitives and are rebuilt rather than restored; four more carry process-local runtime state whose restoration would be a bug, and are reset deliberately. The rest — documents, outbox rows, inbox marks, counters, object bytes, identity, durable runs — round-trip.

The result is that every plane the mock implements becomes durable at once, at roughly two hundred lines, with no new implementation for any contract change to be proven against ever again.

## 2. Motivation

**The gap is real and it is not the one the mock was built for.** `forze_mock`'s own docstring says it is "intended for development and tests". Those are two needs, not one. A test wants fresh state per case and gets it. Development — an MVP someone is actually clicking through — wants its data on Tuesday to still be there on Wednesday, and the only answers today are `docker compose up postgres` or losing the data.

**The framework already specifies the semantics this RFC needs.** `forze_dst/engines/crash_restart.py` builds its deps modules once so that "the `MockState` they hold is the durable store" and then restarts a *fresh runtime* over the same state, asserting recovery invariants across the boundary. The notion "the mock is the durable store across a restart" is therefore not new, and not merely asserted: it is exercised. What is missing is the last step from *same object, new runtime* to *same file, new process*.

**The state is already proven copyable.** `MockState.snapshot_tx_stores()` / `restore_tx_stores()` do full `copy.deepcopy` round-trips today, for strict-mode transaction rollback ([`state.py:408`](../src/forze_mock/state.py), [`state.py:428`](../src/forze_mock/state.py)). Persistence is the same operation with a different destination.

**Measured, not assumed.** A `pickle` round-trip of the state's data fields, populated with documents, a tuple-keyed counter, an inbox mark, raw object bytes and an idempotency record:

```
46 fields, 43 serialized (3 live locks skipped)
round-trip identical: True
```

**Why not SQLite or DuckDB.** DuckDB was measured out: `CREATE INDEX` on a JSON expression fails (`Binder Error: Cannot use json_extract_string in this context`), so every document filter is a full scan, and a second OS process cannot open the file at all (`IO Error: Could not set lock on file`) — which [`tenancy-matrix.md`](../pages/docs/reference/tenancy-matrix.md) already records as DuckDB's `tagged` (in-process) ceiling. SQLite is genuinely capable — it expresses the entire filter DSL including nested quantifiers, field-to-field comparison and path prefixes, indexes JSON expressions, and supports multi-process writes — but it is a *from-scratch* backend: the Postgres renderer is `jsonb`/`@>`/`unnest`/`ltree` throughout, so nothing is reusable, and Firestore (4,762 lines for a deliberately partial document backend) is the honest floor. That buys durability for the three or four planes worth porting. This buys it for all of them, at two orders of magnitude less code.

## 3. Current state

Verified against the tree, not remembered.

**`MockState` is 46 attrs fields.** Enumerated live: 43 hold plain data (`dict` / `set` / `list` / `int` of plain values and `bytes`); exactly three hold live objects — `_MockState__lock` (a `threading.RLock`), `_MockState__tx_serializer` (an `asyncio.Lock | None`) and `rotating_credential_locks` (a `StripedAsyncLocks`).

**Four data fields must not be restored, and it is not obvious from their types.**

| Field | Why restoring it is wrong |
|---|---|
| `dlocks` | Holder records carry expiry stamps taken from `monotonic()` ([`adapters/dlock.py:61`](../src/forze_mock/adapters/dlock.py)). A monotonic clock has no meaning across processes: a restored lock expires at an arbitrary point, possibly never. |
| `mvcc_active` | In-flight transaction ids. Restoring them resurrects transactions that no longer exist and that nothing will ever end. |
| `mvcc_commit_log`, `mvcc_version` | The commit log's snapshot bookkeeping is only meaningful relative to the active set above. |

**Two fields are private and easy to skip, and one of them must not be.** `_MockState__seq` is the monotonic id sequence ([`state.py:354`](../src/forze_mock/state.py)); dropping it restarts ids at 1 and collides with restored rows. The naive "skip everything private" filter drops it. Likewise `dlock_fences` is a monotonic fence counter per route — persist it or fencing tokens regress, which is the exact failure fencing exists to prevent.

**`authn_events: list[AuthnEvent]` holds domain objects**, not plain data. It serializes, but it couples the file to a class definition.

**Two fields are test observability**, not application state: `tx_read_only_calls` and `storage_presigns`. They exist so a test can assert issuance; a running application does not need them across a restart.

**The wiring seam exists.** `MockDepsModule.state` is an injectable `MockState` ([`module.py:266`](../src/forze_mock/execution/module.py)), and `LifecycleStep` carries `startup` / `shutdown` hooks with a `mutates_shared_state` honesty marker ([`value_objects.py:196`](../src/forze/application/contracts/execution/value_objects.py)). Nothing new is needed to hang load/save on.

**`MockRoutedStateRegistry` is an LRU** of per-tenant states with `max_entries: int = 128` ([`routed.py:32`](../src/forze_mock/tenancy/routed.py)). It **evicts**, and eviction currently just drops the state.

## 4. Goals / Non-goals

**Goals**

- An application wired on `forze_mock` keeps its data across an ordinary stop and start.
- Durability reaches every plane the mock implements, not a chosen subset.
- Off by default, and invisible to every existing test and simulation.
- No new implementation of any port, and therefore no new seat in any conformance battery.

**Non-goals**

- **Crash durability.** A `kill -9`, an OOM kill or a power loss discards everything since the last flush. This is a snapshot, not a write-ahead log.
- **A second process.** One process owns the file. Concurrent access is refused, not coordinated.
- **A stable on-disk format.** The file is disposable development data whose schema is `MockState` itself.
- **A migration rehearsal.** Running on this teaches nothing about how Postgres will behave.
- **Replacing the mock's role in DST.** Simulation determinism is the mock's other job and this must not touch it.

## 5. Design

### 5.1 The field classification is the design

Everything else is plumbing. Fields fall in three buckets, declared as explicit frozensets on the persistence object rather than derived by a rule, because two of the three buckets exist precisely because a rule would get them wrong:

- **PERSIST** — the application's data. Everything not named below.
- **RESET** — restored to a fresh value: the three live primitives, plus `dlocks`, `mvcc_active`, `mvcc_commit_log`, `mvcc_version`. A restart means every lock holder and every in-flight transaction is gone; the honest post-restart state is *none held, none active*.
- **DROP** — not written at all: `tx_read_only_calls`, `storage_presigns`. Test observability.

`_MockState__seq` and `dlock_fences` are in PERSIST and the reason is written next to them, because both look droppable and both cause silent corruption when dropped.

**The classification is exhaustive and checked.** A field belonging to no bucket is an error at import, not a field quietly skipped — so adding a substore to `MockState` without deciding its disposition fails loudly. This is the one rule that keeps the design true as the mock grows.

### 5.2 Format and writing

`pickle`, at `HIGHEST_PROTOCOL`, of a `{field_name: value}` mapping, preceded by a header carrying a **format version and a fingerprint of the field set**. On load, a mismatch is a clean refusal naming the file, not an attempted migration: the data is disposable and pretending otherwise is how a development convenience becomes an obligation.

Pickle rather than JSON because the state is not JSON-shaped — `counters` is keyed by `tuple[str, str | None]`, `inbox` is a `set` of tuples, `storage_bytes` holds raw `bytes`. Rather than msgspec/msgpack with a tuple-key encoding because that is a serialization layer to maintain for a file that is explicitly disposable. The cost is stated plainly in §9.

Writes go to a temporary file in the same directory and are `os.replace`d into position — atomic on one filesystem, so a crash during the write leaves either the old snapshot or the new one, never a torn file.

### 5.3 When it writes

- **Shutdown**, via the lifecycle step's `shutdown` hook. The ordinary path.
- **Periodically**, on an interval (default off, `flush_every` to enable), because "shutdown only" means a `kill -9` costs the whole session rather than the last few minutes.

A flush takes the state's existing `lock` for the copy and serializes outside it, so a large snapshot does not hold the mutex for the duration of the write.

### 5.4 Wiring

```python
persistence = MockStatePersistence(path=Path(".forze/mvp.state"))

lifecycle = LifecyclePlan.from_steps(
    mock_state_lifecycle_step(state=state, persistence=persistence),
)
deps = DepsRegistry.from_modules(MockDepsModule(state=state))
```

The step's `startup` loads (a missing file is a fresh start, not an error) and its `shutdown` writes. `mutates_shared_state` is `False`: the file is process-local, not shared infrastructure.

### 5.5 Single-writer enforcement

Startup takes an advisory `flock` on the file and holds it for the process's life. A second process gets a clean configuration refusal naming the holder's pid rather than two processes overwriting each other's snapshots on the way out. This is the whole of the concurrency story and it is deliberately a refusal rather than a mechanism.

### 5.6 Tenant-routed state

`MockRoutedStateRegistry` is an LRU that evicts. Persisting each tenant's state to `<dir>/<tenant_id>.state` would work, but eviction-time flush turns a cache into a write path and an eviction storm into an I/O storm.

V1 **refuses the pairing**: persistence configured against a routed registry raises at wiring. The single-state case is the MVP case; a routed MVP is already past the point this design serves. Recorded as a decision rather than an omission so the next reader knows it was considered.

### Alternatives considered

**Write-through to SQLite instead of a snapshot.** Real crash durability, and the file would be inspectable. Rejected for v1: write-through means every mutating adapter path grows a persistence call, which is the change this RFC exists to avoid — the whole argument for durability-over-the-mock is that it touches no adapter.

**Persist by wrapping the adapters rather than the state.** More granular, and it would give per-write durability. Rejected: 46 substores are reached by dozens of adapters, and the state object is the one place they all already meet.

**Do nothing; document `docker run -d postgres`.** The honest baseline, and it stays the right answer for anyone who can run a container. It loses only the case where the container is the thing being avoided — which is the case that prompted this.

## 6. Tests

- **Round-trip fidelity, against a state the conformance batteries populated** rather than a hand-built one. A state built by hand exercises the fields the author remembered; the battery reaches the substores they did not. The assertion is field-by-field equality across PERSIST.
- **Exhaustiveness.** A test that every `attrs` field of `MockState` appears in exactly one bucket — this is what catches the next substore someone adds.
- **The RESET fields are reset.** Save a state holding a live dlock and an active MVCC transaction id; restore; assert no lock is held and no transaction is active. Both directions matter: a test that only asserts the data came back would pass with `dlocks` restored.
- **`__seq` and `dlock_fences` do not regress.** Allocate ids, save, restore, allocate again, assert strictly increasing. This is the test that fails if someone "cleans up" the private-field handling.
- **Atomicity.** Interrupt a write between the temp file and the replace; assert the previous snapshot is intact and loadable.
- **The DST crash-restart engine still passes** with persistence unconfigured — the regression that matters most, since the mock's determinism role is the thing this must not disturb.
- **Second-process refusal**, with a real second process, not a mocked lock.

## 7. Docs

One section on the `forze_mock` integration page: what it is, the two-line wiring, and the four limits from §4 stated before the wiring rather than after it. The framing is "your MVP survives a restart", never "a database" — the failure mode of this feature is someone believing the latter.

## 8. Out of scope

- **Write-ahead logging / crash durability.** Would change the design from a snapshot to a log, and the file from disposable to owned. Revisit if someone reports losing work to a crash rather than to a restart.
- **A stable, versioned, migratable format.** Only if the file outlives a development session, which it is designed not to.
- **Multi-process.** Requires coordination the mock has no reason to grow. The upgrade path is Postgres.
- **The routed (per-tenant) registry.** §5.6. Unblocked by a consumer with a multi-tenant MVP, and by deciding the eviction-flush question.
- **SQLite or DuckDB document adapters.** Rejected for this problem by the measurements in §2. A SQLite backend remains a reasonable *product* decision for a named single-node deployment; it is not the way to give an MVP persistence.

## 9. Risks

- **Someone ships it.** The whole risk. A durable mock is exactly durable enough to feel production-ready and is not — no crash durability, no second process, no query planner, everything in RAM. Mitigation is naming, defaults and documentation, all of which are weaker than the temptation. This is the risk to weigh when deciding whether to build it at all.
- **Pickle couples the file to the code.** A `MockState` refactor or an `AuthnEvent` change invalidates existing files. Mitigated by the fingerprint check refusing cleanly, and by the format being disposable by declaration — but a user who has been running an MVP for three weeks will not experience "disposable" as a design property.
- **Pickle deserializes arbitrary objects.** The file is written by the application itself, so this is only a risk if an untrusted file is placed at the configured path. Documented; not defended against, because a user who can write that path can write the application.
- **The classification rots.** A new substore is added and lands in PERSIST by default when it should reset. Mitigated by §6's exhaustiveness test making the choice explicit, not by hoping.
- **Perceived contradiction with RFC 0012.** That RFC demand-gates `forze_localfs` and records "fewer dev containers" as explicitly *not* a trigger. The distinction is the standing cost it names: a new backend joins the mock↔real conformance battery **forever**, so every future contract change is proven across one more implementation. This RFC adds **no implementation** — the mock's adapters, semantics and capabilities are untouched, and the battery count does not move. The gate 0012 sets is cleared by not being the thing it gates.

## 10. Unresolved questions

- **Does `flush_every` default on or off?** Off is honest (one behaviour, one moment of I/O); on is what a user actually wants after their first `kill -9`. Left to implementation.
- **Is `authn_events` worth persisting at all?** It is an audit trail whose value across a restart is unclear, and it is the one field holding domain objects. Dropping it would remove the only class-coupling in the file.
- **Does the lock file survive an unclean exit?** `flock` is released by the OS on process death, so a crashed holder does not wedge the path — verify rather than assume, since the whole point of the lock is the case where the previous process did not exit cleanly.

## 11. Decisions

| # | Decision | Grade |
|---|---|---|
| 1 | Durability is added to `forze_mock`, not obtained by building a SQLite or DuckDB document backend. | LOCKED |
| 2 | No adapter, port, capability or semantic of the mock changes; nothing new joins any conformance battery. This is what clears RFC 0012's demand gate, and giving it up reopens that gate. | LOCKED |
| 3 | Fields are classified three ways — PERSIST / RESET / DROP — declared explicitly, and a field in no bucket is an import-time error. | LOCKED |
| 4 | `dlocks`, `mvcc_active`, `mvcc_commit_log` and `mvcc_version` are RESET: monotonic clock values and in-flight transaction ids have no meaning in a new process. | LOCKED |
| 5 | `_MockState__seq` and `dlock_fences` are PERSIST despite looking like internals: both are monotonic counters whose regression corrupts silently. | LOCKED |
| 6 | Persistence is off by default and DST never enables it; the mock's determinism role is unchanged. | LOCKED |
| 7 | Format is pickle with a version + field-set fingerprint; a mismatch refuses cleanly and never migrates. | ASSUMED |
| 8 | Writes are temp-file + `os.replace`, and a snapshot is taken under the state lock but serialized outside it. | ASSUMED |
| 9 | A single process owns the file, enforced by an advisory `flock` and a clean refusal naming the holder. | ASSUMED |
| 10 | Persistence paired with `MockRoutedStateRegistry` is refused at wiring in v1 rather than flushed on eviction. | ASSUMED |
| 11 | `tx_read_only_calls` and `storage_presigns` are DROP — test observability, not application state. | ASSUMED |
| 12 | Whether `flush_every` defaults on, and whether `authn_events` is persisted at all. | OPEN |
| 13 | Resolves the first half of row 12: `flush_every` defaults to `None` (off). A periodic flush needs a background task, so setting it makes the lifecycle step `requires_long_running`, which a `SERVERLESS` deployment profile refuses at assembly — a default that fails a whole profile is not a default. Added by execution 2026-09-09 — see `logs/T-0047.md` (D-12). | ASSUMED |
| 14 | Resolves the second half of row 12: `authn_events` is DROP. The field's own declaration calls it test observability and the deps module fills it from a recording sink wired for seed-style inspection, so it belongs beside `tx_read_only_calls` rather than beside application data. Added by execution 2026-09-09 — see `logs/T-0047.md` (D-12). | ASSUMED |
| 15 | Departs from row 9 on where the lock lives: the advisory `flock` is taken on a sibling `<path>.lock`, not on the snapshot. Row 8's `os.replace` swaps the inode, so a lock held on the snapshot stops guarding the path after the first flush — measured, `inode before=654539 after=654540`. The holder writes its pid into the lock file, since `flock` itself names no holder. Added by execution 2026-09-09 — see `logs/T-0047.md` (D-9). | ASSUMED |
| 16 | Departs from row 10 on where the refusal fires: the routed-registry pairing is refused at **startup**, from the lifecycle hook, not at wiring. `MockStatePersistence` and `MockRoutedStateRegistry` never meet at wiring, so a wiring-time check would have to be handed the very registry it exists to refuse; the hook tests `ctx.deps.exists(MockRoutedStateDepKey)`, which the deps module registers only when a routed registry is configured. Added by execution 2026-09-09 — see `logs/T-0047.md` (D-10). | ASSUMED |
| 17 | A restore refills the existing containers in place and rebinds only scalars. Adapters are constructed when the deps module is built, before lifecycle startup runs, so each already holds a reference to the substore it was given — the discipline `restore_tx_stores` follows for the same reason. Added by execution 2026-09-09 — see `logs/T-0047.md` (Unlisted). | LOCKED |
| 18 | Supersedes row 8 in part: the capture runs where the mock's other writers run — the event loop — and only the serialization and the file write may leave it. The state lock does not settle the question, because `MvccTx.commit` and a journal rollback both publish into `state.documents` without taking it. Splitting `save` into `capture` and `write` is what makes the boundary expressible. Added by execution 2026-09-09 — see `logs/T-0047.md` (Unlisted, found while building). | LOCKED |
| 19 | The header opens with a magic line, so an unrelated file at the same path is refused as "not a mock state snapshot" before anything reaches the unpickler — separately from a snapshot this build cannot read. Row 7's version and fingerprint distinguish snapshots from each other, not a snapshot from an arbitrary file. Added by execution 2026-09-09 — see `logs/T-0047.md` (Unlisted, found while building). | ASSUMED |
| 20 | The documentation section lands in `pages/docs/recipes/mock-server.md`, under its existing `## Limits`. The `forze_mock` integration page the RFC's Docs section assumes does not exist. Added by execution 2026-09-09 — see `logs/T-0047.md` (Unlisted). | ASSUMED |
| 21 | §6's last test is met by running the existing DST suite as evidence rather than copying it: persistence is a module nothing imports unless it is wired, so a duplicate would assert nothing new. The one real coupling — `forze_mock.__init__` now imports the module, so its import-time classification check runs for every DST import — is what that run exercises. Added by execution 2026-09-09 — see `logs/T-0047.md` (Answering §6's last test). | ASSUMED |

## 12. Phasing

One PR. The classification, the persistence object, the lifecycle step, the tests from §6 and the docs section land together — a half-built classification is worse than none, since the failure mode is silent.

Nothing is gated on this and it gates nothing. If it is not built, the answer to "how does my MVP keep its data" stays `docker run -d postgres`, which is a defensible answer.
