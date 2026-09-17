# RFC 0065 — Snapshot-consistent export and its verification

- **Status:** 📝 Draft — **narrowed by verification.** The source proposal asked for a backup manifest with counts, a schema fingerprint, a version pair and a `verify` that restores and diffs; §3 found four of those five already shipped on the portability plane. What is missing is the consistent snapshot and the version gate, and that is what this RFC is.
- **Scope:** A fourth `Consistency` level — `snapshot` — for a Postgres-backed export: counts and rows read inside one `REPEATABLE READ` transaction, the exported snapshot id recorded so an out-of-band `pg_dump --snapshot` can be taken against the same view, a client/server major-version gate, and a DST round-trip battery over the mock. Touches `forze_kits.integrations.portability` (the manifest's consistency vocabulary and the exporter's transaction handling) and `forze_postgres` (the snapshot helper). **No new plane, no new port**, and explicitly not a backup feature.
- **Related:** [`src/forze_kits/integrations/portability/__init__.py`](../src/forze_kits/integrations/portability/__init__.py) (the plane's doctrine: portability, **not** backup; plane-completeness; refuses what it cannot account for), [`manifest.py:29`](../src/forze_kits/integrations/portability/manifest.py) (`Consistency = "tenant" | "quiesced" | "fuzzy"`), `:47-51` (`ArchiveFile.sha256`, `rows`, and the order-independent `content_digest`), `:112-122` (`format_version`, `forze_version` — "diagnostic, not a compatibility gate" — and `registry_fingerprint`, which import **does** gate on), [`determinism.py:45-71`](../src/forze_kits/integrations/portability/determinism.py) (`run_manifest`, `compare_content`, and `unknown` — "could not compare is not equal"), [`export.py:591`](../src/forze_kits/integrations/portability/export.py) (`export_archive`), [RFC 0017](0017-postgres-copy-bulk-load.md) (the plane's file-artifact doctrine this inherits).
- **Origin:** A working-time ledger whose `backup.py` writes a manifest (dump hash, schema revision, PostgreSQL version, app version, per-table counts) and whose `verify` restores into an empty database and checks all twelve domains — with two defects: the counts are taken **outside** the dump's snapshot, so a live backup fails its own verification, and client and server major versions are never compared.

---

## 1. Summary

The portability plane can already say what it exported, prove two exports are equal and refuse an
import into a differently-shaped application. What it cannot say is that one export is a
*consistent* view: today an export of a live system is labelled `fuzzy`, honestly. This RFC adds
the level where the label can be stronger — one `REPEATABLE READ` transaction for every read,
including the counts — plus the version gate the origin application lacked, and a DST battery that
exercises export-then-import under crash.

## 2. Motivation

The origin application's bug is instructive because the manifest looked complete. Counts were
taken in one transaction and the dump in another, so a backup of a live system verified against a
row count that was never true at any single instant, and the verification failed for a *correct*
dump. A verification that fails on healthy input is worse than none: it trains the operator to
skip it.

Forze does not have that bug, because it does not make the claim — a live export is `fuzzy` and
says so. The gap is that `fuzzy` is the only available answer on a running system, so an operator
who wants a defensible point-in-time artifact has to quiesce the application.

## 3. Current state

**Four of the five requested pieces ship.**

| Requested | Shipped |
| --- | --- |
| per-file hash | `ArchiveFile.sha256` |
| logical comparison | `ArchiveFile.content_digest`, order-independent over canonical rows |
| schema fingerprint | `Manifest.registry_fingerprint`; **import refuses** a target whose fingerprint differs |
| restore-and-diff | `run_manifest` + `compare_content`, with `unknown` when a digest is missing on either side — "could not compare" is never "equal" |
| consistent snapshot | **missing** — `Consistency` is `tenant \| quiesced \| fuzzy` |

**The version pair is deliberately not a gate.** `Manifest.forze_version` is documented as
"diagnostic, not a compatibility gate", while `registry_fingerprint` is the gate. That is a
considered split — the shapes matter, the framework version does not — and it says nothing about
the *server's* version, which is not recorded at all.

**The plane is not a backup, and says so twice.** The package docstring: durability stays the
backend's job (WAL / PITR / snapshots), and a file artifact is plaintext by construction unless an
`ArchiveSealer` is passed. Any RFC here has to stay inside that doctrine, which rules out most of
what "backup verification" would imply.

**Nothing in `forze_postgres` exports a snapshot id.** So there is no way today to align an
external `pg_dump` with what the plane read.

## 4. Goals / Non-goals

**Goals**

- A consistency level that is true: every read of an export, counts included, from one snapshot.
- A recorded server version and a refusal when the client's major version is older than the
  server's.
- An exported snapshot id, so an out-of-band physical dump can be taken against the same view.
- A DST battery proving export-then-import round-trips under crash, which the plane's own
  conformance does not yet cover.

**Non-goals**

- **Not backup.** Stated in the plane's own docstring and repeated here: no retention, no schedule,
  no PITR, no restore orchestration. This RFC makes one artifact defensible; it does not make the
  plane a backup tool, and the docs must not drift into implying it.
- **Not physical-dump management.** The snapshot id is *exported* for an operator's `pg_dump`; the
  plane does not run it, store it or verify it.
- **Not a cross-backend guarantee.** `snapshot` is Postgres-only. Mongo's and the mock's answers
  stay what they are.
- **Not a new manifest format.** One enum member, two fields.

## 5. Design

### 5.1 The `snapshot` consistency level

`export_archive(..., consistency="snapshot")` opens one `REPEATABLE READ` transaction and performs
**every** read inside it — per-plane streams, per-file row counts, and the count used in the
manifest. That is the origin's defect inverted into the design: a count outside the snapshot is a
count of a different database.

The level is refused when the export's routes do not all resolve to one Postgres client: two
connections are two snapshots, and a manifest claiming otherwise would be the same lie with a
framework's name on it.

### 5.2 The snapshot id

```python
async def snapshot_manifest(client) -> SnapshotHandle   # (snapshot_id, server_version, taken_at)
```

`pg_export_snapshot()` inside the export's transaction, recorded in the manifest. An operator's
`pg_dump --snapshot=<id>` then reads the same view — **while the export's transaction is still
open**, which is the constraint that makes this usable or not (§9). The plane does not manage that
window; it records the id and the time it was taken.

### 5.3 The version gate

`Manifest` gains `server_version`. On import, the **client**'s major version being older than the
recorded server's major version is a refusal, not a warning: an older client reading a newer
server's dump is the case where a type it does not know is silently mis-decoded.

The framework's own `forze_version` stays diagnostic, as decided — this RFC does not reopen that
split, and §11 records why the two fields are treated differently.

### 5.4 The round-trip battery

A DST battery over the mock store: seed, export, import into a second store, compare with
`compare_content`, and inject a crash between the export's last file and the manifest write. The
property is that a partial archive is **rejected on import** rather than half-loaded — which the
plane's refusal doctrine already implies and nothing currently proves.

### Alternatives considered

- **Take the counts in a second transaction and label the level `quiesced`.** What the origin did.
  Cheap, and it produces a manifest that fails its own verification on a live system.
- **Make `forze_version` a gate too.** Symmetric with `server_version` and it would refuse imports
  across framework upgrades that the registry fingerprint already proves compatible.
- **Own the `pg_dump` invocation.** Complete, and it makes the portability plane a backup tool,
  which §4 refuses.
- **A new `backup` plane.** The doctrine says durability is the backend's; a second plane would say
  otherwise by existing.

## 6. Tests

- Snapshot level: a concurrent writer committing mid-export does **not** appear in the archive, and
  the manifest's row count equals the archive's actual rows (the origin's defect, as a regression
  test).
- Refusal when the export spans two clients.
- Snapshot id: recorded, and a second connection using it observes the same view (an integration
  test, since it is a Postgres property being exercised, not asserted).
- Version gate: an older client major refuses; equal or newer imports; `forze_version` differing
  does not refuse.
- Round trip under crash: a crash before the manifest write yields an archive import rejects; a
  clean run round-trips with `compare_content` reporting no `unknown` files.
- **Not tested:** `pg_dump` alignment. The id is exported; what an operator does with it is outside
  the plane.

## 7. Docs

An addition to the portability page's consistency table, and the paragraph that has to stay sharp:
**`snapshot` makes one artifact defensible; it does not make this a backup.** Plus the operational
note that the snapshot id is only usable while the export's transaction is open, with the
implication for long exports.

## 8. Out of scope

- **Retention, scheduling, restore orchestration.** Backup features, refused by doctrine.
- **Snapshot consistency on Mongo.** Its transactions could express it; no consumer has asked, and
  the per-plane read pattern differs.
- **Verifying a physical dump.** The plane has no view of it.
- **Incremental export.** A different design (change data capture), not a consistency level.

## 9. Risks

- **A long-open `REPEATABLE READ` transaction.** It holds a snapshot, which blocks vacuum and grows
  bloat on a busy database; a multi-hour export is an operational hazard. Mitigation: documented,
  with the `quiesced` level as the alternative for large systems, and the snapshot's `taken_at` in
  the manifest so an operator can see how long it was held.
- **"Snapshot" read as "backup".** The single largest risk in this RFC, and the reason §4 and §7
  both say it. Mitigation: the word `backup` does not appear in the feature's API, and the docs
  paragraph is mandatory.
- **The exported snapshot id is only valid inside the window.** An operator who runs `pg_dump`
  afterwards gets an error, which is the safe direction but reads as a broken feature. Mitigation:
  the docs state the window, and `taken_at` makes a stale id diagnosable.
- **A version gate that refuses a working import.** A cautious major-version rule can block an
  import that would have been fine. Mitigation: it compares majors only, and the refusal names both
  versions.

## 10. Unresolved questions

- **Is `snapshot` a level or a flag on `quiesced`?** A fourth enum member is clearer in the manifest;
  a flag avoids a level that only one backend can produce.
- **Does the export hold the transaction open for the operator's dump, and for how long?** Holding
  it is the only way the id is usable, and an unbounded hold is the §9 hazard. A declared maximum
  window, after which the export finishes and the id is marked expired, is the leaning.
- **Should `server_version` be recorded even at other levels?** It is diagnostic there, and cheap.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | This stays the **portability** plane. No retention, no schedule, no restore orchestration, and the word backup does not enter the API — durability remains the backend's job, as the plane's own doctrine states. |
| 2 | `LOCKED` | At the `snapshot` level, **every** read — streams and the manifest's counts — happens inside one transaction. A count taken outside the snapshot is a count of a different database, which is the origin's defect. |
| 3 | `LOCKED` | An export that cannot be served from one Postgres client **refuses** the `snapshot` level. Two connections are two snapshots. |
| 4 | `ASSUMED` | `server_version` is a **gate** on import (client major older than server major refuses) while `forze_version` stays diagnostic. The registry fingerprint already proves shape compatibility; an unknown server type is what silently mis-decodes. |
| 5 | `ASSUMED` | The snapshot id is **exported, not managed**: the plane records it and the operator aligns their own `pg_dump`. Owning the dump would make this a backup tool. |
| 6 | `ASSUMED` | A `Consistency` member describes **what happened to the data**, so the vocabulary stays backend-agnostic even though only Postgres can produce `snapshot` today; which levels an export can offer is a per-adapter answer, as [RFC 0052](0052-versioned-facts-correction-lineage.md) decision 6 has it for guarantees. Asking for a level the backend cannot produce refuses, rather than silently downgrading to `fuzzy`. |
| 7 | `ASSUMED` | Four of the five requested pieces already ship (§3), so this RFC does not re-propose them; the table is kept in the document so a reader does not rebuild them by accident. |
| 8 | `OPEN` | Whether `snapshot` is a fourth level or a flag, how long the transaction is held for an operator's dump (and whether the window is declared), and whether `server_version` is recorded at every level. |

## 12. Phasing

- **P1** — `server_version`, the import major-version gate, batteries. Independent of everything
  else and useful alone.
- **P2** — the `snapshot` consistency level, the single-transaction read path, the multi-client
  refusal, the concurrent-writer regression test.
- **P3** — `snapshot_manifest` and the exported id, with the window's documentation.
- **P4** — the DST round-trip battery with the crash leg.
