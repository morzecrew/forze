---
title: Portability
icon: lucide/package
summary: Carry an application's state to another backend — inventory, quiesce, export, import, migrate
---

Sooner or later something has to move: a tenant leaves and wants their data, a
Postgres deployment becomes a Mongo one, a self-hosted install becomes a managed
one. A database dump does not solve this — it is tied to one engine, it carries
operational noise (in-flight outbox rows, half-drained queues) as if it were
state, and it says nothing about the planes that live *outside* that database.
Forze moves state the same way it writes it: **through the ports**, plane by
plane, refusing to produce an artifact it cannot vouch for.

This is a **portability** plane, not a backup one. Durability stays your
backend's job — write-ahead logs, point-in-time recovery, snapshots.

## What an application consists of

The mechanism needs an answer to a question nothing could answer before: *what
does this application actually bind?* Specs are handed to ports at resolve time
and stored nowhere, so the dependency registry knows every `(key, route)` pair
and not a single spec. The **spec inventory** closes that gap — you register what
you own, and `build_runtime` reconciles it against the wiring at startup:

```python
from forze.application.contracts.inventory import SpecRegistry

specs = SpecRegistry().register(OrderSpec, OrderSearchSpec, InvoiceBlobSpec)

runtime = build_runtime(deps=deps, specs=specs, ...)
```

Kits contribute their own — an aggregate kit registers the outbox, queue and
inbox its search-sync mints, and the identity plane registers its account,
session and credential specs — so the routes hardest to remember are the ones
you never have to. Reconciliation is a **drift signal, not a gate**: a mismatch
between what you declared and what is wired is logged as a warning, and the
registry's fingerprint is what an import later checks the target against.

## Plane completeness

Every plane an inventoried spec belongs to declares what an export may do with
it. Four answers, and silence is not a fifth — a plane nobody classified is
`REFUSED`, because "there is nothing to carry" and "we did not think about it"
look identical from the outside and only one of them is safe:

| Disposition | Meaning | Planes |
|---|---|---|
| `EXPORTABLE` | System of record; its rows travel | document, storage (blobs), graph, counter |
| `REBUILDABLE` | Derived; recomputed on the target | search, cache, projected analytics |
| `DRAINED` | In-flight work; quiesce brings it to empty | outbox, inbox, queue, pub/sub, stream, idempotency, distributed locks |
| `REFUSED` | Cannot be carried faithfully *or* safely skipped | analytics that is a system of record, an undeclared plane |

Two planes are decided per spec rather than per plane. An analytics spec
declares its provenance — a projection of data you already own is rebuildable, a
warehouse table that is the only copy is refused, and nothing but its author can
tell the difference. A graph is exportable only when every one of its kinds can
be walked back to an identity.

A `REFUSED` plane stops the export cold, by design: you either declare it
carryable or accept that this artifact is not complete.

## Bring the planes to rest first

An export of a running system is a smear — rows written after the walk passed
them are simply absent. `quiesce` is the step that makes a point-consistent
artifact possible:

```python
from forze_kits.integrations.quiesce import quiesce

async with runtime.scope():
    report = await quiesce(runtime, timeout=timedelta(seconds=60))
```

It stops admission (new top-level invocations get a retryable `throttled` with
`code="draining"`), waits out in-flight work, stops the background loops the
runtime owns and asks the outbox relay to flush what is claimable, then polls
each outbox route, the durable-run plane and each named stream group until it is
empty or the budget expires. Outbox routes default to **every route in the
inventory**; consumer groups stay explicit, because a group name is the identity
of whoever is reading and no inventory can supply it.

The report answers two different questions, and the difference is the whole
point:

- `settled` — an *observation*: nothing was moving when the sweep finished.
- `attested` — a *promise*: nothing was moving **and nothing could arrive**,
  because the runtime was holding the door shut.

Only the second is safe to build an export on. Pass `close_gate=False` and you
get a health check instead: the sweep reads every plane and the scope keeps
serving, so the report can settle but can never attest.

!!! warning "Closing the gate is one-way"

    An attested runtime does not start accepting work again — this is the
    shutdown gate, and quiesce is the step *before* a shutdown, an export, or a
    migration. It also holds only **this process** still: a sibling replica that
    is still serving writes will happily invalidate whatever this one attested.
    Stop the fleet first.

Planes that exist but cannot be read are reported `unobserved` and **block
attestation** — an unreadable plane is not an empty one. Planes the runtime does
not wire at all are `not_wired` and cost nothing.

## Export and import

An export walks the inventory and streams each exportable plane into a
backend-agnostic archive — JSONL data files plus raw blobs under a manifest.
Both calls run inside a scope you already own:

```python
from forze_kits.integrations.portability import (
    TenantScope,
    export_archive,
    import_archive,
)

async with source.scope():
    await export_archive(source, Path("/tmp/acme"), scope=TenantScope(tenant_id=acme))

async with target.scope():
    await import_archive(target, Path("/tmp/acme"), tenant=acme)
```

Rows are the **decrypted read models**, so the artifact never depends on the
source's keys and the target re-seals every sealed field under its own. Ids and
business timestamps are preserved; `rev` resets to 1, because revisions are the
concurrency history of the store the rows just left.

**Scope is declared, never inferred.** `TenantScope` carries one tenant's
partition. `FullScope` carries the whole system and takes two things: the
`QuiesceReport` that attests it, and the tenant dimension it spans — the archive
gets one section per declared tenant, or `UNTENANTED` for a single-partition
app. There is no default, because guessing wrong writes an artifact that is
silently missing a tenant.

Import fails closed on everything it can check:

- The manifest is cross-checked against the artifact — an unlisted data file, or
  an expected plane the manifest never listed, refuses the import. A missing
  plane never imports as empty.
- The archive's format version and the source registry's fingerprint must match
  the target's plan.
- A per-tenant archive requires an explicit `tenant=`, and a full-system archive
  anchors its section list with `expect_tenants=`. The manifest names a tenant,
  but it is plaintext and unauthenticated — it is cross-checked, never trusted,
  so a tenant deleted from the manifest cannot vanish with every checksum
  passing.
- `on_conflict` is `"skip"` by default; `"fail"` makes an id collision an error.

Rebuildable planes are not in the artifact — recompute them on the target once
the documents land, with
[`rebuild_search_index`](../reference/contracts/search.md#rebuilding-an-index) for
a search index.

## Migrating without an artifact

When the destination is another live deployment rather than a file, `migrate`
fuses the two pipelines port-to-port. Nothing plaintext is ever written to disk,
which makes it the recommended path for a backend change:

```python
from forze_kits.integrations.portability import migrate

async with source.scope(), target.scope():
    await migrate(source, target, scope=FullScope(quiesce=report, tenants=(acme, globex)))
```

The fingerprint gate the file import applies to an archive is applied here to
the live target before a single row moves: two runtimes whose spec shapes
disagree are refused.

## Sealing the artifact

A file archive is **plaintext by construction** — that is what makes it portable
across key domains, and it is also what makes it credential-adjacent. Treat the
directory accordingly, and for anything beyond a short-lived local copy, seal
it:

```python
await export_archive(runtime, dest, scope=scope, sealer=sealer)
```

A per-archive data key encrypts every data file and blob, wrapped under a KEK
whose plaintext never leaves the [KMS](../integrations/kms.md); import needs the
same sealer and fails closed without one when the manifest says the archive is
sealed. An **unsealed** export whose payload is credential-adjacent — identity
specs included, or specs declaring field encryption — is refused outright unless
`acknowledge_plaintext=True` states that a plaintext artifact is the intent.

Per-tenant exports omit identity and credential specs by default (`sessions`,
API keys, invite and reset tokens): a data-portability request wants the
tenant's business data, not their live session tokens. Pass
`include_identity=True` when moving a deployment rather than answering a
request. A full-system export always carries them — moving a live system means
moving its sessions.

Archives are gzip-compressed by default; `compression="zstd"` needs the
`forze[zstd]` extra and fails closed without it.

## Same content, different bytes

Two exports of the same data are rarely the same file: row order follows the
backend's cursors, compression and sealing differ per run, and byte identity
holds only inside a pinned build environment while proving nothing about
meaning. The manifest therefore records two identities per data file — `sha256`
over the bytes (what import verifies) and `content_digest`, an order-independent
digest of the canonical rows (what *reproducibility* means). `compare_content`
renders the verdict for two manifests:

```python
from forze_kits.integrations.portability import compare_content

verdict = compare_content(manifest_a, manifest_b)
assert verdict.same_content, verdict.differing or verdict.unknown
```

The verdict refuses to call archives equal on files it cannot compare: sealed
archives carry no content digest (a plaintext-derived digest beside ciphertext
would let anyone confirm a guessed row set), so their files land in
`verdict.unknown` rather than counting as matches.

When two builds *aren't* the same content, `run_manifest` says what each was
built with — it binds a run's report (per-plane counts and all) to the code
identity and lockfile digest that made it, as one JSON document to persist as a
row, a file, or both:

```python
from forze_kits.integrations.portability import run_manifest

manifest = run_manifest(
    report, run_id=run_id, started_at=started,
    git_sha=settings.runtime.git_sha, lockfile=Path("uv.lock"),
)
path.write_text(manifest.model_dump_json(indent=2))
```

## Notes

- Both halves refuse to run without a spec inventory. An export that cannot
  enumerate the application cannot claim to be complete.
- A full-system export without an attesting quiesce is refused unless
  `allow_fuzzy=True`, which stamps the manifest `consistency: fuzzy` — importable,
  but not point-consistent.
- Temporal-backed workflows are outside what quiesce can speak for; their state
  lives in the Temporal cluster.
- `ArchiveExporter`, `ArchiveImporter` and `ArchiveMigrator` are the configurable
  cores behind the three functions — reach for them when you want to reuse a
  configuration or already hold a context.

Portability leans on the same drain machinery a rollout does — the gate, the
in-flight window, the loops that stop between units of work — which is
[Shutdown & fleets](shutdown-and-fleets.md) seen from the other side.
