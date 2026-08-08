# RFC 0012 — `forze_localfs` — local-filesystem storage backend (demand-gated)

- **Status:** 📝 Draft — **deliberately unscheduled** (demand-gated; triggers in §6)
- **Scope:** A production-honest storage backend over a local (or mounted-shared) POSIX filesystem, implementing `ObjectStorageClientPort` so the entire shared storage adapter — key validation, tenancy prefixes, client-side encryption, capabilities, streaming — is reused verbatim, exactly as `forze_s3` and `forze_gcs` do. Object semantics only: keys, not paths; no directory API, no hardlinks, no mount-the-workspace bridge.
- **Related:** The shared adapter (`forze/application/integrations/storage/adapter.py`) and its kernel seam (`integrations/storage/client.py` — `ObjectStorageClientPort`, ~25 methods across upload/download/range/conditional/copy/tags/multipart/presign families). The managed-cloud fidelity policy is what this backend must obey (a backend that only ever tests against itself is a tautology). The mock storage adapter stays what it is — the DST/determinism substrate; this backend does **not** replace it and must never be used where the mock is meant.
- **Origin:** The eis-dag platform evaluation (2026-07-30). That system's deeper need — POSIX workspaces as subprocess `cwd`, artifacts passed by path, hardlink dedup — is explicitly **not** what this RFC provides and cannot be provided through object semantics; the recorded recommendation there is to adapt the app to key-shaped storage. What survives as demand for this backend is narrower: a full-fidelity storage plane for deployments that cannot (or should not) run MinIO.

---

## 1. Why this is written down now, and why it is not scheduled

The framework's storage story currently offers two extremes: real object stores (S3/GCS — full fidelity, needs a service) and the in-memory mock (deterministic, vanishes on restart). There is no middle: a single-box deployment that wants durable blobs either runs MinIO or leaves the framework's storage plane.

Writing the design now costs little and pins the semantics before someone improvises them. Scheduling it costs a lot, permanently: a third real implementation joins the mock↔real conformance battery **forever** — every future storage-contract change is then proven across four implementations instead of three. The mock `overwrite_stream` create-gap and the storage real-vs-real divergences were exactly the class of bug that breeds in an under-exercised implementation, so a battery seat is not optional (the "reading isn't proof" rule applies in full). That standing cost is why this ships only against a named consumer.

## 2. Shape

**Package** `forze_localfs`, extra `localfs`, zero new dependencies (stdlib only; blocking file I/O dispatched via `asyncio.to_thread`). One class implementing `ObjectStorageClientPort`, plus the standard trio: `LocalFsStorageConfig` (root path per logical bucket), deps module, lifecycle step (root existence/writability probe at startup — fail the boot, not the first upload).

**Layout.** `<root>/<bucket>/objects/<key>` for content, `<root>/<bucket>/.meta/<key>.json` for metadata (content type, tags, ETag, timestamps). A shadow metadata tree, not xattrs (not portable across filesystems) and not sidecars-next-to-content (would pollute listing). Keys map to relative paths directly — the shared adapter's `_validate_key` already refuses `..` segments, leading `/`, and off-charset bytes before the kernel ever sees a key, and the kernel still resolves-and-contains under the bucket root as its own last line (defense in depth, same doctrine as the artifact routes this design was measured against).

**Semantics that need explicit decisions** (the reason an RFC beats an improvisation):

| Concern | Decision |
|---|---|
| Atomic write | Temp file in the same directory tree + `os.rename` (atomic on one filesystem). A crashed upload leaves a temp file, never a torn object; a startup sweep removes stale temps. |
| ETag | Content hash (streamed while writing), stored in the metadata sidecar. Makes `download_if_changed` / `if_match` honest rather than mtime-flaky. |
| Conditional ops (`if_match` on `overwrite_stream`, fenced replace) | Serialized via `fcntl.flock` on the metadata sidecar for the compare-and-rename window. Advisory locks are enough because every writer goes through this kernel; the limitation (a non-forze process writing the tree breaks the guarantee) is documented, not hidden. |
| `copy` / `move` | Real file copy (never hardlink — link semantics would leak mutable aliasing through a contract that promises independent objects); `move` stays copy+delete, non-atomic, exactly as the port documents. |
| Listing | Ordered walk of the objects tree, lexicographic by key, prefix-filtered — matching the port's flat-listing contract. No delimiter/common-prefix API (the contract has none). |
| `presign_download` / `presign_upload` | **Unsupported, fail-closed** via `StorageCapabilities`. A presigned URL requires something serving HTTP; a filesystem cannot honor the contract's bearer-URL semantics. This is the first storage backend with a capability hole, which is itself useful: it forces the capability surface for storage to be real rather than always-true. |
| Multipart upload sessions | Refused in v1 (capability-gated). Native multipart exists for network efficiency; a local write has none to gain. Emulation via part-files is recorded as a follow-up if a consumer needs the *API shape* for symmetry. |
| Tenancy | The shared adapter's tenant key-prefixing works unchanged; `namespace` isolation = per-tenant subtree. `dedicated` = per-tenant root from config. |

**Encryption:** client-side encryption composes for free — it lives in the shared adapter above the kernel, so an encrypting route writes ciphertext files with no localfs-specific code.

## 3. What this backend is not

- **Not the mock.** The mock is the simulation substrate: in-memory, deterministic, no I/O. This backend adds real disk latency, real `ENOSPC`, real partial-failure modes — the exact things DST excludes on purpose. Tests choose one deliberately.
- **Not a path bridge.** No API returns a filesystem path. A consumer that needs a real file on disk (a subprocess input) downloads to its own scratch — the object key never doubles as a path promise, or every future backend migration breaks.
- **Not a distributed store.** Two processes on two machines pointing at the same NFS mount are outside the guarantee (advisory locks over NFS are a known swamp). Single-box multi-process is in scope; anything wider is MinIO's job.

## 4. Conformance (the actual price)

- Joins the adapter conformance battery as a peer of S3/GCS — the full case set, not a subset.
- A differential leg vs MinIO (`forze_s3`) over the same case set, per the fidelity policy: divergences classified by direction, catalogued, none silent.
- Crash-honesty cases: kill between temp-write and rename (object absent, no torn read); kill between rename and sidecar write (recovery rule decided in P1 — content-without-meta is the crashed state; a `head` treats it as absent and the sweep reaps it).
- Capability honesty: presign and multipart refusals surface as the standard fail-closed errors, and the generated storage routes degrade correctly (no presign endpoints advertised).

## 5. Phases

- **P1** — kernel + config/deps/lifecycle + capability wiring + crash-honesty semantics.
- **P2** — conformance battery seat + MinIO differential leg + divergence catalog.
- **P3** — docs page (deployment guidance: when this vs MinIO; the NFS non-guarantee in bold).

## 6. Triggers (what un-parks this)

1. A named deployment that needs durable framework-plane blobs and demonstrably cannot run an object-store service.
2. An in-repo consumer (example, recipe, or kit) whose story is materially worse over MinIO-in-compose.
3. An explicit product decision that "single-box, zero extra services" is a supported deployment tier.

"Fewer containers in dev" is explicitly **not** a trigger — compose-with-MinIO is the supported answer for development, and the mock is the answer for tests.

## 7. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Implement `ObjectStorageClientPort` (kernel seam), reusing the shared adapter wholesale — never the query/command ports directly | locked |
| 2 | Object semantics only: no path egress, no hardlinks, no directory API | locked |
| 3 | Presign + multipart fail closed via capabilities; first deliberately-partial storage backend | locked |
| 4 | Metadata in a shadow tree, not xattrs, not inline sidecars | locked |
| 5 | Atomicity = same-fs temp+rename; conditionals = flock over the sidecar; non-forze writers void the warranty (documented) | proposed |
| 6 | Battery seat + MinIO differential are non-negotiable gates for shipping | locked |
| 7 | Demand-gated; the RFC exists to pin semantics, not to schedule work | locked |
