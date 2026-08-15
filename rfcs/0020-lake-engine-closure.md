# RFC 0020 — Lake engine closure & breadth: catalog-attached DuckDB, stream sinks, second formats

- **Status:** 📝 Draft (W1 gated on RFC 0018 P1; W2–W4 demand-gated separately — the 0016 shape)
- **Naming:** "lake" carries RFC 0018's meaning — an Iceberg lakehouse over a REST catalog. W1 is the clearest illustration: what DuckDB learns to `ATTACH` is a *catalog*, which is the thing a data lake does not have and the thing the lakehouse is built around.
- **Scope:** Close the loop between the lake write plane (RFC 0018) and the engines that read it, and record the breadth items with their triggers so they never get re-designed ad hoc. **(W1)** `forze_duckdb` learns Iceberg **REST catalog ATTACH** — reversing the "REST/Glue ATTACH explicitly out of scope" note in `kernel/sources.py`, which predates the extension supporting it — with namespace-per-tenant schema mapping for governed analytics routes over live lake tables. **(W2)** engine-side lake writes via DuckDB, recorded and fenced. **(W3)** a stream→lake sink consumer. **(W4)** Delta and Lance as second formats. W2–W4 each ship only against a named consumer.
- **Related:** RFC 0018 (the tables being read), RFC 0016 (DuckDB dynamic-read honesty — still binding, but **its DuckDB verdict changed on 2026-08-15**: trusted-only at a `tagged` ceiling was superseded by 0016 decision 6, which refuses DuckDB dynamic read over raw object storage outright and admits it only as a routed client over a credential-vending catalog. W1 here is what makes that client possible, so the dependency now runs both ways), the offset-log stream contracts and `forze_kafka` (the commit-stream model W3 consumes), the fidelity policy (engine matrices; the differential legs here reuse 0018's Lakekeeper+MinIO fixture).
- **Origin:** Verified 2026-07: the DuckDB iceberg extension attaches Iceberg REST catalogs (namespaces surface as schemas) with **full read support since v1.4.0 and write support since v1.4.0/1.4.2** (deletes/updates for v2 tables), OAuth2 via the existing DuckDB secrets workflow. The current `IcebergSource` metadata-path form — with its `version-hint.text` caveat — becomes the fallback, not the main road: catalog-attached tables always resolve the current snapshot, which kills the staleness-by-construction problem of pinning a `*.metadata.json` path at startup.

---

## 1. W1 — catalog-attached DuckDB reads (the main workstream)

- **Kernel:** `IcebergCatalog(name, uri, warehouse, credential: SecretRef | OAuth token config)` joins `S3Credentials`/`GcsCredentials` in the startup hook's vocabulary; renders to `ATTACH … (TYPE iceberg, …)` + a DuckDB secret. Requires a `duckdb>=1.4.2` floor (today's is `>=1.1.0`) — a real bump, priced: the changelog entry and the perf/regression suite run against it, and the iceberg extension version is pinned alongside (verify exact pins at pickup; capabilities checked 2026-07).
- **Sources doctrine flip:** attached-catalog tables are referenced as `catalog.namespace.table` in route SQL or wrapped as named views; `IcebergSource(metadata_path)` stays for catalog-less tables and its docstring inverts emphasis (fallback, staleness caveat now contrastable with the attach path).
- **Tenancy:** namespaces surface as schemas, so the per-tenant story is the Postgres one transposed — analytics routes get `query_namespace: NamedResourceSpec` resolving the tenant's namespace into qualified names / a per-call `SET schema`. The 0016 honesty is untouched and restated where it bites: DuckDB remains `tagged`-ceiling in-process compute, dynamic read over it remains trusted-only; W1 raises what governed *named-query* routes can reach, not what dynamic statements may do.
- **Freshness semantics documented, not implied:** an attached catalog resolves snapshots per query — dashboards read committed data with no restart; the docs page's "point at the current metadata.json" workaround section shrinks to the fallback paragraph.

## 2. W2 — engine-side lake writes via DuckDB (recorded, fenced) — **absorbed by RFC 0028 (2026-08-03)**

> **Status update.** This workstream is now RFC 0028 W3. The consumer materialized (a DWH-shaped backend), and the design recorded below turned out to be *exactly* right — which is why it needs no separate build: "a procedures-style registered statement over the attached catalog" is a DuckDB `ProcedurePort` adapter plus W1 above, and 0028 builds that adapter for its own reasons. The fence below is unchanged and is restated in 0028 decision 5. Nothing new is designed; the item simply moves to the RFC that ships the mechanism.

DuckDB can now `INSERT`/`UPDATE`/`DELETE` against attached catalogs. The fence: **RFC 0015/0016's dynamic-read plane stays read-only — engine-side lake writes never ride dynamic statements.** If a consumer materializes (the candidate shape: SQL-native Silver→Gold transforms that would otherwise round-trip Arrow through Python), the design is a *procedures-style registered statement* over the attached catalog — wiring-time SQL, tenant-namespace resolver, command-plane port — i.e. the existing procedures doctrine pointed at a lake catalog, not a new liberty. Recorded so nobody "just uses the client" the day the need appears; unbuilt until it does.

## 3. W3 — stream→lake sink (recorded, demand-gated)

The commit-stream/queue → Iceberg micro-batching pattern: a kits consumer buffering by count/age, flushing via `append_batches`, exactly-once by writing the **consumed offset range into the snapshot's summary properties** in the same commit (the Kafka-Connect-Iceberg pattern; on restart, the last committed offset is read back from the table itself — the 0018 `commit_ref` mechanism generalized to offsets). Trigger: an event/telemetry ingestion consumer whose volume outgrows the analytics `append` path. Interplay noted: a sink table is the canonical upsert-free, append-heavy case — its health lives on the 0019 schedule from day one.

## 4. W4 — second formats (recorded)

- **Delta** (`forze_delta` over delta-rs) behind the same `contracts/lake` — the contract was written format-neutral to make this a backend, not a redesign. Trigger: a consumer in a Databricks/Unity-shaped environment. Priced honestly: Delta's catalog story is Unity-centric; the REST-catalog uniformity that makes Iceberg cheap here doesn't transfer.
- **Lance** (vector/ML-native format) — recorded only, as the likely future crossover between the lake plane and the embeddings/inference planes; no design until a consumer exists.

## 5. Acceptance battery (per shipped workstream)

1. W1: attach against the 0018 Lakekeeper fixture; a table written via `forze_iceberg.append_batches` is readable through a DuckDB analytics route **without restart** (the freshness claim, pinned); per-tenant namespace resolution yields disjoint reads for two tenants; version-bump regression: existing duckdb suite green on `>=1.4.2`.
2. W1: metadata-path fallback still works; its staleness (pinned path misses a new snapshot until re-registered) is battery-pinned as documented behavior, contrasted with the attach path in the same test.
3. W3 (when built): crash between flush and offset observation → replay converges via snapshot-property offsets, no duplicates, no gaps — DST schedule + real-leg differential.
4. W4 (when built): the 0018 battery re-run wholesale against the Delta backend — the contract's format-neutrality is proven by test reuse, not asserted.

## 6. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Reverse the `sources.py` "REST/Glue ATTACH out of scope" note — it predates extension support; catalog attach becomes the main road, metadata-path the fallback | locked |
| 2 | DuckDB floor bump to `>=1.4.2` accepted and priced (regression run + pinned extension version) | proposed |
| 3 | Engine-side lake writes are procedures-shaped registered statements if ever built; the dynamic-read plane stays read-only permanently | locked |
| 4 | Stream sink exactly-once = offset range in snapshot summary properties (table as its own offset store), 0018 `commit_ref` generalized | locked |
| 5 | W2–W4 demand-gated with named triggers; W1 ships with 0018 adoption | locked |
