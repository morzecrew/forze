# RFC 0078 — Routed DuckDB lake reads: per-tenant client, vended credentials, governed dynamic read

- **Status:** 📝 Draft. This RFC is the owner RFC 0016 §2 says W2's prerequisite 2 lacks ("a per-tenant routed DuckDB client … **No RFC owns this**"). It also runs prerequisite 4 (the ambient-credential probe) as far as an in-process engine allows. Prerequisite 1 (vended credentials, RFC 0018 P3) is consumed, not re-specified. Prerequisite 3 (the process filesystem control) stays a deployment property, with its own battery case.
- **Scope:** Three parts, which together let a dynamic-read route with `origin="compiled"` run on DuckDB at a tier that clears the RFC 0037 floor. **(1)** `RoutedDuckDbClient`: one in-memory DuckDB instance per tenant, in a bounded LRU with a guarded drain. **(2)** A Lakekeeper REST-catalog `ATTACH` per tenant warehouse, with table-scoped vended credentials, a fixed session lock-down sequence, and an attestation that the session really is locked before it serves anything. **(3)** `DuckDbDynamicReadAdapter` behind the shipped `DynamicReadPort`, with an honest capability row. Read-only, permanently. There is no new port, no new plane, and no new tenancy tier.
- **Related:** RFC 0015 (the shipped contract, shell, taxonomy and mock, reused verbatim). RFC 0016 §1 addendum (b)/(c), §2 prerequisites and decisions 6–9 (the binding verdict this RFC executes). RFC 0018 §3 (the `dedicated` tier = a warehouse per tenant through a routed catalog client; vended credentials preferred) and §7 (the Lakekeeper fixture). RFC 0020 W1 (`IcebergCatalog`/`ATTACH` vocabulary; its 2026-08-15 addendum says W1 does not deliver this). RFC 0028 §4 (the single-connection concurrency rule) and W3 (DuckDB *writes*, which are not this RFC). RFC 0037 (the origin floor, shipped as `_ORIGIN_FLOORS` in `src/forze/application/contracts/tenancy/wiring.py:122-126`). RFC 0038 (compiled read; this RFC answers its §10 q3 for this backend). RFC 0019 does not interact: maintenance writes never ride this plane.
- **Origin:** A BI system (a spec compiler plus a semantic layer) whose planner emits per-request SQL over per-tenant gold marts. It serves those reads through `ctx.dynamic_read` on Postgres today and wants the lake path built in parallel and swapped in by wiring. As wired today, the swap is refused at freeze by design: `forze_duckdb`'s deps module hard-codes `client_is_routed=False` and `max_supported_isolation="tagged"` (`src/forze_duckdb/execution/deps/module.py:52-66`), and `compiled` needs `namespace`.

**Fact tags.** Every ecosystem claim carries one. ✅ = verified, with a source: a repo path:line, a URL, or **[probe]**, meaning observed on 2026-10-27 against the `duckdb==1.5.6` that `uv.lock:1069-1070` resolves (floor `duckdb>=1.1.0`, `pyproject.toml:145`), in-process, with a local HTTP listener standing in for S3. 🔎 = verify at pickup (P0). DuckDB 2.0.0 shipped on 2026-10-21 and is **not** the pinned engine ✅ (github.com/duckdb/duckdb releases), so every probe re-runs on a bump.

---

## 1. The tier is `dedicated`, and `namespace` is not available on DuckDB

The floor is chosen by *"what happens when the check is wrong"* (`wiring.py:127-141`). A container clears it when a defective compiled statement either names a relation that does not resolve or stays inside the tenant's own data. Two candidate containers:

- **`namespace` on a shared instance** (one DuckDB instance, one catalog principal, a namespace per tenant, a per-call `USE`/`SET schema`). This is RFC 0020 W1's shape, and its addendum already gives the verdict: it *"confines text the framework generated, and confines nothing a caller wrote"*. In Postgres the namespace tier at least has a second identity, the per-tenant `SET LOCAL ROLE`, for mistake-proofing. DuckDB has no in-engine identity of any kind. Every setting that matters is `GLOBAL` scope, which means per instance and shared by all its connections ✅ [probe: `duckdb_settings()`; one instance's lock did not affect the next], and a cursor is just another connection to the same instance (`src/forze_duckdb/kernel/client/client.py:303`). So the principal that loads tenant A's tables loads tenant B's too, and the catalog vends credentials for both. A naming defect, such as a wrong prefix or a plan cache keyed without the tenant, resolves and returns another tenant's rows. That is exactly the outcome the floor exists to exclude.
- **`dedicated` through a routed client** (one instance per tenant, holding one tenant-scoped catalog principal and only that tenant's vended credentials). The `DynamicReadProvenance` docstring (`src/forze/application/contracts/dynamic_read/value_objects.py:23`) names *"the only scoping key a statement cannot forge from inside the session"*: the session's own credential. For DuckDB that credential is the instance's catalog principal, so a per-tenant instance **is** the dedicated tier. It is not an approximation of it. The wiring already derives it that way (`_route_isolation_mode` returns `"dedicated"` whenever `client_is_routed`, `wiring.py:342-343`), so no tier logic changes.

So the answer is `dedicated`. It costs one unnamed `:memory:` instance per active tenant, and two such instances are distinct databases ✅ [probe: a table created in one is invisible to the other]. RFC 0016 decision 6 already locked this verdict, and this section records the reason. What `dedicated` does **not** mean here: CPU, memory, local disk and network egress are still shared by the process. The tier describes what data a statement can **reach**, which credentials decide. §3.4 closes disk and HTTP egress inside DuckDB, and the deployment closes them outside it (RFC 0016 decision 9).

## 2. Part 1 — the routed per-tenant client

### 2.1 Shape: reuse, don't fork

`RoutedDuckDbClient(StructuredSecretRoutedTenantClientBase[DuckDbClient], DuckDbClientPort)` in `forze_duckdb/kernel/client/routed_client.py`. The routed integrations build on `src/forze/application/contracts/tenancy/routed_client_base.py` (e.g. `RoutedPostgresClient(DsnRoutedTenantClientBase[…])`, `src/forze_postgres/kernel/client/routed_client.py:47`). That base supplies the LRU pool (`max_cached_tenants`, `:80`), the guarded drain (`:52-54`), credential-fingerprint refresh (`fingerprint_ttl`, `:89`), `evict_tenant` (`:126`), `client_scope` (`:201`) and `pool_stats`. The inner client is the shipped `DuckDbClient`, unchanged except for one method (§4.2). `initialize_client(tenant_id, creds)` runs §3.4 on a fresh instance.

```python
@attrs.define(slots=True, frozen=True, kw_only=True)
class LakeTenantCatalog:                 # resolved per tenant; JSON-trivial
    uri: str                              # Lakekeeper REST base, e.g. http://lakekeeper:8181/catalog
    warehouse: str                        # the tenant's warehouse name
    oauth2_server_uri: str
    oauth2_scope: str | None = None       # client_id / client_secret come from the SecretRef payload

@attrs.define(slots=True, kw_only=True)
class RoutedDuckDbClient(...):
    catalog_for_tenant: Callable[[UUID], LakeTenantCatalog]
    engine: DuckDbConfig = DuckDbConfig(threads=2, memory_limit="1GB", max_concurrent_queries=2)
    max_concurrent_queries_total: int = 8   # process-wide, across every tenant instance
    http_timeout: timedelta = timedelta(seconds=5)
    guarded: bool = True                    # refused if set False (§2.4)
```

It has no `object_stores`, `secrets`, `sources` or `bootstrap_sql` field, unlike `DuckDbStartupHook` (`src/forze_duckdb/execution/lifecycle/pool.py:101-124`). Each of those is a path for static credentials or raw statements. RFC 0016 §2 prerequisite 1 calls static credentials *"the unwireable configuration wearing the wiring of a safe one"*, so leaving the fields out is the refusal. A construction that smuggles them in through `engine` fails with `duckdb_routed_client_static_credentials` (configuration).

### 2.2 Concurrency

forze's DuckDB rule (RFC 0028 §4): one statement in flight per connection, with the pool outside. `DuckDbClient` already keeps it: one `duckdb.connect` per database, a fresh `cursor()` per statement, and a bounded executor (`client.py:198-201`, `:303`). Per tenant that does not change. What does change is the sum across tenants. N cached tenants × `max_concurrent_queries` executors × `threads` engine workers oversubscribes the host (the probe host defaults to `threads=16`). So the routed client holds a process-wide `asyncio.Semaphore(max_concurrent_queries_total)` around every `client_scope` use, which is the Postgres routed client's `query_concurrency_limit`/`gather_concurrency_semaphore` pattern (`src/forze_postgres/kernel/client/routed_client.py:160-189`). Memory follows the semaphore rather than the cache: peak ≈ `max_concurrent_queries_total × memory_limit`, because an idle in-memory instance holds no buffers. The idle footprint with `httpfs` and `iceberg` loaded is 🔎, and `max_cached_tenants` is sized from it.

### 2.3 The cache-partition key is the tenant id, by construction

A routed client must declare a partition key (`wiring.py:591-598`). Here the key is the instance. Secrets, the attached catalog and its metadata, and the external file cache (`enable_external_file_cache`, on by default) all live in the per-tenant instance ✅ [probe: settings are per instance]. Whether the external file cache is per `DatabaseInstance` and not per process is 🔎. Until it is confirmed, P1 sets it `false`. The one cache the docs call *global*, `enable_http_metadata_cache`, defaults to `false` ✅ [probe] and is locked off. Three rules follow, and the module passes `partition_key_set=True` with a detail string that cites them:

- Only **unnamed** `:memory:` instances. A file path or a named in-memory database can be shared through the process-wide instance cache, and RFC 0016 addendum (b) shows that a per-tenant file is not a container anyway.
- Nothing persists. `allow_persistent_secrets=false` (§3.4).
- Caches above the port belong to the application, and their key must include the tenant (RFC 0038 decision 8). Caching *compiled plans* by fingerprint is safe across tenants only because plans are tenant-neutral (§5.1).

### 2.4 Lifecycle and eviction

- **Create** on first `client_scope(tenant_id)`: resolve the secret (client credentials) and `catalog_for_tenant`, run §3.4, attest, then serve. A failed attestation drops the instance and raises. It never serves a half-built session.
- **LRU eviction** past `max_cached_tenants` must be **guarded** (`guarded=False` is refused). An evicted instance moves to the draining set and closes after its last in-flight statement. Close is the existing `DuckDbClient.close` (executor shutdown, then `conn.close`), and the token and vended secrets go down with the instance.
- **Forced eviction** (`evict_tenant`) happens on credential rotation, tenant offboarding, or a grant revocation the app learns about. Without a rotation signal, set `fingerprint_ttl`. Revocation does not depend on eviction (§3.2): the next statement re-loads the table with the principal's current grants.

### 2.5 Wiring

`DuckDbDepsModule` gains `dynamic_reads: StrKeyMapping[DuckDbDynamicReadConfig] | None`, bound to `DynamicReadDepKey` (`"dynamic_read_query"`). Validation becomes:

```python
routed = isinstance(self.client, RoutedDuckDbClient)
validate_module_tenancy(
    integration="DuckDB", client_is_routed=routed,
    groups=[analytics_group,
            TenancyRouteGroup(kind="dynamic_read", configs=self.dynamic_reads,
                              tenant_aware=lambda c: c.tenant_aware,
                              origin=lambda _c: "compiled")],   # the plane's definition, unconditional
    required_isolation=self.required_tenant_isolation,
    max_supported_isolation="dedicated" if routed else "tagged",
    partition_key_set=routed, partition_key_detail="one in-memory instance per tenant (RFC 0078 §2.3)",
    validation_failed_code="duckdb_analytics_tenancy_validation_failed")
```

`origin` is hard-coded, as Postgres does it (`src/forze_postgres/execution/deps/module.py:566-573`), because the plane *is* runtime text. A dynamic route on the shipped non-routed client therefore fails at freeze with `statement_origin_isolation_floor`, which is RFC 0016 §4's replacement case "a route without a routed catalog client is refused". Named-query analytics routes keep working on the routed client and derive `dedicated`.

## 3. Part 2 — Lakekeeper vending and session lock-down

### 3.1 Catalog topology (deployment contract, asserted by the fixture)

- **One warehouse per tenant** (RFC 0018 §3, `dedicated` row), each with its own storage profile. Lakekeeper downscopes vended credentials to the table's location and guarantees no two table locations in a warehouse overlap ✅ (docs.lakekeeper.io/docs/nightly/storage/).
- **The profile:** `sts-enabled: true` and **`remote-signing-enabled: false`**. The latter defaults to `true` ✅ (…/storage-s3/), and three facts make disabling it mandatory. DuckDB does not implement remote signing ✅ (same page; duckdb-iceberg#670 open). Remote signing is unsafe on MinIO-class stores, where *"a client can change a request after it was signed"* ✅ (storage page). And a client offered a method it lacks *"needs its own storage credentials"* ✅, which is the ambient-fallback door.
- **One OAuth2 client-credentials principal per tenant**, granted `select` and nothing else, on its warehouse only. The authorizer must be OpenFGA or Cedar. `AllowAll` *"records grants faithfully but enforces nothing"* ✅ (…/authorization/), so a fixture running it proves nothing.
- **Use `CLIENT_ID`/`CLIENT_SECRET`, never `TOKEN`.** `TOKEN` *"disables refreshing"* ✅ (duckdb.org/docs/2.0/core_extensions/iceberg/iceberg_options; the 1.5 page carries the same option table). Lakekeeper is at v0.14.2 (2026-10-25) ✅ (GitHub releases). Pin the fixture tag at pickup.

### 3.2 Credential flow and refresh

`ACCESS_DELEGATION_MODE` defaults to `vended_credentials` ✅ (duckdb.org/docs/1.5/core_extensions/iceberg/iceberg_options). The session sets it explicitly anyway, so a changed default cannot silently change the posture. On `LoadTable` the catalog returns `storage-credentials` scoped to the table prefix, and duckdb-iceberg turns them into a temporary S3 secret with that scope. According to the extension maintainer, those secrets are **scoped to the transaction and discarded after it**, so the next scan re-loads (duckdb-iceberg#1352 thread). That is 🔎, and P0 checks it on 1.5.6. forze opens no explicit transaction (one autocommit cursor per statement, `client.py:303-317`), so in the design every statement starts from a fresh `LoadTable`. Consequences:

- **Revocation** takes effect at the next statement, without eviction. Battery 11a.
- **Storage-credential expiry inside a statement:** the maintainer says re-`LoadTable` *"should be available … in v1.5.6"*, and #1352 itself is still open (🔎). The design does not rely on it. It requires Lakekeeper's `sts-token-validity-seconds` (default 3600 ✅, storage-s3) to be ≥ 10 × (route `statement_timeout` + `http_timeout`). Then a statement cannot outlive its credentials. The session never sets `MAX_TABLE_STALENESS`, because cached table state is where an expired credential gets reused (🔎 semantics).
- **Catalog token expiry:** the extension refreshes client-credentials tokens on its own (proactive and on a 401). The fix for the empty-`CLIENT_ID` case is duckdb-iceberg#1378 (closed). The build that contains it is 🔎.
- **Scope broadening:** nested-prefix credentials used to be widened to the table root (duckdb-iceberg#1390, fixed by #1536). Whether the pinned build has the fix is 🔎. Battery item 17 pins it either way.

### 3.3 No ambient fallback: what was observed

On 1.5.6 with `httpfs`, an S3 read whose path matches **no** secret is signed with the process's `AWS_ACCESS_KEY_ID`. That includes reads while a tenant-scoped secret exists for a different prefix ✅ [probe: the listener captured `Credential=AMBIENTKEY/…` in both cases]. This is the concrete form of RFC 0016's *"silently fall back to ambient ones"*, and it is wider than vended credentials alone. Running `SET enable_global_s3_configuration=false` **after** `LOAD httpfs` stops it: the request goes out unsigned ✅ [probe]. The option cannot be passed at connect, because it belongs to the extension (the connect call fails with *"options were not recognized"* ✅ [probe]). `current_setting('s3_access_key_id')` still shows the env value afterwards ✅ [probe], so the sequence resets those settings too. Persistent secrets load from `secret_directory` (default `~/.duckdb/stored_secrets` ✅ [probe]), so they are disabled at connect. The `credential_chain` provider only exists once the `aws` extension is loaded ✅ [probe], so `aws` is never loaded and autoload is off. The last link is outside DuckDB: **the store must refuse unsigned requests** (no anonymous bucket policy), and battery item 1 checks that.

### 3.4 Hardening sequence (exact; per tenant instance; nothing is served before step 8)

```text
connect(":memory:", config={allow_persistent_secrets: false, autoinstall_known_extensions: false,
        autoload_known_extensions: false, allow_community_extensions: false,
        extension_directory: <read-only dir baked into the image>, temp_directory: "",
        threads, memory_limit})
1  LOAD httpfs; LOAD iceberg                       -- pre-installed; INSTALL never runs at runtime
2  SET enable_global_s3_configuration=false;
   SET s3_access_key_id=''; SET s3_secret_access_key=''; SET s3_session_token=''
3  CREATE SECRET lake_cat (TYPE iceberg, CLIENT_ID …, CLIENT_SECRET …, OAUTH2_SERVER_URI …, OAUTH2_SCOPE …)
4  ATTACH '<warehouse>' AS lake (TYPE iceberg, URI …, SECRET lake_cat,
          ACCESS_DELEGATION_MODE 'vended_credentials')          -- + READ_ONLY if the extension honours it 🔎
5  SET disabled_filesystems='LocalFileSystem,HTTPFileSystem'
6  SET http_timeout=<≤ route timeout ceiling>; SET enable_http_metadata_cache=false; SET enable_external_file_cache=false
7  SET lock_configuration=true
8  attest: duckdb_settings() has every value above; duckdb_secrets() types ⊆ {iceberg};
          duckdb_databases() = {memory, system, temp, lake}  — else duckdb_session_attestation_failed
```

| Step | Closes | Status |
|---|---|---|
| `LocalFileSystem` disabled | `read_csv`/`read_text` of local paths (incl. `/proc/self/environ`), `COPY … TO '/local'` (no file written), `ATTACH` of a local file, `INSTALL`, spill to disk | ✅ [probe], each a `PermissionException` |
| `HTTPFileSystem` disabled | `http(s)://` reads, i.e. fetch-by-URL and exfiltration through a URL; `s3://` keeps working | ✅ [probe]. That the REST catalog's own HTTP traffic survives it: 🔎 |
| `temp_directory=''` | spilling. `memory_limit` becomes a hard OOM instead of a confusing filesystem `PermissionException` | ✅ [probe] |
| `lock_configuration` | every later `SET`, including unlocking and re-enabling filesystems, on the parent connection and on cursors | ✅ [probe]. `USE` still works (harmless: only `lake` exists) |
| autoload/autoinstall/community off | extension loading as a side effect of SQL; the community flag locks itself | ✅ docs (duckdb.org/docs/1.5/operations_manual/securing_duckdb) |
| `allow_unredacted_secrets` (default false) | `duckdb_secrets(redact=false)` | ✅ [probe] refused |
| — | `getenv()` does not exist in the Python API; `query('…')` accepts a single SELECT only | ✅ [probe] |

**The stronger profile, adopted in P2 if P0 allows.** `enable_external_access=false` together with `allowed_directories=['s3://<bucket>/<tenant-prefix>/']` refuses any `s3://` path outside the prefix **inside DuckDB, before any network I/O**, and lets paths inside it through ✅ [probe]. That moves the cross-prefix refusal from the store into the engine as a belt. It is not the P1 default, because whether the iceberg `ATTACH` and the per-statement `LoadTable` traffic survive `enable_external_access=false` is 🔎.

### 3.5 Promise ledger: whose promise is each property

| Property | Primary enforcement | DuckDB's part |
|---|---|---|
| No cross-tenant read through the catalog | Lakekeeper authz (per-tenant principal, `select` on own warehouse) | none |
| No cross-tenant read by object path | object store: vended credentials downscoped to the table, no anonymous access | §3.3 settings stop ambient signing; the P2 allowlist is a belt |
| Read-only lake | catalog grant `select`-only; whether vended credentials for a select-only principal are read-only is 🔎 | statement classification (§4.2), a belt |
| No local-disk read/write | process filesystem control (RFC 0016 prereq 3, deployment) | `LocalFileSystem` disabled (promise) |
| No arbitrary HTTP egress | network egress policy (deployment) | `HTTPFileSystem` disabled (promise) |
| No ambient credentials | the process holds none (deployment) | §3.3 settings (promise) |
| Session cannot be reconfigured | — | `lock_configuration` (promise) + attestation |

Every "promise" row is something a DuckDB bug can break. That is why each one has a deployment row beside it and a battery case that removes the DuckDB setting and asserts the outside control still refuses (battery item 7).

## 4. Part 3 — the dynamic-read adapter

### 4.1 Config

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbDynamicReadConfig(TenantAwareIntegrationConfig):
    provenance: DynamicReadProvenance                  # mandatory; "untrusted" refused at wiring (§4.4)
    statement_timeout: timedelta = timedelta(seconds=5)  # Postgres's default; clamp-down only
```

There is no `query_schema` and no `role`. DuckDB has no analogue of either, and the container is the instance. `DuckDbDynamicReadAdapter(DynamicReadAdapter)` sets `tenant_placeholder_pattern=r"\$tenant\b"` (the analytics spelling, `src/forze_duckdb/execution/deps/configs.py`). Merging the tenant into the parameters is advisory, as on Postgres.

### 4.2 Execution (`_fetch_rows`, plus one client method)

`DuckDbClientPort.run_dynamic(statement, params, *, row_probe, timeout) -> list[JsonDict]`, run through the existing `__submit`/interrupt path (`client.py:249-282`). On a fresh cursor:

1. `USE lake` is issued by the framework, the way Postgres issues `SET LOCAL search_path`. `USE` is per-connection and cursors do not inherit it ✅ [probe].
2. `stmts = cursor.extract_statements(statement)`. If `len(stmts) != 1` → `multi_statement`. If the type is not `StatementType.SELECT` → `write_refused` with `details.statement_type`. This is **the engine's own parser**, the BigQuery dry-run precedent (RFC 0016 §1), not a forze parser, so RFC 0015 decision 3 is not engaged. Classes observed ✅ [probe]: `COPY`, `ATTACH`, `DETACH`, `SET` (incl. `USE`), `CREATE` (incl. `CREATE SECRET`/`MACRO`), `LOAD` (incl. `INSTALL`), `CALL`, `EXPLAIN`, `EXPORT` are all refused. `PRAGMA version`, `DESCRIBE`, `SUMMARIZE`, `VALUES` and FROM-first all classify as `SELECT`. `PIVOT` expands to `CREATE`+`SELECT`, so it is refused as multi-statement. That is a documented false refusal, and the compiler must not emit `PIVOT`.
3. Execute the **extracted `Statement` object** rather than the string, so the text that ran is the text that was classified ✅ [probe: `cursor.execute(Statement)` works]. Then `fetchmany(row_probe)` and close the cursor. The statement is **not** wrapped in `apply_limit_offset` (`client.py:298`). The shell never rewrites a statement (`src/forze/application/integrations/dynamic_read/adapter.py:62`).

Classification is a belt and not the read-only boundary. A `SELECT`-typed call can still reach a side-effecting table function (for example an iceberg maintenance function), which is why read-only belongs to the catalog grant (§3.5).

### 4.3 Capability row (to be appended to RFC 0016 §1's matrix when this lands)

| Read-only | Single statement | Timeout | Row cap | Container | Untrusted tier? |
|---|---|---|---|---|---|
| catalog `select`-only grant + store (external); engine-parser classification (belt) | `extract_statements` count, engine parser, client-invoked | **client-side `cursor.interrupt()`, cooperative.** No server-side timeout exists. A blocking HTTP read is bounded by `http_timeout` (whether interrupt cuts it short is 🔎), and the executor thread is held until DuckDB unwinds (`client.py:276`). Worst case ≈ timeout + `http_timeout` | client cap+1 via `fetchmany`. Whether that streams or materializes first is 🔎; either way the cap bounds the *result*, and `memory_limit` + timeout bound the *work* | per-tenant instance + per-tenant principal + vended credentials → `dedicated` | ❌ (§4.4) |

### 4.4 Provenance: trusted only

`provenance="untrusted"` fails at freeze with `duckdb_dynamic_read_untrusted_refused`. Postgres admits untrusted statements on a routed client because its *server* enforces the timeout and the read-only transaction. Here neither is server-side. On top of that, all tenant instances share one process's CPU (DuckDB has no per-statement CPU quota) and its executor threads (held past an interrupt). An unreviewed author can therefore starve every other tenant, which `dedicated` does not protect against here. The one consumer is a trusted compiler (RFC 0015 Tier A), so nothing is lost. The decision reopens on a named untrusted consumer **and** an out-of-process engine (RFC 0021 territory).

### 4.5 Taxonomy mapping (existing codes; one message generalized)

| Source | Code |
|---|---|
| classification: count ≠ 1 | `dynamic_read_multi_statement` |
| classification: type ≠ SELECT | `dynamic_read_write_refused`. The shared message says "READ ONLY transaction" (`src/forze/application/integrations/dynamic_read/errors.py:70`); make it engine-neutral in the same change |
| `ParserException`, `BinderException`, `CatalogException` (relation does not exist), `ConversionException` | `dynamic_read_statement_invalid` |
| `PermissionException` (disabled filesystem / allowlist); `HTTPException` 401/403 from the store or the catalog | `dynamic_read_permission_denied`: confinement doing its job, greppable |
| `InterruptException`; the client's timeout `exc.infrastructure` (`client.py:280`) | `dynamic_read_timeout` |
| `OutOfMemoryException`, `IOException` (connection) | deferred to infrastructure, as Postgres defers non-statement failures |
| result > cap | `dynamic_read_row_cap_exceeded` (shell, never truncates) |

How a Lakekeeper 403 or 404 on `LoadTable` surfaces (`CatalogException`, `HTTPException` or `IOException`) is 🔎, and battery item 16 pins it. The invariant is "never rows". The exact code is a mapping detail.

### 4.6 Mock parity and DST

`forze_mock`'s dynamic-read adapter already serves as the oracle for the shell and says it cannot make engine refusals (`src/forze_mock/adapters/dynamic_read.py:1-18`). Nothing changes there. The engine-specific refusals (classification, store 403, locked settings) are real-leg facts. A mock handler scripts them by raising the same factories. Parity means running the RFC 0015 shell battery against the mock and the DuckDB adapter (RFC 0038 §8 item 7 shape). DST cannot run a real DuckDB instance (that is I/O). What DST does cover is the routed pool, through a fake `initialize_client`: eviction during an in-flight statement, the guarded drain, and a rotation-evict race under forced schedules. Statement text stays masked (`capture_statements=False`). The interrupt path runs on wall-clock time and is tested only on the real leg, which is stated here rather than hidden.

## 5. Relation addressing, `RowPolicy`, and coexistence with Postgres

### 5.1 Plans are tenant-neutral: `DefaultNaming`, never `PrefixNaming`, on this read path

bloomery's `NamingPolicy.relation(entity, layer) -> (namespace, relation)` has two policies. `DefaultNaming` gives `("gold", "orders")`. `PrefixNaming` writes the tenant into the namespace (`f"{prefix}_{namespace}"`, `bloomery/src/bloomery/naming.py:66-77`). On this backend the planner compiles with `DefaultNaming`. The plan says `gold.orders`, and `USE lake` resolves that inside the tenant's attached warehouse. Three reasons:

1. **Text that carries no tenant cannot name another tenant.** Under `PrefixNaming`, a prefix defect produces a valid name of someone else's relation. Here it fails at the catalog. Under `DefaultNaming` there is nothing in the text to get wrong.
2. **RFC 0038's surface stays static.** `CompiledSurfaceSpec.relations` is a frozenset of `RelationRef(namespace, name)`. Tenant-neutral refs give one surface for all tenants, whereas `PrefixNaming` would need a surface per tenant.
3. **Plan caching across tenants becomes safe by construction.** The text is identical for every tenant, so a fingerprint-keyed *plan* cache cannot cross tenants. A *result* cache still must key on the tenant (RFC 0038 decision 8).

The catalog alias `lake` is a wiring constant, and the compiler never emits it. Three-part names also resolve ✅ [probe], but they would leak a wiring choice into plans. `PrefixNaming` stays a writer-side layout choice for shared-warehouse deployments, and this RFC refuses to serve dynamic reads from those.

### 5.2 Where the tenant predicate lives (RFC 0038 §10 q3, answered for this backend)

**In the relation the container resolves to, not in the statement.** A tenant's warehouse holds only that tenant's rows, so the text needs no tenant predicate. bloomery's `RowPolicy` (`bloomery/src/bloomery/planner/policy.py:37`) is a predicate rendered *into* the statement (`dimension op value`). It is legitimate for scoping *within* a tenant (for example a user's region), but it is authored by the compiler, invisible to RFC 0038's relation-level read-set check, and therefore **never a tenancy boundary**. A `RowPolicy` that carries the tenant id is redundant defense in depth, the same shape the wiring already warns about for `tenant_aware=True` on a routed client (`wiring.py:654-664`). If intra-tenant row security ever has to be a *boundary*, it needs its own container (per-policy views or principals). That is out of scope here.

### 5.3 Swap by wiring

The swap changes nothing on the application side: the port (`ctx.dynamic_read.query(spec)`), the `DynamicReadSpec`, the caps, the clamps and the taxonomy all stay. What changes is which deps module binds the route, plus the compiler's target dialect, since plans are per-dialect regardless. Recommendation for the Postgres MVP: adopt tenant-neutral plans **now**, either `DefaultNaming` plus a routed Postgres client (same text as here) or unqualified names plus a per-tenant `query_schema`. Then the swap really is wiring and dialect only. When RFC 0038 unparks, `CompiledReadPort` reuses this adapter's `_fetch_rows`, and verification sits in the shared shell. Nothing about it is DuckDB-specific.

## 6. Non-goals

- Writes of any kind. The dynamic-read plane stays read-only (RFC 0020 decision 3), and DuckDB lake writes belong to RFC 0028 W3 procedures.
- Raw object storage or static credentials (RFC 0016 decision 6). Shared-instance `namespace` tier (§1). Remote signing (§3.1).
- Untrusted or adversarial provenance. Out-of-process isolation (RFC 0021). Per-statement CPU fairness between tenants.
- A result cache inside the port. Pagination (RFC 0015 decision 9 is RFC 0038's to reopen).
- The DuckDB 2.0 bump. That is its own change, and it re-runs every ✅ [probe] above.

## 7. Acceptance battery

Real legs run against a pinned Lakekeeper (OpenFGA or Cedar authorizer, OAuth2 IdP) and an STS-capable object store, extending RFC 0018 §7's fixture. Lakekeeper tests vending against AWS, Silo (a maintained MinIO fork) and SeaweedFS ✅ (storage-s3). Whether plain MinIO or RustFS honour STS session-policy downscoping is 🔎, and item 1 decides it.

1. **Fixture self-test (negative control, runs first).** Anonymous `GET` is refused. A principal without a grant gets no `LoadTable`. Profiles report `remote-signing-enabled: false`. Tenant A's *vended* credential, used directly (boto3), cannot `GET` tenant B's prefix or `PUT` anywhere. If this fails, the fixture is wrong, and every later pass would be meaningless.
2. Wiring *(unit)*: a dynamic route on the non-routed client → `statement_origin_isolation_floor`. A routed client with static credentials → refused. `provenance="untrusted"` → refused. `guarded=False` → refused. A routed module derives `dedicated`.
3. Disjoint tenants: the same tenant-neutral plan for two tenants returns disjoint rows, with RFC 0038 verification stubbed to accept everything (0038 §8 item 6 shape).
4. Cross-tenant through the catalog: tenant A names B's namespace, or `ATTACH`es B's warehouse → refusal, **never rows**.
5. Cross-tenant by path, with the worker's environment holding the store's **root** key as `AWS_*`: `iceberg_scan('s3://…/B/…metadata.json')` and `read_parquet('s3://…/B/**')` → `permission_denied`. The listener variant asserts no request is signed with the ambient key (RFC 0016 prereq 4).
6. Classification: `COPY`, `ATTACH`, `SET`, `INSTALL`, `CREATE SECRET`, multi-statement → refused before execution. With classification stubbed out, `ATTACH '/local.db'` still fails at the engine (`PermissionException`), proving the belt and the braces separately.
7. **Local filesystem, two legs, asserting refusal and never mount flags:** `read_csv('/etc/passwd')`, `read_text('/proc/self/environ')` and `COPY … TO '/tmp/x'` are refused and no file appears. Then the same `COPY` with step 5 of §3.4 removed is refused by the process filesystem control (RFC 0016 decision 9).
8. HTTP egress: `read_csv('https://…')` → `permission_denied`.
9. Lock: `SET disabled_filesystems=''`, `SET lock_configuration=false`, `LOAD aws` and `duckdb_secrets(redact=false)` are refused, on the parent connection and on cursors.
10. Read-only: `INSERT`/`UPDATE`/`DELETE`/CTAS into `lake` → `write_refused`. With classification stubbed, the catalog or store refuses, which proves the catalog is the read-only boundary. This answers the read-only-vending 🔎 either way.
11. Credentials: (a) revoke A's grant, and A's next statement is refused with no restart or eviction. (b) Set validity to the minimum the store allows (🔎). A statement after expiry succeeds via a fresh `LoadTable`. A statement spanning expiry either succeeds or fails loud, and the result is pinned as documented behavior (duckdb-iceberg#1352). (c) Catalog token expiry → auto-refresh. (b) and (c) are slow and run nightly.
12. Shell: cap+1 → `row_cap_exceeded` (no truncation), byte cap, clamp-down options, timeout on a long query lands as `dynamic_read_timeout` within timeout + `http_timeout`. The RFC 0015 shell battery runs mock ≡ DuckDB.
13. Pool: LRU eviction with a statement in flight lets it complete (guarded). `evict_tenant` → the next call rebuilds with rotated credentials. The process-wide semaphore bounds concurrent statements across tenants.
14. Attestation: a session missing any §3.4 step (fault-injected) refuses to serve with `duckdb_session_attestation_failed`.
15. Capability honesty: each cell of §4.3 has a test that exercises it (RFC 0016 §4 item 5).
16. Taxonomy pin: every DuckDB exception class the battery provokes maps as in §4.5. Lakekeeper 403 and 404 are included.
17. Scope regression (duckdb-iceberg#1390): a table with nested-prefix credentials reads without broadening and without a 403.

## 8. Phases

- **P0 — pickup probes.** Every 🔎 above, run against the pinned `duckdb` + `iceberg` build and the fixture. Findings go to `EXECUTION-LOG.md`, and a flipped fact becomes a decision row (INDEX policy). Gate: items 1, 5 and 11a pass by hand.
- **P1 — the routed read path.** `RoutedDuckDbClient` (§2), the hardening sequence and attestation (§3.4), `run_dynamic`, the adapter, wiring, the taxonomy, docs, and battery items 1–10 and 12–17. It needs RFC 0018 P3's vended-credential fixture. The `IcebergCatalog` value object is built here as RFC 0020 W1's first piece if W1 has not landed, and W1 then reuses it.
- **P2 — tightening.** The `enable_external_access=false` + `allowed_directories` profile if P0 shows catalog traffic survives it. Battery 11b/c in the nightly matrix. Re-probe on DuckDB 2.0.
- **P3 — compiled read.** Bind `CompiledReadPort` when RFC 0038 unparks. This needs no DuckDB-specific work beyond `_fetch_rows` reuse.

## 9. Open questions

1. Does the iceberg REST client's traffic go through the filesystem layer? That decides whether `HTTPFileSystem` can stay disabled in P1, and whether the P2 allowlist profile is reachable at all.
2. Does Lakekeeper vend read-only credentials to a `select`-only principal? If not, read-only rests on the catalog grant plus classification, and the store would accept a `PUT` signed with vended credentials (battery item 10 decides). *Lean: acceptable for trusted provenance, recorded in §4.3 either way.*
3. OOM: add a new `dynamic_read_memory_exceeded` code, or defer as now? It is caller-caused in practice, and `memory_limit` is the only work bound besides the timeout. *Lean: defer until a dashboard hits it.*
4. Idle footprint of an instance with `httpfs` + `iceberg` loaded. It sizes `max_cached_tenants`.
5. A warehouse per tenant, or a shared warehouse with a per-tenant principal granted at namespace level? Both put the boundary in credentials. *Lean: warehouse per tenant (RFC 0018 §3), so the storage prefixes and the P2 allowlist stay per tenant.*

## 10. Decision log

| # | Decision | State |
|---|---|---|
| 1 | This RFC owns RFC 0016 W2 prerequisite 2 (the routed client) and executes prerequisite 4. Prerequisite 1 is consumed from RFC 0018 P3. Prerequisite 3 stays a deployment property with its own battery case (item 7). On landing, RFC 0016's decision table gets a row pointing here, and its prose is not edited (INDEX policy) | proposed |
| 2 | The tier is **`dedicated`**: a per-tenant in-memory instance holding one tenant-scoped catalog principal. `namespace` on a shared instance is rejected because DuckDB has no in-engine identity, so the shared principal reaches every tenant and a naming defect returns rows | proposed |
| 3 | Reuse `StructuredSecretRoutedTenantClientBase` and the shipped `DuckDbClient`. No new pool and no new client class beyond the routed wrapper | proposed |
| 4 | Guarded eviction is mandatory. A process-wide statement semaphore sits over the per-tenant executors, and memory is sized by concurrency, not by cache size | proposed |
| 5 | The cache-partition key is the instance (tenant id). Only unnamed `:memory:`, no persistent secrets, and the global HTTP metadata cache off | proposed |
| 6 | The routed client has no static-credential surface (`object_stores`, `secrets`, `sources`, `bootstrap_sql`). Vended credentials only, `remote-signing-enabled: false`, client credentials and never a static `TOKEN` | proposed |
| 7 | Session hardening is the exact §3.4 sequence, ending in `lock_configuration` and an attestation read-back. A session that fails attestation never serves | proposed |
| 8 | Every DuckDB setting is labelled a *promise* and paired with an external control. The battery removes the setting and asserts the external control still refuses | proposed |
| 9 | Read-only belongs to the catalog grant and the store. Engine-parser classification (single `SELECT`, executing the extracted `Statement`) is a belt that also produces the taxonomy. It does not amend RFC 0015 decision 3, because the parser is DuckDB's | proposed |
| 10 | Timeout is client-side and cooperative, and is stated as such in the capability row. `http_timeout` ≤ the route ceiling | proposed |
| 11 | Trusted provenance only. Untrusted reopens only with a named consumer and an out-of-process engine | proposed |
| 12 | Plans on this path are tenant-neutral (`DefaultNaming`, `USE lake`), and `PrefixNaming` is a writer-side choice. The tenant predicate lives in the container, and `RowPolicy` is never a tenancy boundary | proposed |
| 13 | Same port, spec, caps and taxonomy as Postgres. The swap is wiring plus dialect | proposed |
| 14 | `sts-token-validity-seconds` ≥ 10 × (timeout + `http_timeout`), and `MAX_TABLE_STALENESS` is never set. Mid-statement refresh is not relied on until duckdb-iceberg#1352 closes | proposed |
