# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`SerializeOperation(group=...)` shares one lock between operations.** Operations naming one group take turns per key and tenant, keyed apart from operation names. Freeze refuses a group spread over several transaction routes (`tx_lock_group_spans_routes`), found through the new `SharesTransactionLock` protocol.

- **`runtime_lifespan(nested=...)` runs mounted apps' lifespans inside the runtime's scope.** They enter in order once the scope opens; on shutdown the runtime drains, then they exit in reverse, then it tears down. An app mounting an MCP HTTP app passes `nested=(mcp_app,)` instead of writing its own lifespan.

- **A mock derived source can aggregate child rows.** A `MockDerivedRegistry` source with two required positional parameters gets `(row, view)`, the view analytics resolvers get, so `view.rows(spec)` counts the caller's own uncommitted children. Existing `(row)` sources keep working.

- **`GuaranteeImplying` lets a domain model declare the guarantees its fields need.** A tuple of guarantees in the model's `implied_guarantees` class attribute is added to every document spec over it. `implied_guarantees_of(model)` reads it and refuses anything else as `implied_guarantees_invalid`.

- **A `Reference` guarantee declares a foreign key's `ON DELETE`, which the mock applies.** `Reference(field=, spec=, on_delete="cascade"|"restrict"|"set_null")` on the child spec. Postgres schema validation checks the key and warns about undeclared cascading ones; Mongo and Firestore refuse it.

- **A durable recovery worker can claim only some functions, so slow ones get their own worker.** Register one with `queue="llm"`, then pass `queues={"llm"}` to `durable_recovery_background_lifecycle_step`. Existing tables add the new `(status, name, created_at)` index first.

- **`run_cpu(..., isolated=True)` runs pure-Python compute outside the event loop's GIL.** The runtime holds a lazy `ProcessPoolCpuExecutor` beside its thread pool (`isolated_cpu_workers=`), or an `isolated_cpu_executor=` such as `InterpreterPoolCpuExecutor` on Python 3.14. Inputs must pickle; unbound, it refuses.

- **The isolated executor has its own slot: `bind_isolated_cpu_executor`, `hold_isolated_cpu_executor`, `current_isolated_cpu_executor` and `isolated_cpu_executor_bound`.** They mirror the default executor's, but nothing bound or held gives `None`. `InlineCpuExecutor(pickled=True)` adds the pickle round trip, for tests.

### Changed

- **`configure_cursor_signer(None)` and `configure_cursor_cipher(None)` clear the context's own key** (**Breaking**). The key an open runtime scope holds then applies again. `bind_cursor_signer(None)` and `bind_cursor_cipher(None)` still turn protection off for a block.

- **A spec over a `NumberIdMixin` domain declares `number_id` unique by itself** (**Breaking**). The mock refuses a duplicate, and startup needs the unique index, per tenant on a tenant-aware route. A declared uniqueness over `number_id` replaces it; on Firestore, set `implied_guarantees = ()` on the model.

- **`DocumentSpec` keeps what `guarantees=` passes in a field named `declared_guarantees`** (**Breaking**). Passing `guarantees=` and reading `spec.guarantees` still work; the latter adds what the domain model implies. A numbered domain whose read model does not store `number_id` is now refused when the spec is built.

- **The mock's `kill_many` deletes all of its rows or none** (**behaviour change**). A missing id or a declared `restrict` now refuses before any row is removed, as on Postgres.

- **Mock state snapshots written before this release are refused at load** (**behaviour change**). A snapshot now carries the declared references with the rows; delete the old file to start fresh.

- **`DurableRunStorePort.claim_abandoned` takes `functions=`** (**Breaking**). A run store implemented outside forze must accept it. The Postgres recovery indexes `(status, created_at)` and `(tenant_id, status, created_at)` are replaced by `(status, name, created_at)` and `(tenant_id, status, name, created_at)`.

- **`run_cpu` reserves the keyword `isolated`** (**Breaking**). A call that passed `isolated=` through to its function now selects the isolated executor instead; pass that argument with `functools.partial`.

### Fixed

- **`run_cpu` in a request offloads to the runtime's pool instead of running inline.** A runtime scope holds its pool process-wide while open, the oldest scope's when several are. New `hold_cpu_executor`; a closed `ThreadPoolCpuExecutor` refuses work (`cpu_executor_closed`).

- **Postgres schema validation no longer warns that declared derived read fields are missing from the write relation.** A field in `derived_read_fields` comes from the read view, so the startup warning now names only read fields that are neither derived nor omitted.

- **Startup accepts an index over `tenant_id` plus the fields for a guarantee on a tenant-aware route.** It keeps the guarantee per tenant, as the mock does, so Postgres and Mongo print that index when it is missing. The Postgres undeclared-index warning no longer suggests naming `tenant_id`, which a spec cannot.

- **A Postgres hard delete refused by a foreign key raises `conflict`, not `not_found`** (**behaviour change**). `kill`/`kill_many` on a row a plain foreign key still references no longer reads as "Reference document not found"; `ON DELETE RESTRICT` maps to `conflict` too.

### Security

- **Cursor signing and encryption set on an `ExecutionRuntime` apply to every request** (**Breaking**). Code outside the scope's task now mints and requires protected cursors, refused (`cursor_protection_ambiguous`) while open scopes hold different keys. New `hold_cursor_signer` and `hold_cursor_cipher`.

## [0.17.0] - 2026-10-30

### Added

- **Counters can expire: every write verb takes `ttl`.** An increment that creates a counter starts a fixed lifetime no later write extends, and `reset` replaces it; a lapsed counter reads as absent. Postgres needs an optional `expires_at` column, and `counter_capabilities(port).expiry` reports support.

- **`LockoutConfig.lock_for` keeps a locked login locked past its window.** The failure that reaches the threshold stores an unlock instant on a separate lock key. The login unlocks at the later of the window's end and that instant, and `Retry-After` carries the later one.

- **`postgres_counter_purge_lifecycle_step` deletes lapsed Postgres counter rows on an interval.** It calls `purge_expired()` for each route in `specs`, and binds each tenant in turn with `tenants=`.

- **An agent or MCP tool can advertise and accept fewer query fields than the spec.** `operation_tools`, `register_tools` and `build_mcp_server` take `query_policies`, a narrower `QueryFieldPolicy` per operation. The description names only its fields, the field guard checks calls against it, and widening fails at build.

- **Federated search takes `member_filters`, a filter for one member's leg alone.** It joins `filters` with `$and` on that leg, and a hub refuses it. New `SearchMemberGrant`, `bind_search_member_grant`, `OperationDescriptor.members` and `AuthorizesMembers` back member authorization.

- **`MockDepsModule(row_locks="block")` makes a held row lock wait under simulation, as Postgres does.** Two transactions taking rows in one order now both succeed. Opposite orders abort one with `deadlock_detected`, and DST reports a `no_lock_order_deadlock` violation naming the rows.

- **A document write can refuse any stale revision, history or not.** `update(..., merge=False)` and `update_many(..., merge=False)` raise `revision_mismatch` for any `rev` but the stored one, even for a no-op, instead of merging it. `touch(pk, rev=...)` and `touch_many(pks, revs=...)` bump only rows still at it.

- **A handler can take a spec's `SerializedBy` lock before it reads.** `hold({"order_id": order_id})` on the document command port takes the lock its writes take, without writing, until the transaction ends. Refused outside a transaction, for a datetime, validator-rewritten or serialized key, and on Mongo and Firestore.

- **`SerializeOperation(key=...)` makes invocations of an operation that share a key take turns, each until its transaction ends.** It holds `ctx.tx_locks(route)`: `pg_advisory_xact_lock` on Postgres, the `SerializedBy` locks on the mock. Freeze refuses it without a transaction route, and resolve on Mongo or Firestore.

- **DuckDB serves trusted dynamic reads over a per-tenant Iceberg lake.** `RoutedDuckDbClient` gives each tenant a locked-down instance holding its catalog principal and the S3 credentials vended per statement, and serves nothing from one that fails its attestation. Wire routes with `DuckDbDepsModule(dynamic_reads=...)`.

- **`attach_readiness_route` takes a runtime provider.** Pass a zero-argument callable returning the runtime, or `None` before it exists; it is asked on every request. No runtime, or a provider that raises, answers `503 unavailable`.

- **`runtime_lifespan(build=...)` builds the runtime when the app starts.** An app factory then imports without the runtime's environment, and each worker process builds its own. Wire routes with `ctx_dep=lifespan.get_context` and the probe with `lifespan.runtime`; a build that raises fails the startup.

- **`runtime_lifespan(drain_budget=...)` drains on `SIGTERM` in every uvicorn worker.** Under `uvicorn --workers N` or gunicorn's uvicorn worker, each worker drains its own runtime while it still serves, then lets its server exit. An app that does not use `runtime_lifespan` gets the same from `drain_ahead_of_uvicorn`.

- **`bind_max_in_size` raises the `$in` size limit for one block of trusted code.** Application code matching an id list it built itself no longer chunks it under the spec's limit, which still bounds every caller-supplied filter. A backend's own cap, such as Firestore's 30, still applies.

- **A hand-written upload route can cap its read like the generated one.** `forze_fastapi.routes.read_upload_capped(file, max_size)` returns the multipart file's bytes, or refuses a file over the cap with the generated route's 422 `upload_too_large`.

- **`serve_runtime` drains a process that serves no HTTP on `SIGTERM`.** The counterpart of `serve_uvicorn` for relays, consumers and workers holds the runtime's scope open, runs `runtime.drain()` on the first `SIGTERM` or `SIGINT`, or when `stop` is set, then tears down.

- **Generated routes describe the parameters they declare in OpenAPI.** A document's `id` and `rev`, a storage `key`, and the tenancy and API-key `id`s need no `app.openapi` patch. A `RouteBinding` of yours takes `param_descriptions`.

- **`apply_openapi_conventions(app, tag_groups=...)` writes `x-tagGroups`, the sidebar groups Scalar and Redoc render.** A listed tag no operation uses is left out with a warning, and tags no group lists join a trailing `Other` group.

- **`attach_tenancy_routes` takes the `cookies=` carrier `attach_authn_routes` does.** A tenant switch on cookie auth then rotates both session cookies and strips the tokens from its body, behind login's CSRF gate, so the browser's session takes the new tenant.

- **A declared HTTP service can stream a response.** `HttpServicePort.stream(op, args)` yields the body as it arrives, governed as `invoke` is, and raises a rejection before the first chunk. `HttpClient.stream` and `RoutedHttpClient.stream` do the same at the client.

- **A vendor SDK's requests can go through a declared HTTP service.** `http_service_transport(ctx, spec)` gives an `HttpServiceTransport` for `httpx.AsyncClient`: requests stay on the service's route, its headers replace same-named ones, and deadline, trace and egress tag apply. Custom `HttpClientPort`s must add `send`.

- **Agent tools can state the filter grammar once.** `operation_tools(shared_filter_grammar=True)` strips it from tool schemas as MCP does; `OperationToolset.filter_grammar` holds it for the system prompt. `forze.application.contracts.querying` gains `filter_grammar`, `without_filter_grammar`, `takes_filter_expression`.

- **An `openai_chat` route can serve a host that refuses a schema constraint, such as DeepSeek.** `structured_output="json_object"` puts the output model's schema in the system message, asks for valid JSON, and refuses a reply that does not fit as `inference_output_mismatch`. `json_schema` stays the default.

- **`forze_identity.tenancy.application` exports `TenancyResourceName`.** It matches `AuthnResourceName` in `forze_identity.authn.application`.

- **A throttled HTTP error tells the client when to come back.** Its `retry_after` (seconds) or else `retry_at` (aware `datetime`) detail becomes a `Retry-After` header, which `AllowlistCORSMiddleware` exposes; other details stay withheld. No header when neither key is usable.

- **A locked login's 429 tells the client when to try again.** The `login_locked` throttle carries the window's end as `details["retry_at"]`, which FastAPI sends as `Retry-After`; the body still withholds details. `LoginLockoutGuard.locked_until(digest)` returns that instant, or `None` when the login is not locked.

- **An `unavailable` error kind answers 503 for something switched off right now.** `exc.unavailable(...)` is retryable, keeps its summary and code on every transport, and is not logged as a server error; details stay withheld. A `retry_after` or `retry_at` detail sets `Retry-After`, as on a throttle.

- **Retries wait out a `retry_at` hint too.** A throttled or unavailable error's aware `retry_at` detail now delays the resilience, saga and durable retries until that instant, read as the `Retry-After` header reads it. `retry_hint(error)` in `forze.application.execution.resilience.backoff` returns the hint as given.

- **`RoutedDuckDbClient(ca_cert_file=...)` lets a lake session use HTTPS on DuckDB 2.** The file must hold a PEM certificate, and any statement on the instance can read it. A dynamic read calling a table function other than `range`, `generate_series` or `unnest`, naming a path, or naming an unattached catalog is refused.

- **A cancel after an operation's commit point is recorded on every surface.** The operation logs a `commit_ambiguous` warning carrying its name and invocation ids, adds a `forze.commit_ambiguous` span event, and re-raises the cancellation. `forze ops call` still reports it as an error.

- **A transaction manager can say its store refused writes in a read-only root.** Implement `ReadOnlyAware.read_only_enforced()`, as Postgres and the mock's `journal` and `strict` managers do; that root then marks no commit point.

- **A derived read field can resolve an expanded reference.** `DerivedReadField(source=..., via=..., fields=("id", "name"))` makes the mock build a nested object from the one joined row, under `field`'s one-hop and `optional` rules. An optional join that finds no row yields `None` for the whole object.

- **`run_derived_fidelity` checks the mock's derived fields against the real view.** It loads one seed into a real backend and the mock, reads the same ids through both, and reports each derived field that differs, per row. Import it from `forze_dst.conformance`.

- **`forze_fastapi.run_startup_checks(app)` runs every FastAPI startup check in one call.** It checks the app and each Starlette or FastAPI app mounted beneath it, and `runtime_lifespan` calls it. An app with its own lifespan calls it there, on the outermost app: Starlette never runs a mounted app's lifespan.

- **Postgres startup warns about a unique index or `EXCLUDE` constraint that no guarantee declares.** The in-memory store does not enforce it, so a test run accepts writes the database refuses. It suggests the declaration; the primary key, an index over its columns and framework-owned specs are left out.

- **`postgres_document_schema_specs(registry, deps)` validates every bound Postgres document.** It pairs each route with its catalogued `DocumentSpec` for `schema_specs`, and refuses a route with no such spec or a read-write route over a read-only one. `exclude=` skips routes; a per-tenant one is skipped with a warning.

- **The mock can compute an analytics query from its current documents.** Register `fn(view, params)` with `MockDepsModule(analytics=MockAnalyticsRegistry().on(spec, query_key, fn))`; `view.rows(spec)` reads stored rows as the caller's transaction sees them. A query with no resolver keeps its seeded rows.

- **`forze_dst.conformance.compare_analytics_query` checks a mock resolver against the SQL it stands in for.** It loads one seed into both engines, runs the query on each and lists every differing row. `query_invariant(spec, query_key, holds=...)` turns the same query into a DST oracle.

- **SSE- and WebSocket-only apps get offline delivery without Socket.IO.** `RealtimeMailboxWriter` and its lifecycle step fill the mailbox those routes replay from, using the gateway's store code. They and the signal sources live in `forze.application.execution.realtime`; `forze_socketio` re-exports what moved.

- **A `SystemInvariant` can compare two read-sets per scope.** `against=Measure(...)` bounds a sum by another sum or by a parent's field, and `holds` takes both aggregates. Every scope with rows on either side is checked, so an order left with no lines breaks `orders == 0 or lines >= 1`.

- **Stored files can stream and presign downloads, opt-in.** `StoredFileKitSpec(stream_downloads=True, presign_downloads=True)` registers `download_stream` and `presign_download`; guard each like `download`. Generated surfaces refuse the sensitive `presign_download`. A route streams with `streamed_download_response`.

- **`OperationDescriptor(streams=True)` keeps a byte-stream result off JSON surfaces.** MCP and `forze ops` leave such an operation out by default; they, agent tools and `attach_operation_routes` refuse it by name. The storage and stored-file kits' `download_stream` set it (**behaviour change**).

**Self-hosted durable functions:**

- **A cron trigger declares a timezone and an input.** `DurableFunctionCronTrigger(expression, tz=..., input=...)` registers its schedule with both; an unknown zone or an input not of the spec's `args_type` fails the declaration. A changed input alone keeps the next fire; Inngest maps `tz` to `TZ=` and refuses an input.

- **Finished runs can be purged past a retention window.** `DurableRunAdminPort.purge_finished(older_than, ...)` deletes up to `limit` runs that landed before the window, with their step journal, freeing their idempotency keys. `durable_run_retention_lifecycle_step` sweeps; a hand-built store needs `step_config`.

- **An enqueue can coalesce onto the run waiting unclaimed.** `enqueue(..., coalesce_key=...)` joins a `pending` run never claimed under that key, dropping the submit's own input and `run_at`, and records a new run once it is claimed. **Migration:** add the `coalesce_key` column and its partial unique index first.

- **An operation-backed function can run as a service principal.** `register_operation_functions(..., identity=DurableServiceIdentity(...))` binds it around the operation, so `AuthnRequired`, authz and audit see it; the identity must be acknowledged as unverified. Without one, a guarded operation refuses the run.

- **A presigned download can name the file it saves as.** `presign_download(key, expires_in=..., response_filename=...)` on the storage query port signs an RFC 6266 `Content-Disposition` into the URL on S3, GCS and the mock. The stored-file kit's `presign_download` passes the stored filename.

- **Stored files can serve byte ranges, opt-in.** `StoredFileKitSpec(range_downloads=True)` registers `download_range`, with `download`'s row checks. JSON surfaces leave it out. `attach_stored_file_download_route` serves `GET {id}/content` and answers `Range` and conditional requests as the storage download route does.

- **Stored files can upload straight to the bucket, opt-in.** `StoredFileKitSpec(direct_uploads=True)` registers `initiate_upload`, `presign_upload_part` and `complete_upload`: a key the kit mints, then a create-only presigned PUT or parts. Completion checks size, type and `max_bytes`, and fails the row on a mismatch.

- **`stored_file_pending_sweep_lifecycle_step` fails abandoned stored-file uploads and frees their bytes.** A row `pending` with no update for `older_than` (24 hours) is marked `failed` at its revision, then its multipart uploads, object and key go; a failed cleanup is retried. `presign_upload_part` touches the row.

- **A presigned upload and a multipart completion can refuse to replace an object** (**Breaking**). `create_only=True` (never with `if_match`) makes `presign_upload` sign `If-None-Match: *` or `x-goog-if-generation-match: 0`, and `complete_upload` refuse an existing key. A custom `ObjectStorageClientPort` must accept it.

- **A durable service identity can be an API key verified on every run.** `DurableServiceIdentity(api_key=SecretRef(...), authn_spec=...)` runs the body as the key's principal, a member of the run's tenant. A revoked, expired or delegated key fails the run, within cached key states' `ttl` if a revocation's write is lost.

**`forze ops` for people at a shell and for backfills:**

- **`forze ops` gains typed flags, JSONL batches and shell completion.** `call OP --field VALUE` takes a flat-scalar input as flags; `--batch @PATH|-` runs one call per line in one runtime, stopping at the first failure; `OP` completes in the shell. An unknown option after `OP` now exits 2 with an error object.

### Changed

- **Postgres schema validation reports every refused document in one `postgres_schema_documents_refused` error** (**behaviour change**). Its `details["failures"]` lists each; a single refused document still raises its own error. A connection or partition error stops the checks and raises the refusals found before it.

- **`incr(by=0)` no longer creates the counter it reads** (**behaviour change**). An absent counter reads 0 and stays absent, so `list_counters` and a portable export no longer show it at 0.

- **A draining instance refuses new work with a 503, not a 429** (**behaviour change**). The drain gate now raises `unavailable` with `code="draining"`, so a load balancer retries elsewhere, and no resilience retry repeats it, whatever its `retry_on`. `is_draining_refusal` still matches it, by code.

- **HTTP error bodies carry the error's `code` and `kind`** (**behaviour change**). The body is `{"detail", "code", "kind", "context"}`, so a client tells `revision_mismatch` from other 400s; a whole-body comparison breaks. An unhandled 500 now also sends `X-Error-Code: core.internal`.

- **The DuckDB lake session no longer disables any filesystem** (**behaviour change**). On 1.5 as on 2 it relies on an allowlist of the tenant's prefix, the catalog's tree and the token endpoint, plus attested settings. That tree is shared by every tenant, and analytics SQL can send it unauthenticated requests.

- **The `duckdb` extra takes DuckDB 1.5 or 2** (**behaviour change**). It now requires `duckdb>=1.5.6,<3`, so releases before 1.5.6 no longer install with it. `DuckDbClient` and `RoutedDuckDbClient` run on both majors with the same code.

- **`HttpServicePort` requires `stream`** (**behaviour change**). A hand-written implementation or test stub must add it, or it fails type checks and `isinstance` against the runtime-checkable port.

- **A durable body never inherits the caller's principal** (**behaviour change**). `run_now` inside a request now runs an untagged run as no one, as recovery always did: audit, `CreatorIdMappingStep` and progress jobs record no actor, and guarded saga steps refuse. Name a principal with `DurableServiceIdentity`.

- **Durable store ports gain members** (**behaviour change**). `DurableRunAdminPort` requires `purge_finished`, `DurableScheduleStorePort` requires `set_input`, and `DurableRunStorePort.enqueue` takes `coalesce_key`, which `DurableFunctionRunner.enqueue` always passes. A hand-written store must add them.

- **`local_identity_deps` wires no tenant resolver for a config that maps no tenant** (**behaviour change**). There, `forze ops call --tenant` gets `400 tenant_request_unverifiable`, not `401 tenant_mismatch`. A tenant on an API key counts and resolves, also in a config built directly.

- **MCP and agent tool descriptions group filterable fields by type** (**behaviour change**). `describe_query_discovery` states the operators every field shares once, then each further operator set once, with its fields under their own types. `QueryDiscovery` and `x-forze-query` are unchanged.

- **A `SystemInvariant` sums a `Decimal` field exactly** (**behaviour change**). `holds` gets a `Decimal`, not a `float`, so float arithmetic in it (`total * 1.1`) raises `TypeError`; the per-commit oracle sums one instead of refusing it. A count is an `int`, printed `3` not `3.0`; other sums stay `float`.

- **The dynamic-read write refusal no longer names a READ ONLY transaction.** The message is engine-neutral and the details carry the engine's `statement_type` where it has one. A `DuckDbClient` statement timeout now carries the code `duckdb_query_timeout`.

- **An encrypting storage route refuses the stored-file kit's direct operations at resolve** (**behaviour change**). `presign_download` and the direct uploads resolve storage as the new `StorageSpec(direct_transfer=True)`, which a client-side-encrypting route refuses (`configuration`), so `check_wiring` reports them.

- **The stored-file kit's buffered `download` sets `streams=True`** (**behaviour change**). JSON cannot carry a binary file, so MCP and `forze ops` failed on one. Both now leave `download` out by default, and MCP, `forze ops`, agent tools and `attach_operation_routes` refuse it when named.

- **`StorageSpec` gains `direct_transfer`, so every storage spec's inventory fingerprint changes** (**behaviour change**). Portability archives and `migrate` fingerprints taken before the upgrade no longer match; take them again after it. The new field defaults to `False`.

- **`StorageUploadSessionPort` requires `list_uploads`** (**Breaking**). `list_uploads(key)` returns the sessions open at a key, so a cleanup that lost their handles can abort them; S3, GCS and the mock have it. A custom port, or an `ObjectStorageClientPort` (`list_multipart_uploads`), must add it.

### Fixed

- **A storage download no longer fails with a 500 on an odd `Range`.** A position in non-ASCII digits is malformed and serves the whole file; one too long to convert reads as past the end, so it answers 416 or clamps. An upload's `Content-Length` is read the same way.

- **Firestore `update_many` and `touch_many` work inside a transaction.** They read every row before writing any, as a Firestore transaction requires; a second row used to fail with a read-after-write error.

- **Postgres schema validation accepts the required fields of a view-backed read model.** Postgres records `NOT NULL` on tables only, so a read relation that is a view or a materialized view no longer has its nullability checked. Write and history relations keep the check.

- **The login lockout writes a key only for a failed attempt or a reset** (**behaviour change**). Each lives a window plus a minute from creation; Redis drops it, Postgres, Mongo and Firestore need a purge or TTL setup. On a Postgres table without `expires_at` keys get no ttl and pile up, with a warning per table.

- **The login lockout no longer grows its counter store without bound** (**behaviour change**). A check writes no key, and a window's bucket lapses a minute after the window; Redis, the Postgres purge step, a Mongo TTL index or a Firestore TTL policy reclaims it. Without Postgres `expires_at` it writes no ttl and warns.

- **S3 and GCS streams and encrypted ranged reads no longer mix two versions of an object** (**Breaking**). A replacement mid-read, or a GCS metadata-only change, fails the next read with `conflict` (`READ_PRECONDITION_FAILED_CODE`). A custom `ObjectStorageClientPort` must accept `if_match` on `download_range_bytes`.

- **`HttpClient` reads a compressed response.** A gzip or deflate body, and with no cap a zstd or brotli one, no longer fails `request`, or a rejected response from `stream`, as an infrastructure error. `RoutedHttpClient` and declared services are fixed with it.

- **A federated result snapshot replays only for the same members and member filters.** Its fingerprint now covers each member's weight and leg filters, and a thin replay drops a row its member's filters no longer match.

- **A datetime `SerializedBy` key locks its UTC instant** (**behaviour change**). One owner's create and update no longer lock apart by offset; naive reads as UTC (right in a UTC session), an out-of-range instant is refused. Drain writers of naive or non-UTC keys while deploying, or accept a brief unlocked window.

- **An operation span stays clean on a failure the caller can handle** (**behaviour change**). OpenTelemetry's own handling marked every failed operation's span `ERROR` whatever its kind and recorded a server error's exception twice. Now only a server-side failure sets `ERROR`, with one exception event, as port spans do.

- **A durable schedule with no timezone fires in UTC whatever zone `now` is given in.** A non-UTC `now` passed to `DurableScheduler.put` or `tick` moved its fires by that zone's offset.

- **A timezoned durable schedule no longer fires twice when the clocks fall back.** A wall time the clocks repeat fires at its first occurrence only, unless the expression runs every hour; a wall time they skip still fires once, when the gap ends.

- **A warning logged before `configure_logging` carries the invocation's ids.** The fallback renderer now merges the bound logging context, as the configured chain does.

- **`register_scalar_docs` loads the document the app actually serves** (**behaviour change**). It fetched `/openapi.json` whatever `openapi_url` said, so a versioned app got an empty `/docs`. It now defaults to `app.openapi_url` and takes `openapi_url=`; an app serving no document fails at startup unless it passes one.

- **`forze[duckdb]` works on a fresh install of DuckDB 2.0.** 2.0 removed `fetch_arrow_table`, so every `DuckDbClient` query failed; the client now fetches with `to_arrow_table`, which 1.5 has too.

- **A principal in several tenants reaches the tenant selector after a password login** (**behaviour change**). With no `tid` the resolver binds no tenant, as for no membership, instead of refusing every request with `tenant_ambiguous`. Tenant-scoped work still refuses it with `tenant_required`.

- **An authz route with `require_invocation_tenant` refuses a caller with no tenant bound with `401 tenant_required`.** It answered `500`, unlike `TenantRequired` and the tenant-aware stores.

- **`TenancyDepsModule` refuses a `tenant_resolvers` override for a route `tenant_resolver` does not name** (**behaviour change**). It registered nothing, so the first request failed with `Tenant resolver is not registered`; now the module fails when built.

- **`AuthnDepsModule` refuses an override that would never be registered** (**behaviour change**). That is one for a route `authn` does not name, or a verifier override for a method its route does not enable. It registered nothing, so the route silently kept its default.

- **A query interrupted after its read no longer reports `commit_ambiguous` on Postgres or the mock's `journal` and `strict` managers** (**behaviour change**). When its read-only transaction was the only one it entered, it reports `deadline_exceeded` (CLI exit 6) or `interrupted`, and is safe to re-run.

- **A Postgres dynamic read no longer collapses duplicate column names.** `SELECT a.id, b.id` came back as one `id` holding the second value. A result with rows now fails with `dynamic_read_statement_invalid`, naming the duplicates; an empty one still returns no rows.

- **A timed-out `DuckDbClient` statement returns at its timeout.** The caller no longer waits for a hung HTTP request to give up, about two minutes on the Iceberg path; the worker keeps its thread until DuckDB unwinds, and `close` still waits for it.

- **A cancelled `DuckDbClient` caller now interrupts its statement.** The statement ran to completion and kept its executor thread; it now stops as a timed-out one does.

- **Two quick `SIGINT`s to `serve_uvicorn` now give the drain phases up.** A second one handled in the same loop turn as the first cancelled the drain before it started, so the forced exit still ran every phase at length in teardown.

- **Agent tool names are ones model providers accept** (**behaviour change**). `operation_tools` names `orders.list` as `orders__list` (`provider_safe_tool_name`) or as `name=` says; the old name, the op key, still dispatches. A name outside `^[A-Za-z0-9_-]{1,64}$`, or one two operations share, is refused at build time.

- **An aliased input field no longer loses the caller's value.** `forze ops call` and agent tools refuse a name the DTO's config does not read, which pydantic dropped, running the operation on the default. MCP reads each field by the name its tool signature advertises, so an aliased field is no longer dropped there.

- **Each Inngest function call mints its own execution id** (**behaviour change**). The producer's id becomes the causation; the correlation id is kept, or derived from the run. An idempotency claim belongs to one handler entry, so one a crashed entry left pending blocks a retry until its TTL, as on Temporal.

- **A malformed Forze id header no longer fails a Temporal task.** An execution or correlation id header that is not a UUID is dropped, and the workflow or activity runs under freshly minted metadata. Principal, actor and tenant headers decode as strictly as before.

- **Duplex realtime commands get their own invocation metadata** (**behaviour change**). A `cmd` frame on `attach_realtime_ws_route` and a Socket.IO command event each mint a fresh execution id and, with no caller id to carry, a fresh correlation id, so the operation's logs, spans and integration events carry them.

- **A Socket.IO command event runs under its connection's tenant** (**behaviour change**). On a connection `attach_realtime_connection` admitted, an event bound the principal but no tenant, so a tenant-aware operation failed `tenant_required`. It now binds the tenant resolved at connect, as the WS route does.

- **A Postgres update with a changed `jsonb` field no longer answers `500` on the generated update route** (**behaviour change**). Its diff holds the values the caller set, as on the other backends: no `Jsonb` wrapper, a `StrEnum` stays a member and a datetime is as given, not coerced to the column type.

- **A Postgres document field stored in a native `jsonb[]` or `json[]` column can be written.** Create, update and upsert, single and batch, failed with `cannot adapt type 'dict'`; each element is now sent as JSON, a `None` one as SQL `NULL`, and reads back as written.

- **A Postgres locking read works when the read relation differs from the write relation (a view, typically).** `get`, `find`, `project` and `select` with `for_update` failed on a view that joins with `LEFT JOIN`. They now lock the row on the write table, honouring `nowait` and `skip_locked`, then read.

- **Postgres startup accepts a plain unique index for a `skip_null` guarantee.** Postgres already treats a tuple holding a null as distinct, so no predicate is needed, and a deferred constraint keeps `skip_null` at commit. A `where` ruling out nulls in a column no longer demands `NULLS NOT DISTINCT`.

- **Postgres startup no longer warns about a `tenant_id` column the document model maps itself.** Such a table stores the tenant as data, as identity's sessions and tenant memberships do, so the column is not an unused isolation column.

- **A filter on an `Annotated` field is checked against the type it wraps** (**behaviour change**). `Annotated[str, ...] | None` and its list elements resolved to no type, so discovery advertised every operator and `$gt` passed. They now get their type's operators; a string bound on an annotated number or date is cast.

- **Filters and sorts see through `Annotated` fields** (**behaviour change**). `Annotated[str, ...] | None` and list elements took any operator unchecked; now they get their type's and a string bound is cast. A sort into an annotated submodel resolves; Postgres compares annotated `Decimal` elements to an int numerically.

- **Backends and spec checks see through `Annotated` and `type` aliases** (**behaviour change**). A Postgres filter via `Annotated[M, ...] | None` returned 500; BigQuery parameters and `forze ops` flags misread them. A schema, index, null or graph-key mismatch fails at startup; a mock seed nulls an aliased optional link.

- **The mock matches a plain `Enum` field by its value, as Postgres does.** A filter naming `"open"` matched no row holding `Status.OPEN` unless `Status` was a `StrEnum`. Equality, membership, ordering and set operators now compare the value, in arrays and nested objects too, as do the DST oracles.

- **A plain `Enum` document field is stored as its value on Postgres, MongoDB and Firestore.** Postgres wrote `Status.OPEN`, as it did for a `(str, Enum)` class, which no read could decode; MongoDB and Firestore refused it. A `StrEnum` or `IntEnum` is stored as before.

- **Analytics ingest stores a plain `Enum` member as its value, as a document row does** (**behaviour change**). Postgres wrote its name (`OPEN`), and ClickHouse refused it. Rows ingested earlier keep the name until rewritten.

- **A download's `Content-Disposition` encodes `/` in `filename*`.** The generated storage download routes and `streamed_download_response` encode every character outside RFC 5987 `attr-char`, so a name such as `q3/rëport.pdf` stays valid and clients keep it.

- **GCS `list_parts` reports each part's stored size.** It reported `0` for every part, so a caller resuming or bounding a multipart upload could not tell how much had landed.

- **S3 presigned URLs are always signed with SigV4** (**behaviour change**). With no region configured, botocore fell back to legacy SigV2, which newer AWS regions refuse and floci serves as anonymous. `S3Config(signature_version=...)` still overrides.

- **`If-None-Match: *` answers 304 for an object without an ETag.** The streaming storage download route served it a 200; per RFC 9110 the wildcard matches any existing object, on the stored-file download route too.

- **A password change or reset racing its principal's deactivation no longer fails on `revision_mismatch`, nor does the deactivation.** In a Postgres transaction they lock the account (a `read` view must accept `FOR UPDATE`), so the second waits; elsewhere the deactivation rereads a stale credential and closes it.

- **In-place search and analytics read a document's sealed fields.** Name the document with `sealed_by` on the search, hub or analytics spec; under `binds_record_id` each row must also carry its `id`. Meilisearch and analytics `ingest` refuse `sealed_by`, since they seal their own rows.

- **Spec-time encryption refusals carry the documented code.** Analytics `binds_record_id` without `sealed_by`, or `sealed_by` with `ingest`, raises `core.analytics.encryption_wiring`. Procedure `binds_record_id` gets `core.procedures.encryption_wiring`; a hub member's other `sealed_by`, `core.search.encryption_wiring`.

### Security

- **An untagged caller can no longer match a tenant's durable idempotency key or schedule id** (**Breaking**). Every untagged key and id, deployments without tenancy included, is now stored as `:{key}`; rows written before keep the old form. A resubmit of an in-flight key written before the upgrade starts a second run.

- **A document query policy checks `$having` and aggregate sorts against their source fields** (**Breaking**). Refused now: `$having` over a field that is not filterable, and a sort by an alias whose field is not sortable. Allowed now: a sort by a `$count` alias, or by an alias whose field is sortable.

- **A guarded hub or federated search authorizes each member it reads** (**Breaking**). New `AuthzSearchMembers` checks each member's read action; the adapter then refuses a request naming a forbidden member and skips one it only reaches. The registry refuses to freeze a guarded search without that search's hook.

- **`AuthzDocumentScopeWrap` in an operation's outer scope applies its row filter** (**behaviour change**). A wrap bound with `bind_outer().wrap(...)`, as the authz recipe does, now hands the handler the arguments it passes on, so a scoped list returns only the rows the policy allows.

- **`max_response_bytes` bounds a compressed body's decoded size.** (**behaviour change**) Decoding stops one byte past the cap, so a small compressed body cannot expand beyond it. A client with a cap asks only for `gzip` and `deflate`, and refuses any other content coding or a truncated compressed body.

- **A realtime connection needs a tenant once tenancy is wired** (**behaviour change**). Socket.IO, WebSocket and SSE refuse one without a tenant (`tenant_required`), and Socket.IO an anonymous one, instead of admitting it to the untenanted rooms. Socket.IO checks this at the connect when given the new `ctx_dep`.

- **A stale access cookie could keep a cookie-mode logout from ending the session.** `runtime_lifespan` now refuses an app whose `anonymous_paths` misses a cookie-mode login, refresh or logout route (`check_authn_cookie_paths`, **behaviour change**).

- **`bypass_paths` and `allowed_websocket_paths` admit more paths** (**behaviour change**). On a mounted FastAPI app or under a server root path, an entry matches the full path or the path relative to `root_path`, as `anonymous_paths` does. `root_path` must come from server configuration, never from a request header.

- **The FastAPI startup checks cover every mounted Starlette or FastAPI app** (**behaviour change**). The boot fails on a bypass reaching a mounted generated route, on a bypass or websocket allowlist entry that could reach a mounted app they cannot inspect, and on a mounted FastAPI app overriding its `root_path`.

- **Postgres dynamic-read routes refuse an unknown `provenance` value** (**Breaking**). `PostgresDynamicReadConfig` accepts only `"trusted"` or `"untrusted"` (`postgres_dynamic_read_provenance_invalid`); a config with any other value now fails at startup. New `validate_dynamic_read_provenance`.

- **The storage kit's `presign_download` is sensitive** (**Breaking**). MCP, `forze ops`, agent tools and generated routes refuse it, so `attach_storage_routes` no longer serves it; an MCP server or `OpsApp` over the storage registry needs `operations=` without it. Trace capture masks presigned URLs.

- **A tenant-sharded realtime consumer refuses a stream route that is not `tenant_aware`** (**behaviour change**). `TenantShardedSignalSource` and `realtime_sse_sharded_tail_lifecycle_step` fail with `tenant_aware_required`, so a signal is never delivered or stored under another tenant. New `require_tenant_aware`.

- **A nested identity scope no longer logs the outer principal.** `bind_identity` and `bind` now clear the principal, actor and tenant log fields they do not set, and restore them when the scope ends.

## [0.16.2] - 2026-10-26

### Added

- **`forze ops` runs your operations from a terminal, cron or a Kubernetes Job, governed as over HTTP.** Declare an `OpsApp` with a registry, a `SERVERLESS` runtime factory and an identity. `call` verifies an API key, resolves the tenant, and prints one JSON document with a stable exit code.

### Fixed

- **Postgres `jsonb[]` and `json[]` arrays treat a SQL NULL element as JSON null.** A `None` member of an element `$in` now matches it, and one of `$nin` now excludes it, as in the in-memory evaluator.

- **MCP reads an API key and its tenant as the HTTP edge does.** A bearer ending in `:` is a prefix with an empty secret, not the bare key before it, and a verified key's tenant is bound when the tenancy resolver has no binding for the principal.

- **An outbox relay wired to a missing or mismatched destination fails at wiring.** The background relay step checks its destination when built, and the refusal is now a `configuration` error coded `outbox_relay_misconfigured` (HTTP 500) for direct `OutboxRelay` callers too. It was a `precondition`, logged every batch.

## [0.16.1] - 2026-10-23

### Added

- **A Postgres vector search can rank by stored vectors alone**: `VectorEngine.embeddings_name` is now optional. Without it no embeddings provider is resolved, and the search ranks by a `query_vector` and refuses a text query with code `search_query_vector_required`.

- **A Mongo vector search can rank by stored vectors alone**: `MongoVectorEngine.embeddings_name` is now optional. Without it no embeddings provider is resolved, and the search ranks by a `query_vector` and refuses a text query with code `search_query_vector_required`.

### Changed

- **A Postgres vector search no longer formats its query vector for the fingerprint**: without result snapshots the fingerprint is skipped, and with them a `query_vector` is keyed by a digest, saving about 0.25 ms per 1536-wide search. Snapshots taken for a `query_vector` before the upgrade miss once.

- **A Postgres ranked search reads its relation once**: when it is both read relation and heap and no highlights are asked for, the ranked CTE carries the page's columns instead of a join back, so a view is planned once. A filter-first text search with no snapshot to write keeps only `offset + limit` candidates.

- **Postgres element quantifiers read their array once.** `$any`, `$all` and `$none` name the array column a single time, so a view column computed by a correlated subquery runs once per row, not once per reference. `$any` on a JSON value that is not an array no longer fails with `cannot get array length of a scalar`.

### Fixed

- **Postgres compares a boolean or a number in an untyped JSON array.** A quantifier field the read model leaves untyped (`list[dict]`) compares it as JSON instead of failing on `text = boolean`, and refuses a `Decimal`, date or UUID operand; `nested_field_hints` types such a field by its full path (`offers.price`).

- **Postgres element quantifiers agree with the in-memory evaluator on missing keys and non-object elements.** `$all` now fails an element whose predicate is NULL (a missing field, a null element), and `$neq`, `$nin` or a nested `$all` / `$none` no longer match a scalar or null element.

- On an untyped JSON array element, a `None` member of `$in` / `$nin` matches a stored JSON null and an integer past int64 compares instead of failing; a NaN or infinite float is refused with `query_feature_unsupported`.

- **Postgres filters a scalar array stored as JSON.** A quantifier on a `jsonb` column holding `["a", "b"]` compares each element as JSON by the operand's type instead of failing with invalid json or `jsonb = smallint`. A `Decimal` array no longer fails on a non-numeric element, and a datetime operand is refused.

- **Postgres native arrays treat a NULL element as the in-memory evaluator does.** A `None` member of an element `$in` now matches it, and one of `$nin` now excludes it.

- A Postgres `undefined_function` error (SQLSTATE 42883) now says no function or operator matches the argument types, not that the statement names an object that does not exist. It keeps the `postgres_schema_mismatch` code.

### Security

- **A validation error no longer echoes the caller's input**: FastAPI request validation, assembled route payloads and dynamic-read row checks scrub validator messages, and an invalid Inngest event no longer sends its payload to the platform. A filter refusal still names the bad key, as type `query_expression_invalid`.

## [0.16.0] - 2026-10-22

### Added

- **`CoreException.expose_details`** withholds an error's details from the client envelope, whatever its kind.

- `CircuitBreakerStrategy` takes `failure_on` and `ignore_on`: which error kinds count as breaker failures and which are not recorded. The defaults keep the previous classification: infrastructure and timeout fail, throttled and concurrency are ignored.

- **A durable run can retry a retryable failure.** `retry=RetryStrategy(...)` on `DurableFunctionRunner`, `durable_kits_deps` or one registered function sends a run raising a `retry_on` kind back to `PENDING` after the backoff, counted as `retrying`; its journal replays. **Breaking** for own run stores (`mark_retry`).

- **Failed durable runs can be requeued.** `DurableRunAdminPort.requeue_failed(run_ids=, name=)` returns the named `FAILED` and `TIMED_OUT` runs in scope to `PENDING` under their own id, so their journaled steps replay, on Postgres, Mongo and the mock. **Breaking** for own admin ports.

- **A warm API key skips the eligibility read too.** With revocation states on the authn and authz kernels, a key whose cached state recorded its principal's current epoch is admitted without reading Postgres, as a token is. New `ApiKeyEligibilitySkip`.

- **Vector search can rank by a vector you already hold.** `SearchOptions(query_vector=...)` ranks pgvector or Atlas Vector Search with a blank query; other searches refuse it. A custom vector adapter passes `capabilities=`, `backend=` and `vector_dimensions=` to `search_options_for_simple_adapter` to accept it.

- **Durable schema reference.** A new page lists the Postgres tables and Mongo collections the durable stores use, with their required and recommended indexes and the migrations between releases. The concurrency page now states that a stale-revision precondition is not retried for you.

- **Hub searches aggregate.** `aggregate_search` / `aggregate_search_page` on a Postgres or in-memory hub group every merged match of its members, legs uncapped. New `HubSearchSpec.extra_aggregatable_fields` names hub-relation columns outside the read model that an aggregate may also group by or measure.

- **`upsert_many` can mirror a source and keep values a null would erase.** `UpsertItem.mirror(id, payload)` updates with the create payload; `skip_none=True` leaves out each top-level field an update sets to `None`. A set-based upsert is now judged on the fields the items' updates write.

- **`IssuedApiKey.secret`** returns the raw key of a freshly issued API key. It stays out of `repr()` and `str()`.

- **`attach_authn_routes` no longer needs `ns=`.** Without `ns=` or `resource=` it uses the one namespace in the registry holding its `password_login` or `refresh_tokens` operation, `spec.default_namespace` for `build_authn_registry(spec)`, and refuses to guess.

- **Overlapping refreshes need not revoke a session.** `RefreshTokenConfig(reuse_grace=...)` refuses a token rotated within the grace, its successor unused, or one losing a concurrent rotation, as `refresh_superseded` (event `REFRESH_SUPERSEDED`), revoking nothing. The default `0` keeps strict reuse detection.

### Changed

- **forze prints nothing to stdout until structlog is configured** (**behaviour change**): until `configure_logging` or your own `structlog.configure`, its events go to the same-named stdlib logger at its level, reaching only stdlib handlers, or stderr from warnings up by default. `configure_logging` keeps info output.

- **A Temporal schedule describes and lists with its cron expressions as written** (**behaviour change**), not the equivalent ones rebuilt from its calendars. Each is kept as its calendar's comment (`forze:cron=…`); schedules from earlier versions or rewritten outside forze still report the rebuilt cron.

- **`UpsertItem.update` is now derived from the update passed and `skip_none`.** Constructors are unchanged; `attrs.asdict` output and `__match_args__` differ. An override of `DocumentSpec.require_set_based_upsert` must accept the new `updates` argument.

- **`anonymous_paths` entries may be relative to the mount.** With `SecurityContextMiddleware` on an application mounted under a prefix or served with a `root_path`, `/auth/login` now matches `/v1/auth/login`; full paths keep working.

- **A set-based `upsert_many` on Postgres takes less time**: about 13% for calls of a few dozen rows and 30% for calls of thousands, on a fresh load or a reload. The rows written are unchanged.

### Fixed

- **A `CoreException` can be copied and pickled.** `copy.copy`, `copy.deepcopy` and a `pickle` round-trip raised `TypeError`; they now keep every field, a subclass's own fields, `expose_details` set after construction, and the notes.

- **Firestore raises a stale write's preconditions as Postgres and Mongo do** (**behaviour change**). A future revision is `precondition` with `revision_mismatch`, and a revision whose history snapshot is missing is `precondition` with `history_not_found_retry`, not `not_found`.

- **A refused Socket.IO connect renders its error as an error ack does** (**behaviour change**). Its data is now `{"detail", "code", "kind"}`, with `context` when exposed, and a non-disclosing denial posture applies: a denial or not-found about a covered resource type is refused as `Not found`.

- **A failed Firestore history query is raised, not skipped.** A `NotFound` from the API, such as for a missing database, was taken for a missing snapshot, so `read_many` returned fewer snapshots or none.

- **A set-based `upsert_many` is refused for a domain with serializers of its own** (**behaviour change**), as for validators: a `field_serializer`, `model_serializer`, `PlainSerializer` or `WrapSerializer` that runs in python mode now raises `set_based_upsert_unsupported`.

- **The mock hub search sorts and projects as the Postgres hub does** (**behaviour change**). It merges its legs' relevance, not their row positions, so ties follow the sort or `default_sort`; `project_search` and `project_search_page` work. Mock hub and federated streams refuse with `query_feature_unsupported`.

- A circuit breaker whose half-open probes recorded no outcome (an ignored error kind, a cancelled call) no longer rejects every call until restart: after `break_duration` it admits a new probe round.

- **A logout everywhere followed by a deactivation in one transaction no longer deadlocks with a concurrent login** (revocation states, policy principal on Postgres; deactivation through SQL or its document port).

- **`PostgresConfig` session options no longer empty the pool.** With `application_name`, `statement_timeout`, `lock_timeout` or `idle_in_transaction_session_timeout` set, every new connection was discarded, and queries failed as a busy pool. They now take effect on every pooled connection.

- **Postgres schema and programming errors are no longer retried** (**behaviour change**). A missing object (`postgres_schema_mismatch`), a refused privilege (`postgres_privilege_denied`), a syntax error or a PL/pgSQL `RAISE EXCEPTION` is now `internal`, not retryable `infrastructure`.

- **Upstream throttles are `throttled`, rejections `internal`** (**behaviour change**). `forze_http`: 429 `throttled` (`retry_after` in `details`), 401/403 `infrastructure` (now retried), 3xx, other 4xx, bad bodies `internal`. GCS, BigQuery, ClickHouse, S3, SQS, KMS throttles too; `transient` retries none.

- **A bulk update or upsert mixing patch types no longer drops fields.** Each patch in an `update_many` or `upsert_many` was encoded as the first one's type, so the fields a subclass or other type adds were silently not written, on Postgres and Mongo.

- **A document update keeps the nulls inside a mapping or `Any` value** (**behaviour change**). A `None` there is stored as null, as create stores it, or refused where the value type admits no null. An update can no longer remove a key from a mapping.

- **Resending an unchanged instant no longer bumps `rev`.** A document `update`, `upsert` or `upsert_many` compares a naive `datetime` with an aware one as UTC, either way round; on Mongo, finer than a millisecond is no change either.

- **A stale-revision update no longer reverts a concurrent change** (**behaviour change**). With revision history on, a field it resends with the value it read is not written; only what it changes lands. New `Document.historical_update`.

- **`update_many` with revision history no longer fails on a row it leaves unchanged.** Postgres raised a unique violation and rolled the batch back; Mongo wrote a duplicate snapshot.

- **Cookie-mode `/refresh` accepts an empty JSON object.** A `{}` body now reads the refresh token from its cookie, as a body-less request does, instead of answering 422. A token in the body still takes precedence.

- **The Socket.IO rebalance step sheds fewer connections during a connect storm**: a replica's load is now the lower of its live count and its own last report, so connections since that report are not counted against its peers' reports.

- **A hub's `search_count="approximate"` total counts its search text** (**behaviour change**): with a query it is the merged rows the page reads instead of the cap: exact under the cap, the cap above it, zero for no match, a lower bound once a leg is cut at `per_leg_limit`.

- **A filtered Postgres search no longer fails with `postgres_schema_mismatch`** under `search_count="approximate"` or a PGroonga `plan="auto"`.

- **A Postgres hub page with `limit: 0` reports its total** instead of 0.

- **A missing or closed Temporal workflow, or a missing schedule, is `not_found`; a taken workflow id `conflict`** (**behaviour change**), not `RPCError` or `WorkflowAlreadyStartedError`. A taken schedule kept `conflict`, now coded. New `WORKFLOW_NOT_FOUND_CODE` etc.; the mock raises them and refuses a taken schedule.

- **`FleetView.excess` leaves a negative load out of the mean** (**behaviour change**), as it does a non-numeric one: a load is a count. A negative peer load no longer inflates a member's excess, and a member whose own load is negative gets 0.

- **A retry waits out a throttle's `retry_after`** (**behaviour change**). Resilience policies, durable runs and durable saga steps retry a `throttled` error no sooner than its `details["retry_after"]`; a policy or saga whose longest backoff is shorter gives up at once.

- **Mock workflows and schedules answer as Temporal does** (**behaviour change**): cancel or terminate of a missing run, terminate of a closed one, and delete, pause or unpause of a missing schedule are `not_found`. A start conflicts only with a running run, which `raise_on_already_started=False` keeps.

- **A mock workflow `start` returns an unpinned handle** (**behaviour change**): its `run_id` is `None`, as Temporal's, so it follows the workflow id's latest run. Once the run is replaced, a signal, cancel or terminate through it reaches the new run instead of `not_found`.

- **The isolation battery's predicate cases no longer count another case's rows on a reused store**, so `forze_dst.conformance.collect_verdicts` over one store no longer reports a false `predicate_write_skew` divergence.

### Security

- **An error an integration translates from its backend no longer shows the failed call's context to API callers** (**behaviour change**): such an error carries no `context`, whatever its kind. The details stay on the exception and are logged at debug. A stream's failure between items is now translated too.

- **A cookie read by several `CookieTokenAuthn` ingresses keeps every CSRF gate they declare**: one ingress's `csrf=None` no longer lets the cookie through ungated.

- **`saga.step_failed` no longer shows the failed step's error text to API callers** (**behaviour change**): its `cause` detail is now the cause's `code` and `kind`, and only when the cause is a `CoreException`. The original error stays chained as `__cause__`.

- **An error translated from a backend keeps a fixed summary** (**behaviour change**): the backend's message moves to `details["error"]` in the Neo4j, Mongo, Firestore, Vault, inference, Meilisearch, GCS and container-sandbox integrations, secret resolution and credential rotation, so `str()` no longer includes it.

- **A container sandbox error summary no longer names the daemon host** (**behaviour change**): the host, which can carry credentials, stays in `details["docker_host"]`.

- **Cookie-mode `/login`, `/logout` and `/refresh` apply the cookie CSRF gate to every request** (**behaviour change**), with or without a cookie and wherever the refresh token rides, under the access cookie's `CookieTokenAuthn(csrf=...)`. A client sending neither `Origin` nor `Referer` needs `allow_missing_origin=True`.

- **Cookie-mode logout revokes from the refresh cookie without a live access cookie** (`logout_by_refresh_token`). `runtime_lifespan` refuses an app whose refresh cookie path misses logout (`check_authn_cookie_paths`). **Breaking** for own token lifecycle ports: add `revoke_tokens_by_refresh_token`.

## [0.15.1] - 2026-10-19

### Fixed

- **A leadership body resumed after a pause stops at its next `Term.live()` check.** `live()` now ends the term once its soft deadline has passed on the clock, instead of letting the body run one more unit past it. Since it may now end the term, call it on the step's event loop, not from a worker thread.

- **The Socket.IO expiry sweep and presence heartbeat skip a socket already on its way out**, instead of logging a server error for it. A connection the drain closes while another close is under way no longer leaves its presence entry behind, and a presence store failing a leave no longer strands the connection.

## [0.15.0] - 2026-10-18

### Added

- **The Postgres outbox can log what it consumes.** `PostgresOutboxConfig(storage="consume", delivery_log=("app", "outbox_delivered"))` appends each published row to an application-owned table in the statement that deletes it, so a log row proves a committed delivery. A failure to write it fails the pass.

- **Token verifications can skip the eligibility read too.** Pass the authn kernel's `RevocationStates` to `AuthzKernelConfig(revocation_states=)`: a deactivating policy-principal write then raises the epoch in its transaction, and a fast-path token skips the read. Postgres only; needs `key_mac`.

- **`DocumentCommandHookDepKey` wraps a document's command port wherever it is resolved**, registered routed under the document's name.

- **API keys can be verified from Redis, and cached states signed.** `RevocationStates(key_mac=StateMacKeys(...))` checks a key from a MAC-signed state, reading its row only on a miss, and signs cached epochs and session states. A custom states store then needs `ApiKeyStatesPort` and `raise_epoch(state=)`.

- **An API key admitted from Redis cannot mint past its revocation.** With `key_mac` set, any credential minted in a request or socket command an API key authenticated re-reads that key's row first, whoever it is for, and fails with `api_key_revoked` once the key is revoked or expired.

- **The revocation-states fast path is counted.** `instrument_revocation_states({"default": states})` exports hits, misses, fallbacks, failed write-throughs and clock-skew refusals, for sessions and API keys.

### Fixed

- **Mongo searches and counters, tenant-aware Mongo outboxes and Firestore counters on a per-tenant collection now meet a `namespace` floor.** A Mongo search's index name, scoped to its collection, no longer counts.

- **Mongo no longer advises a tenant-aware outbox on a per-tenant collection to drop `tenant_aware`**, which would put every tenant's records in one collection.

### Security

- **A route is `namespace`-isolated only when every relation it names resolves per tenant** (**Breaking**): a `namespace` floor refuses one with a static relation, and a per-tenant Postgres inbox, idempotency store or outbox that is not tenant-aware. New: `TenancyRouteGroup(other_namespaces=)`, `resolves_per_tenant`.

- **Postgres idempotency stores and federated searches are held to a declared floor** (**Breaking**): an idempotency store on a shared table rates `tagged`, and a federated search rates `none` unless every leg is a hub leg, tenant-aware or per-tenant.

- **`ensure_principal(..., is_active=False)` creates the principal inactive in one write**, so a failure can no longer leave it active; `CreatePolicyPrincipalCmd` gains `is_active` (default `True`).

- **With revocation states, a revocation or a deactivation in a transaction that does not hold all of its writes (the epoch's, the credentials', the principal's) is refused before it writes** (`authn_revocation_requires_transaction`), not committed part-way; Postgres stores report `is_transactionally_enlisted()`.

- **A credential minted by a WebSocket or Socket.IO command re-checks the token that authenticated the connection.** Both transports carry what the handshake or last reauth recorded (`current_authn_admission`) to each command; once that session is revoked, or rotated under `"session"` binding, mints fail until a reauth.

- **Deactivating a principal deactivates every API key it holds.** The keys were read with no limit, so past a store's implicit read limit (10,000 rows on Postgres, Mongo and Firestore) the rest stayed active; deactivation now pages through all of them.

- **With revocation states, a credential minted from an access token re-checks that token whoever it is for**, including a password account or invite an operator issues for another principal.

- **A tenant-aware Postgres inbox keeps tenants apart on a shared table.** It carries the tenant in its dedup key, as the Mongo inbox does. After upgrading, a message redelivered to a tenant-aware Postgres inbox is processed once more.

## [0.14.0] - 2026-10-17

### Added

- **A replica can withdraw readiness before refusing work, and drain while it still serves.** `runtime.begin_draining()` fails readiness, operations still admitted; `runtime.drain(budget, announce_delay=)` runs the phases steps put on `ctx.drain_phases`. `shutdown()` drains first if nobody did, within `drain_budget`.

- **An embedded uvicorn drains the runtime on `SIGTERM` before it stops.** `forze_fastapi.serve_uvicorn(config, runtime, drain_budget=)` runs `runtime.drain()` on the first `SIGTERM` or `SIGINT`; a `SIGINT` during it forces the exit. A finite graceful timeout is set when none is, so an open stream cannot hold the exit.

- **A leaving replica closes its socket.io connections spread over a window, and turns new ones away.** `connection_drain_lifecycle_step(sio, window=, tail=)` closes them with `close_spread` in a drain phase, before the server stops; `attach_realtime_connection(draining=)` closes the transport of connects meanwhile.

- **FastAPI's SSE and WebSocket live tails drain the same way.** `realtime_sse_drain_lifecycle_step(hub, window=, tail=)` ends them spread over a window; a new stream is told to retry, a new upgrade gets a 1012 close. `spread_drain_lifecycle_step` is the phase for connections of your own.

- **Apps can admit or delay socket.io connects, and connections are counted.** `attach_realtime_connection(admission=)` runs before authentication; `instrument_realtime_connections(RealtimeConnectionStats)` exports open connections, refused connects by reason, and drain closes.

- **The worker probe listener can check dependencies, and needs no runtime.** `probe_listener_step(checks=, check_timeout=)` answers through `evaluate_readiness`, the evaluator `attach_readiness_route` now uses, so both answer alike from the same probes. Steps read `ctx.lifecycle.started` and `.announced`.

- **A fenced writer can recover after Redis loses its counters.** `FenceAdvancing.advance_fence(key, past=...)`, on Redis and the mock, lifts a fencing counter to the store's highest fence; `requires_fence_advance` fails closed without it. The documented rule now claims a token first, so a repeated one is acted on once.

- **Work can run on one replica at a time for the life of the process.** `leadership_lifecycle_step` elects a leader on a renewing lease, ends its term inside the lease, hands it over on stop or on request, and fences it: a token, a claim, and `fence_floor` to recover after Redis loses its counters.

- **Replicas can see which peers are alive, and split work over them.** `ctx.fleet(FleetSpec(...))`, on Redis via `RedisDepsModule(fleets=...)`, gives a `FleetPort` whose `beat` writes a replica's state and report and returns the `FleetView`; its policies agree for all who hold it. `instance_id()` names a process.

- **A replica keeps its fleet entry current from startup to shutdown.** `fleet_membership_lifecycle_step(spec, every=)` beats `starting`, then `serving` once startup finishes, then `draining` once a drain is announced, and leaves the fleet or lingers on stop.

- **An overloaded socket.io replica sheds connections toward the fleet's mean.** `connection_rebalance_lifecycle_step(sio, fleet=, every=, movable=, per_minute=)` closes movable connections at a bounded rate once the fleet has settled; `connection_report(sio, movable=)` fills the membership report it reads.

- **The Postgres outbox can delete what it relays.** `PostgresOutboxConfig(storage="consume")` runs a relay pass in one transaction bounded by `pass_deadline` and `pass_statement_timeout`: the claim locks rows, publishing deletes them, nothing is left to prune. A failed pass backs its route, or tenant, off.

- **A relay can bound each publish, and a run.** `publish_timeout=` on `OutboxRelay` and the background step makes a slow publish a transient failure; `OutboxRelay(publish_until=)` ends a run's publishing. A store opts into transactional passes with `TransactionalOutboxPasses`; `outbox_pass_scope()` enters one.

- **The delivery conformance battery covers a relay pass that rolls back.** `forze_dst.conformance.run_rollback_crash_delivery` runs on the mock, on Mongo, and on Postgres in both storage modes.

- **Tokens can be verified from cached revocation states, not the session row.** `AuthnKernelConfig(revocation_states=RevocationStates(...))` checks new `fam` and `pep` claims against `RedisDepsModule(revocation_states=)` and `PostgresDepsModule(principal_epochs=)`. Opt-in; issue and revocation then need a transaction.

- **Durable runs recovery gave up decrypting can be requeued.** `DurableRunStatus.QUARANTINED` marks a run the scan no longer claims; `DurableRunAdminPort.requeue_quarantined(run_ids=)` returns a bound tenant's or named ones, on Postgres, Mongo and the mock. **Breaking** for exhaustive status matches and own admin ports.

### Changed

- **A durable mock snapshot written by an earlier release is refused on load** (`mock_state_snapshot_incompatible`), since the mock state gained a store.

- **The `socketio` extra bounds `python-engineio` below 5.** The connection drain and a draining replica's turn-away close connections through its internals, and refuse at assembly a server that lacks them.

- **Readiness waits for startup, and `runtime.draining` turns true when a drain is announced.** `runtime.ready`, the readiness route and the probe listener answer `503` until startup finishes; `ctx.drain_gate.draining` alone means refused. **Breaking** for a startup probe on `/readyz` sized for the earlier `200`.

- **The readiness route logs a failed dependency check under the application logger**, as "Readiness check … failed". Alerts keyed on the `fastapi.probes` logger no longer match.

- **The tenant-isolation floor leaves out routes that belong to the deployment.** Redis fleets, and lock and pub/sub routes marked `tenant_global=True` (a lifecycle lock, an outbox wake), no longer count toward `required_tenant_isolation`, so a module of only those meets it. Every other route is still held to it.

- **The outbox relay step resolves its outbox route at startup even with `tenants=`.** A route that is not wired fails startup instead of every tick.

- **A durable run whose key is gone no longer resumes by itself once its recovery hold reaches a day.** Quarantined, it waits for a requeue even after the key is back; one already claimed nine times, crash reclaims included, is parked on its first failed open. **Breaking** for deployments relying on a restored key.

### Deprecated

- **`probe_listener_step(runtime=...)`.** The step reads its scope from its hook's `ctx`, so it can sit in its own runtime's plan; a runtime passed in is still watched, with a `DeprecationWarning`.

### Fixed

- **The crash-recovery delivery scenario marks the rows it re-publishes.** `forze_dst.conformance.run_crash_recovery_delivery` marked them by event id, which matched no row, so they stayed `processing`; the outcome never read the mark.

- **Recovery no longer lands a saga asked to stop unread when its holder died first.** It landed `CANCELLED`, abandoning forward steps past the pivot and skipping the rollback before it. Such a saga now runs again and decides by its journal at its next step, as a live one does.

- **Durable recovery no longer stalls on runs it cannot decrypt.** On Postgres and Mongo, a claimed run whose tenant's key was gone failed the whole claim. Now only that run is skipped and held out of the scan, from two leases doubling to a day; a passing KMS fault keeps the lease. A missing keyring still fails the scan.

- **A durable run whose key is gone no longer fails the run listing.** On Postgres and Mongo, `list_runs` returns it without its payloads, with the new `DurableRunRecord.payloads_unreadable` set; a cancel asked of it is decided once it can be decrypted. A passing KMS fault or a missing keyring still fails the listing.

- **A realtime stream whose client disconnects mid-wait now leaves its presence rooms.** An SSE stream or WebSocket tail cancelled while waiting kept its member counted in a shared presence store such as Redis until the TTL; the leave now runs shielded, bounded by 2 s.

- **A readiness check whose client is registered only per route says so**, as `registered per route only`, instead of reporting it `not registered`.

- **A retry past its 1,024th attempt no longer fails with `OverflowError`.** Resilience retries and outbox redelivery keep their backoff at the cap however many attempts came before, and a zero-delay saga step or client read keeps retrying.

- **Ensuring a Redis consumer group that already exists no longer fails**, so the realtime group-ensure step survives a restart. A `BUSYGROUP` reply now maps to a conflict coded `redis_busygroup` rather than a generic busy infrastructure error.

- **A `SERVERLESS` runtime now refuses the durable recovery and scheduler steps** at assembly, as it does the realtime sweeps: each runs a background loop a frozen function cannot host. **Breaking** for serverless apps that assembled them.

- **A lock spec with `requires_fencing_token` no longer refuses a capable backend whose ports are wrapped** by OTel port spans, tracing, interceptors or a resilience port policy; resolving it failed with `dlock.fencing_unsupported`.

- **A lock spec's fencing requirements are judged against the backend it hands out.** With the port cache off, the check resolved a second backend and judged that one instead. `implements(port, Protocol)` in `forze.application.contracts.deps` asks a protocol check of the backend behind a port's guard and wrappers.

- **An inbox on another pool than its transaction is refused under port logging or OTel port spans too.** The exactly-once check passed any wrapped inbox, so its dedup mark could commit apart from the handler's writes.

- **A stream spec with `requires_transactions` no longer refuses an exactly-once backend whose port is wrapped** by OTel port spans, port logging, tracing or a resilience port policy; resolving it failed with `stream.transactions_unsupported`.

- **Exporting or migrating a graph no longer refuses a backend whose query port is wrapped** by OTel port spans, port logging, tracing or a resilience port policy, as one that cannot surface edge endpoints.

- **A document's L1 cache subscribes to invalidation push when its cache port is wrapped** by OTel port spans, port logging, tracing or a resilience port policy, instead of falling back silently to TTL-only staleness.

- **`graph_read_capabilities` reports the streams of a wrapped graph port.** Under OTel port spans, port logging, tracing or a resilience port policy it reported none, and a stream guarded by it was refused.

- **`capabilities_of` and `durable_run_control_capabilities` read the backend behind a wrapped port**, as the other capability checks now do, instead of reporting no storage guarantees and no run control.

- **A Redis lock acquire that fails after taking the lock no longer leaves it held**, as on a corrupt fence counter. Tokens stop at 2⁵³ − 1 on Redis and the mock: an acquire past it is refused with `dlock.fence_exhausted` rather than issuing a token Redis would round onto another.

- **Socket.io connects replaying at the same time all get their offline replay.** Replay and `realtime.ack` run on the live context of `attach_realtime_connection(runtime=)` rather than opening a scope per connect. **Breaking:** pass the serving runtime, with its scope open.

- **The documented fenced write no longer refuses the lock holder's own second write.** The Redis lock example now accepts an equal token on a holder's later writes, the same lock generation, as the contract does.

- **The singleton guard and leadership refuse, at startup, a lock no tenant-less call can use**: tenant-aware, or on a routed client with no tenant to route by. It failed startup with `tenant_required` or never led; now `tenant_global_required` (new `require_tenant_global`). **Breaking** for such a leadership step.

- **Background steps run without `tenants` refuse a tenant-scoped port at startup:** the outbox and realtime relays, durable recovery and scheduler, `job_staleness`, stream trim and mailbox retention, which failed every tick, and group ensure. So does a relay whose wake is on a routed client. **Breaking** for such apps.

- **The outbox relay and the durable recovery and scheduler loops stop on a wiring error, logged once,** such as no keyring wired, instead of logging it every tick. One tenant's own configuration error, such as its key's state, still leaves the rest of the shard running.

- **The Temporal schedule bootstrap, the S3 bucket step and Mongo index validation refuse a tenant-scoped client or schedule at startup**, as `tenant_global_required`, instead of failing with `tenant_required`: they run with no tenant bound.

- **`permission_providers_lifecycle_step` without `tenant=` refuses a tenant-scoped permission catalog at startup**, as `tenant_global_required`, instead of failing with `tenant_required`.

- **A tenant-aware Kafka route refuses a call with no tenant bound**, with `tenant_required` as other tenant-aware adapters do, instead of resolving its namespace for no tenant. The commit-stream consumer refuses such a group at startup. **Breaking** for apps that used one unbound.

- **The queue and notification consumer steps refuse a tenant-scoped queue at startup**, as `tenant_global_required`: they receive with no tenant bound, so they failed every poll. **Breaking** for apps that ran one so.

- **The plain SSE tail and the socket.io gateway's stream-group and pub/sub sources refuse a tenant-scoped port at startup**, as `tenant_global_required`: they read with no tenant bound, so every read failed. The sharded tail and source bind their tenants. **Breaking** for apps that ran one so.

- **A readiness check on a client scoped per tenant says so**, as `routed per tenant` or `tenant-aware`, instead of a bare `CoreException`: a probe has no tenant bound.

- **Realtime presence refuses a store on a client routed per tenant**, as `tenant_global_required`: `attach_realtime_connection`, the gateway step and both heartbeat steps use it with no tenant bound, so connects, live counts and refreshes failed. **Breaking** for apps wired so.

### Security

- **A socket.io connection acts only under the identity it authenticated.** Upgrade if `attach_realtime_connection` or `ForzeSocketIOAdapter(identity_resolver=)` authenticates your socket.io connections.

- **A tenant-aware Kafka route with a fixed namespace no longer counts as tenant-isolated**; a declared isolation floor now refuses it. Kafka resolves a per-tenant namespace as the other backends do. **Breaking** for such a floored module, and a per-tenant route that is not tenant-aware may name new topics.

## [0.13.0] - 2026-10-14

### Added

- **A socket.io client opening several namespaces can authenticate once.** `attach_realtime_connection(share_identity=True)` reuses the identity one Engine.IO connection resolved for its other namespaces with the same credential and resolver, until it expires or the connection closes; another credential is checked again.

- **An outbox relay can deliver a row as soon as it commits.** `OutboxSpec(wake=OutboxWake(route=...))` publishes a wake on a pub/sub route after each flush's transaction commits, and the background relay runs a pass when it hears one; its poll stays the fallback for a lost wake.

- **The outbox relay reclaims stale claims on its own cadence.** The reclaim runs at most once per `reclaim_every`, by default once per poll tick, rather than on every pass a wake starts.

- **A Redis call can wait for a free connection instead of failing.** `RedisConfig(pool_timeout=...)`, also on `RedisSettings`, queues a call for up to that long once all `max_size` connections are in use; a retried read waits again. When unset, a full pool still refuses the call at once, as it does past the wait.

### Changed

- **An emit to a large socket.io room takes far less of the event loop.** The manager `build_socketio_server` builds no longer schedules a task for every socket in the room, only for a send that has to wait. Pass `client_manager=AsyncManager()` to keep python-socketio's manager.

- **A reused refresh token raises `RefreshTokenReuseError`**, still the uniform refusal; refresh commits the family's revocation after rolling back, so it refuses a transaction its caller opened. **Breaking** for those callers and for token lifecycles, which must raise it on reuse and implement `revoke_chain_of_tokens`.

### Fixed

- **A failed deactivation, password change or reset no longer half-applies.** Login, refresh, logout, password change and reset, and deactivation commit whole on `build_authn_registry(tx_route=)`, the tenant switch on `build_tenancy_registry`'s. **Breaking**: each needs a transaction manager, `"default"` unless set.

- **On Postgres, a login racing a deactivation can no longer leave the principal a live session.** Login, refresh and the tenant switch read the principal with a row lock until the session commits. **Breaking** for eligibility ports: `require_authentication_allowed` takes `for_update`.

- **Logout, deactivation and a password change or reset revoke every live session, however many.** Revocation stopped at the store's implicit 10,000-row cap, or failed when a refresh rotated a session mid-way; it now revokes in batches, rereading a stale one.

- **The mock's principal deactivation revokes the principal's sessions**, emitting `LOGOUT` before `PRINCIPAL_DEACTIVATED`, as the real one does.

## [0.12.0] - 2026-10-13

### Added

- **OpenAPI can list the response fields an app always sends as required.** `apply_openapi_conventions(app, require_sent_fields=True)` requires every field of a schema only null-keeping routes return, and only the non-nullable ones where a route drops nulls. Schemas requests also use are left as they are.

- **A cross-origin browser can read forze's response headers.** `AllowlistCORSMiddleware` is Starlette's CORS middleware answering an `OriginAllowlist` (loopback port ranges included) and exposing `EXPOSED_RESPONSE_HEADERS`: `X-Error-Code`, the invocation ids, and a download's filename, `ETag` and range headers.

- **An administrator can revoke any principal's API key.** `build_authn_registry(admin_guards=...)` adds `revoke_principal_api_key` (`DELETE /admin/api-keys/{id}`) behind those guards, like `deactivate_principal`; unguarded, neither is registered. Both act in every tenant. **Breaking** for API-key lifecycle implementers.

- **An administrator can list any principal's API keys**, to find one to revoke. `list_principal_api_keys` (`GET /admin/principals/{id}/api-keys`) returns the non-secret descriptors, revoked keys included, behind `admin_guards` only, and in every tenant. **Breaking** for API-key lifecycle implementers.

- **`admin_guards` must include a step that declares its permission keys.** Unless one implements `permission_keys()`, as `AuthzBeforeAuthorize` does, `build_authn_registry` refuses them; `trust_admin_guards=True` skips the check.

- **A revoked API key emits an `api_key_revoked` authn event**, from self-service and admin revocation alike, with the key id and who revoked it. The principal-deactivation cascade, which also revokes keys, does not emit it.

- **A hub search can choose how an unasked page counts its total.** `HubSearchSpec(default_search_count="approximate")` (or `"none"`) applies when a request sets no `search_count`, as it does on a `SearchSpec`. The request option still wins and the members' own defaults don't apply; Postgres honours it.

- **A Temporal worker can deliver a cancel to a heartbeating activity within a second.** `temporal_worker_lifecycle_step(max_heartbeat_throttle_interval=...)` caps how long the SDK holds back heartbeats, and a cancel arrives only in a heartbeat's reply; uncapped it waited up to 0.8 × the `heartbeat_timeout`.

- **A search can group and measure the rows it matches.** `aggregate_search` and `aggregate_search_page` take the `AggregatesExpression` the document port's `aggregate_many` does and read every match, uncapped. Postgres FTS, PGroonga and the mock serve it; the others refuse via `SearchCapabilities.supports_aggregates`.

- **A bulk upsert can write set-based.** `upsert_many(items, return_new=False, set_based=True)` inserts the missing rows and patches the stored ones, only where the update changes them, reading nothing back; a Postgres reload of unchanged rows ran about 4× faster. A spec whose updates need the domain model refuses it.

- **An MCP server can expose a chosen set of tools, and smaller ones.** `register_tools` and `build_mcp_server` take `operations=[...]`, an allowlist refusing an unknown name; `output_schemas=False`; and `shared_filter_grammar=True`, stating the filter grammar once in the instructions, which `build_mcp_server` now takes.

### Changed

- **A listed API key always carries `is_active` and `created_at`** (**Breaking** for API-key lifecycle implementers). `AuthnApiKeyListItemDTO` requires them, so generated clients type them as present; `hint` stays optional, as a key minted before hints has none. `ApiKeyInfo.created_at` is required.

- **`/deactivate` answers 404 after upgrading: pass `admin_guards`** (**Breaking**). `deactivate_principal` is registered only behind them, so guards an app bound on the operation itself leave it absent; pass them as `admin_guards` instead (bound twice, a step fails at `freeze()`).

- **`AuthnKernelOp` has members a default registry does not register** (**upgrade note**). An `include=` built from the whole enum fails at startup without `admin_guards`; with them, MCP's default read-only tools gain `list_principal_api_keys`.

- **A query operation may hold a write port it never uses** (**behaviour change**). A handler built with a command port failed to build under `as_query()`; it now gets a stand-in that refuses when used, so a read service carrying write ports can be a query. A write in a query is still refused.

- **A forze model's `Decimal` graph key and stored text are fixed point** (**behaviour change**). A vertex keyed earlier by a `Decimal` that wrote in scientific form (`"1E+3"`) keeps that key; portability archive digests differ for such values.

### Fixed

- **A federated search over members named by a `StrEnum` runs, in Postgres and Meilisearch.** Each search keys its snapshot by its members' default sorts, and a `StrEnum` name as that key failed it with a 500. A fingerprint now hashes a `StrEnum` key as its value; any other non-string key is still refused.

- **Revoking an API key the caller does not hold is a 404** (**behaviour change**). `revoke_api_key` answered a malformed, unknown or another principal's key id with a 401, which a browser client reads as its session ending; it is now `not_found` (`api_key_not_found`). A revoked key still answers 204.

- **An access token is checked on the clock that issued it.** `issue_token` stamps `iat` and `exp` from forze's time source, but PyJWT checked them on the wall clock, so under `FrozenTimeSource` every token was expired or not yet valid. `exp`, `iat` and `nbf` are now checked against `utcnow()`, with the leeway.

- **An operation can declare an output that may be nothing.** `OperationDescriptor(output_type=Model | None)` (or `list[Model]`) is served as `200 null` over HTTP and `{"result": null}` over MCP; its catalog schema and fingerprint no longer fail. A model's schema, and so its fingerprint, is unchanged.

- **A read-only operation cannot write through a port a command built and cached** (**behaviour change**). A query reusing a handler or adapter that a command built first held its real write port. A command accessor now always returns `GuardedWritePort`; in a test, `forze.testing.unguarded` reaches the port.

- **A filter, sort or aggregates expression refuses a key it does not define, naming it** (**behaviour change**). A request DTO dropped it, so `{"$values": {…}, "$bogus": 1}` returned a wider page and `{"$bogus": …}` failed as `{}`; the port ignored one beside `$values`, a second combinator, or a mistyped `$having`.

- **An unknown option is refused, naming it** (**behaviour change**). A search request refuses one in `options`, highlight or `snapshot` options, or `result_snapshot` (a 422), as search, analytics, dynamic-read and inference ports now do. A single-index request refuses `members` / `member_weights`; its port only warns.

- **A request filter can nest an element quantifier, as the port can.** A quantifier inside another, or under an object element's `$values`, failed a request DTO or tool schema with a list of union errors; the filter types now express it. The nesting counts toward `max_depth`.

- **A filter's `max_depth` bounds how deep its `$and` / `$or` / `$not` nest, not how many it has** (**behaviour change**). Every combinator counted toward it, so 40 `$not` side by side under one `$and` were refused as deeper than 32. Siblings now each sit one level down; `max_clauses` still bounds the width.

- **A Temporal schedule created from cron expressions can be described and listed.** The server keeps each expression only as a compiled calendar, so the timing came back empty; it now reads as an equivalent cron expression. A timing it cannot state exactly, such as one skipping periods, is refused; a listing skips it.

- **A blank PGroonga search decrypts encrypted fields and refuses a sort on them, as a ranked search does.** A projection or `select_search` returned the ciphertext, and a sort on an encrypted field ordered the rows by it.

- **A blank Postgres FTS or vector search reads its read relation, as its cursor does** (**behaviour change**). Its page and count read only rows with an index-heap match, so a view with more rows paged fewer than it walked; a blank page without a limit also stops at `max_results` now.

- **A Postgres `$having` on a date or time output compares as a filter on that column does.** A threshold was sent untyped, so under a session time zone other than UTC it kept the wrong groups; it now coerces by the output's type, and any numeric output takes a fractional bound.

- **`$having` refuses an operator its output cannot take before the query runs.** `$like` on a `$count` failed in Postgres and matched a stringified number in the mock; a text, set or hierarchy operator on a numeric measure is now a 400 on every backend.

- **A Postgres aggregate sorts its groups as every other read sorts rows** (**behaviour change**). A null group comes first ascending and last descending, or where `nulls` puts it; `"ASC"` now sorts ascending rather than descending, and an unknown direction is a 400 rather than a descending sort.

- **An update to a mapping or nested-model field keeps the keys it does not name, in Postgres, Mongo and Firestore** (**behaviour change**). The merge patch was stored, so `{"k": 2}` into `{"k": 1, "j": 5}` stored `{"k": 2}`; the field is now written whole. `update_matching`, which cannot merge, refuses such a field.

- **A search with result snapshots can return projected hits.** `project_search` and `select_search` on a snapshot-enabled spec failed in Postgres and Meilisearch, because the snapshot was filled from the projected rows; it now keeps whole records, and only the page returned is projected.

- **A `$having` bound on a time bucket compares with the instant the bucket starts at.** In another zone, Postgres read an aware bound as UTC wall time and the mock matched no datetime at all; a naive bound is wall time in the bucket's zone, and a Postgres `timestamp` column is read as UTC before it is bucketed.

- **A string range bound in `$having` or a metric `filter` compares as its field's type in the mock and Mongo, as in Postgres.** A `$gt` bound like `"40"` was compared with a number as text; `$like` on a numeric or time output is refused everywhere, and Mongo takes a `Decimal` bound.

- **A string bound on an integer column filters in Postgres.** `{"age": {"$gt": "28"}}` failed with `Invalid int`; it compares as the number, as on the other backends.

- **`uuid7` ids keep their nanosecond order.** The variant bits overwrote two bits of the sub-millisecond time, so ids minted under 256 ns apart could sort backwards, as `FrozenTimeSource` and the DST clock mint them 1 ns apart. Existing ids stay valid; `high_precision` reads them within 256 ns.

- **A search page with `limit=0` is empty when it writes or replays a result snapshot.** It returned one hit.

- **An encrypted field of any type can be updated in Postgres, Mongo and Firestore** (**behaviour change**). The patch was sealed before the domain merged it, so an encrypted `int`, mapping or date failed validation; it is now merged open, then sealed. Diffs report encrypted fields open; the mock rewrites a named one.

- **The in-memory mock sums `Decimal` values exactly, as Postgres sums `numeric`.** An aggregate's `$sum` over `Decimal` fields came back as a float, dropping digits a ledger total keeps.

- **A `Decimal` field is written in fixed point in JSON** (**behaviour change**). A forze model wrote `Decimal("0.00000000")` as `"0E-8"` and `1000` as `"1E+3"`; both now come out as written, up to an exponent of 100 either way. A graph property filter matches a `Decimal`, at any depth, in either text.

- **A `set` or `frozenset` field is written in one order in every process.** A forze model failed on a mixed-type set and wrote a `frozenset` (or a set of sets) in hash-seeded order. Sets now sort naturally when their items allow, else by canonical JSON text; `pydantic_model_hash` digests of set-bearing models change.

## [0.11.1] - 2026-10-07

### Security

- **Tenant-aware Postgres search returns only the current tenant's rows in every configuration.** Upgrade if a Postgres search route is configured with `tenant_aware=True`.

## [0.11.0] - 2026-10-05

### Added

- **A hub search can raise the limits its filters are parsed under.** `HubSearchSpec(filter_limits=...)` bounds the hub's filter, parsed once for its hub rows on Postgres and in the in-memory hub alike; a member's own limits do not apply to it.

- **A search spec can choose how an unasked page counts its total.** `SearchSpec(default_search_count="approximate")` (or `"none"`) applies when a request sets no `search_count`, so a large log need not run `COUNT(*)` on every page. The request option still wins; Postgres honours it.

- **Authorization can remember a principal's catalog grants.** `AuthzKernelConfig(grants_cache=GrantsCache(ttl=...))` caches roles and permissions by principal, tenant and scope; active status and provider grants stay live. Off by default; role assignment clears it on commit; other changes wait for the TTL.

- **Deployment configuration can grant a permission, and be its only source.** `ConfigGrantsProvider(keys=...)` grants each key to the principal ids a `ConfigGrants` settings model lists and denies it to everyone else, so a catalog binding of the key grants nothing, and the startup step refuses one.

- **A route that runs no operation can require a permission.** `require_permission(key, spec=, ctx_dep=)` is a FastAPI dependency making the authz hook's own decision, delegated actors included, and raising its denial, which `resource_type=` lets a non-disclosing posture collapse.

- **DST can check that a deactivation closes a derived permission.** `no_permission_after_deactivation(deactivate, guarded)` flags a guarded operation that succeeds after the deactivation returned.

- **An allowed origin can be a dev server's port range.** `CookieCsrf` and the realtime WebSocket's `allowed_origins` accept `http://localhost:5173-5199` or `http://localhost:*` on a loopback host. The WebSocket route now refuses an entry that is not an origin (`null`, `*`) when attached.

- **The stored-file kit's write wiring leaves room for your guards.** `bind_stored_file_writes(kit, tx_route=...)` binds upload and delete to their transaction, outbox flush and after-commit stages and returns the registry unfrozen, so authn and authz hooks can bind before it freezes.

- **A document can turn off hard deletes.** `DocumentSpec(hard_delete=False)` drops the generated `kill` operation, so no route or tool reaches it, and the command port refuses `kill` and `kill_many` with `hard_delete_forbidden`. A provisioner's `drop_on_deprovision=True` and raw statements can still delete rows.

- **A soft-deleted row can still be read by id.** `soft_delete_wiring(spec, get_deleted="read")`, or the same option on `AggregateKit`, makes `GET` return the row with `is_deleted` set instead of a 404. Lists still leave it out.

- **A delete or restore can set its own markers in the same write.** A model on the soft-deletion mixin may list `soft_delete_companions`, such as a cascade marker: fields that change only in a write that flips `is_deleted`, so no other update can set or clear one.

- **Stored-file delete can keep the object, and uploads can be capped.** `bind_stored_file_writes(..., purge_on_delete=False)` soft-deletes the row and keeps its blob, dropping only the search entry; `StoredFileKitSpec(max_bytes=...)` refuses a larger upload with `upload_too_large` before a row is written.

- **A spec can raise the limits its filters are parsed under.** `DocumentSpec(filter_limits=...)` and `SearchSpec(filter_limits=...)` take a `QueryFilterLimits`, so an internal read can match more than 1,000 ids. Backends and the mock apply it to filters, aggregate filters and `$having`, as do generated routes.

- **A backend can declare caps its server enforces on a filter.** `QueryCapabilities(max_in_size=..., max_disjunctions=...)` bound an operand list and the disjunctions a filter expands to; a filter past either is refused with `query_feature_unsupported` before it is sent.

### Changed

- **Federated searches that merge full hits use 18–32% less CPU in total.** Results and scores are unchanged; exact duplicate records keep the first occurrence's highlight rather than the last.

- **Federated search merges thin by default** (**behaviour change**). `FederatedSearchSpec.thin_merge` now defaults to `True`: members return ids and only the page is re-read, bounding memory. Existing result snapshots miss once, and a row deleted between ranking and re-read shortens its page.

- **Already-normalized text skips normalization when it is read back.** `normalize_string`, behind the kits' `String` and `LongString`, first checks whether it would change anything: a 500-row read of three such fields went from 16 ms to 1.3 ms. Text that needs work is normalized as before.

- **A debug call below the configured level costs a comparison.** While `configure_logging`'s level is above debug, `Logger.debug` returns before building the structlog logger, about 2 µs to 0.1 µs. Unconfigured, after `structlog.reset_defaults()` or under another wrapper class, debug reaches structlog as before.

- **Authorization and tenant listing read in batches, not row by row.** A decision reads once per kind of row and hierarchy level, plus once per 30 roles or groups: 7 reads, not 27, for a role, its parent and 18 permissions. Tenants are read together, not per membership. Firestore scans need an index on field and `id`.

- **A generated route with no output type writes a model result as a typed route does** (**behaviour change**), at a fraction of the CPU: `None` fields drop under `exclude_none` and `_sa…` keys are kept. Any result writes inf/nan as `null` instead of failing.

- **Sorting by a `NOT NULL` Postgres column, or by the record `id`, can read from a plain index.** The `ORDER BY` now leaves out the null placement a plain btree does not hold, so a document list or blank-query search sorted by it, through a view too, no longer sorts the whole filtered set first.

- **Meilisearch's `ensure_index` makes the `default_sort` fields sortable** (**behaviour change**), as an unsorted browse sorts by them; on an index without them that browse fails with a configuration error naming the attribute. A port whose pinned `sortable_attributes` lacks them refuses to build.

- **A search spec whose `default_sort` places nulls where its backend cannot is refused when the port is built**, on Mongo and Meilisearch, and on a Postgres search cursor as the spec's error, not the caller's.

- **An explicit null placement no Postgres search cursor can keep is refused** (**behaviour change**), as Mongo does; offset pages still honour it. Meilisearch refuses one on any page. Before, the placement was dropped and the cursor walked a different order from the offset page.

### Fixed

- **A RabbitMQ `receive` whose window ends as a message arrives returns messages it can ack.** The deadline could cut off the consumer cancel sent on exit, which closed the channel: the messages just returned could not be acked, and the broker redelivered them later.

- **A blank Meilisearch native federated search follows the members' shared `default_sort`** (**behaviour change**: page order). When every member resolves to the same sort and primary key, an unsorted browse orders by that sort, then the id, instead of index order; members that differ keep the engine's order.

- **A PGroonga search uses its index for any supported index expression** (**behaviour change**). An index on `ARRAY[title, content]` or `(title)` never served it, so each search scanned the table. Matches and order now follow the index's tokenizer and normalizer, as for a `coalesce`-declared index.

- **A PGroonga index mixing columns with an unsupported expression is refused, not half-used** (**behaviour change**). A search over an index such as `(title, lower(content))` matched `title` alone and silently ignored the rest; it now fails, naming the element it cannot serve.

- **`get_many` on Firestore finds a document by its name, as `get` does.** It queried the `id` field in the document body, so a document written without that field (by the console, a migration or another service) was found by `get` and reported missing by `get_many`.

- **Permission providers declaring more than 30 keys work on Firestore.** The check that their keys exist in the permission catalog named them all in one `in`, past Firestore's limit of 30 values, so every decision failed. It now reads 30 at a time.

- **Normalized text drops control characters** (**behaviour change**). `normalize_string`, behind the kits' `String` and `LongString`, kept NUL, ESC, DEL and other controls that are not whitespace; Postgres refuses NUL. They are removed now; an accent a removed character separated from its letter is stored composed.

- **A kits text field answers 422 to a value that is not text.** `String`, `LongString` and the metadata mixin's `display_name` and `description` called string methods on any input, so a JSON number, list, object or boolean answered 500. Pydantic now refuses it; UTF-8 bytes are decoded and normalized.

- **Deactivating a principal works without password or API-key accounts wired.** The cascade resolved both credential stores and failed on a deployment with no route for one. It now closes every store the application wires, from any authn module, and refuses a store wired with only one of its ports.

- **Authorization and tenant membership read every binding on Firestore.** The grant resolver and tenancy management paged bindings by offset, which Firestore refuses past the first page, so a principal with more bindings than one page holds, or a tenant with more members, failed. They read by cursor now.

- **OpenAPI descriptions drop a relative target's leading dot.** A docstring's ``:class:`.Foo` `` renders as `Foo`, as Sphinx shows it, instead of `.Foo`.

- **A read without a limit no longer repeats or drops rows tied on its sort** (**behaviour change**). Ties now break by `id` and each batch seeks past the last row. A projection without a sort key, a model without `id`, or a non-`id` sort on MongoDB pages by offset; on Firestore, which refuses offsets, it is one query.

- **Firestore refuses a filter it cannot run before sending it.** An `$in` past 30 values, or a filter past 30 disjunctions once expanded (`$in` values multiply under AND, add under OR), failed at the server with a validation error; it is now a clean `query_feature_unsupported` naming the cap.

- **A Firestore query it cannot run is no longer reported as a transaction conflict.** A query needing an index, or a shape Firestore refuses, failed as a retryable `concurrency` error; it is now a `configuration` error naming the reason.

- **A cursor continues from the right row whatever the model does to a sort key.** A token took each key's value from the decoded row, so a key the model excluded, aliased, serialized or rewrote on read (`EmailStr` lowercasing a domain) made the next page skip or repeat rows. It now takes the stored value.

- **A pagination offset that is not a whole, non-negative number is refused as the caller's mistake.** A document read sent `-1` to the backend, which answered with a server error; `"abc"` raised one itself; and `True`, `1.9` or `""` were read as 1, 1 and 0. Each is now a `precondition`, on every backend and the mock.

- **A membership filter's size limit holds for every operand collection** (**behaviour change**). A `frozenset` or other iterable given as a `$values` shortcut skipped the `max_in_size` check. A list, tuple, set or frozenset is bounded now, and any other iterable is refused.

- **A cursor sorted by `id` first pages in `id` order** (**behaviour change**). A sort such as `{id: asc, x: desc}` moved `id` to the end and ordered by `x` first; `id` now stays put and later keys are dropped, so MongoDB and Firestore accept it. A token minted for such a sort earlier is refused; start over.

- **An aggregate read without a limit pages its groups in one order.** With no sort it asked for the read model's `id`, which no aggregate returns, and failed; with a sort that tied, groups could repeat or go missing between batches. The group keys now break every tie.

- **A MongoDB aggregate read honours an object-form sort direction.** A sort written `{"n": {"dir": "asc"}}` returned the groups in reverse, as anything but the string `"asc"` read as descending. A null placement MongoDB cannot express is now refused, as on other MongoDB reads.

- **A membership operand given as a set or frozenset works on every backend.** The parser handed it on as is: Postgres could not bind one inside a JSONB element and answered with a server error, and Meilisearch refused a set. Every operand now reaches the backend as a list.

- **A search page without a sort follows the spec's `default_sort`, then `id`** (**behaviour change**: page order). Offset search ignored it on Postgres, Mongo, Meilisearch and the in-memory search. On Meilisearch only a blank query takes them: with search text, relevance ties keep the engine's order.

- **A blank Postgres full-text or vector search sorts every matching row.** Its candidate cap kept the first rows the scan met, so a sorted page over a table larger than the cap could miss the rows that belonged on it.

- **A ranked Postgres search cursor reaches rows whose sort key is null.** The page's order put them last on an ascending key while the cursor's seek reads them as the smallest value, so walking the pages skipped them.

- **A Postgres hub search page breaks ties by `id`.** Rows of equal rank and sort keys had no final key on an offset page, so paging could repeat one row and skip another, and a cursor walk listed them in a different order than offset pages did.

- **A search result snapshot replays only for the order it was taken in.** Its key named the request's sorts but not the `default_sort` an unsorted request follows, or a federated member's, and on Mongo not even the sorts, so a re-sorted request or a changed default could replay the old order.

- **A capped Postgres vector search keeps the nearest rows.** The candidate cap, and a hub's per-leg cap, kept the lowest scores, which are the farthest rows, so on a table larger than the cap the nearest matches never reached the page.

- **A capped Postgres search page holds the rows the uncapped order puts there.** The candidate cap, and a hub's, kept rank ties at its edge in scan order (a hub's also put nulls last), so an offset page could differ between requests and from the cursor. The cap now orders as the page does, on every engine.

- **A Mongo search cursor walks past rows whose sort key is null.** Its seek compared with `$gt`/`$lt`, which never match a null, so walking a sort over a nullable field stopped at the first or last null.

- **A search sort that names `id` ends there**, so offset and cursor pages order by the same keys; a key after the id changed nothing but the cursor's order.

- **Meilisearch sorts by the primary key wherever a sort names the id.** With a custom `primary_key`, an unsorted browse failed because the `id` tie-breaker named an attribute the index does not sort by.

- **A search sort naming a field the read model lacks is a 400 on every backend**, wherever it sits. Postgres answered with a database error, and a key after `id` was dropped unchecked.

- **The in-memory search orders a ranked page by relevance before the sort**, as Postgres, Mongo and Meilisearch do, instead of sorting first.

### Security

- **Firestore reads and deletes by id check the tenant first.** A tenant-aware `get`, `get_many` or `kill` without a bound tenant is refused before it reads the store, as the filtered reads already were.

- **A request no ingress authenticates is refused by default** (**behaviour change**). `AuthnRequirement(required=True)` answers 401 `auth_required` outside the middleware's `anonymous_paths`; list login, refresh and public pages there, or pass `required=False`. CORS preflights pass.

- **The documented authn → authz hook chain freezes.** `AuthnRequired.to_step()` under its default id now provides the `authn.principal` capability that `AuthzBeforeAuthorize.to_step()` requires by default, so authorization runs after authentication; `AuthzBeforeAuthorize.to_step()` no longer needs a `step_id`.

- **Words with "uri" inside them are no longer read as secrets.** The scrubber matched `uri` anywhere in a name, so an audit spec refused metadata such as `manufacturing` or `security`. It now matches only at either end of a segment (`uri_template`, `db_uri`, `dburi`), so a word ending in "uri" is still masked.

## [0.10.0] - 2026-09-30

### Added

- **An app's own routes come from a binding table.** `forze_fastapi.routes` exports `attach_operation_routes`, `RouteBinding` and the endpoint builders, plus `query_endpoint` for a GET whose input arrives as query parameters. Each route is named by its operation key, e.g. `stock.add`; an unregistered binding is refused.

- **The OpenAPI schema can document what a Forze app serves.** `apply_openapi_conventions(app)` documents the Forze error envelope in place of FastAPI's 422 body, adds a `default` error to every operation that lacks one, and renders reST in descriptions (roles, literals, field lists) as Markdown.

- **Wall-clock times convert to instants without guessing.** `CivilZone`, `to_instant`, `local_day_bounds`, `month_bounds`, `spanned_local_days` and `elapsed_minutes` in `forze.base.primitives`: conversions refuse a repeated time without `fold`, a skipped one, or a result past years 1–9999; `AwareDatetime` refuses naive.

- **Permissions can be derived from state per decision.** A `PermissionProvider` (declared on `AuthzKernelConfig(permission_providers=…)`) grants or denies keys from documents or config. A denial outranks every catalog binding, an unknown key is refused, and a late provider denies its keys; the mock decides with them.

- **A confirmation can refuse when its preview has changed.** `PreviewBinding(name=…, projector=…, exclude=…)` returns a fingerprinted preview with `reviewed()`, and `bind()` makes the confirming operation recompute it inside its transaction, at snapshot isolation, and refuse with `preview_changed` on a mismatch.

- **An aggregate kit can audit the operations it generates.** `AggregateKit(audit={kernel op: Audited(...)})` binds the audit hooks and runs an audited write in a transaction, so its row commits with it. `audit_row_per_effect` checks under simulation that each committed audited write carries exactly one row.

- **S3 buckets can be created at startup.** `s3_bucket_lifecycle_step(buckets=[...])` runs after `s3_lifecycle_step` and creates each missing bucket, leaving existing ones alone; a bucket it cannot create fails startup. `s3_lifecycle_step` now provides `s3.client`, so a plan with two of them is refused.

- **An aggregate kit takes your mappers and DTOs, and can run its writes in a transaction.** `AggregateKit(mappers=…, dtos=…)` composes your mappers under soft deletion and versioning; `transactional_writes=True` binds every generated write to `tx_route`; `update_returns="record"` returns the updated record.

- **The number-id step can name what it numbers.** `NumberIdMappingStepFactory(spec, name_field="name")` also sets that field to `name_format` (`"{name} #{number}"` by default), so `"Order"` becomes `"Order #12"`; an empty value is left alone.

### Changed

- **Soft deletion's read mappers compose with the ones you pass.** `SoftDeleteWiring.read_mappers(base)` runs its exclusion after each list-family mapper `base` carries instead of replacing it, so your own restriction still applies.

### Fixed

- **A malformed Inngest envelope no longer fails every retry.** An identity that does not decode is ignored when the function does not bind it, and stops one that does with a `NonRetriableError`; malformed tracing ids are dropped.

- **A multi-tenant app no longer refuses authorization in every tenant but the first.** Grant resolution compared a requested tenant with the tenant bound when its port was first built, then cached for the process, so other tenants' decisions failed with `authz.scope_tenant_mismatch`.

- **Sorting and keyset paging order aware datetimes by instant.** The shared comparator ordered datetimes by their ISO text, so different UTC offsets, or a repeated hour, sorted out of order in the mock and the search merges. A cursor keeps its ISO text and compares as the instant it names.

- **The in-memory store compares aware datetimes as instants, as a real store does.** Filters, the mock's aggregates and its unique guarantee read a zoned value by the wall clock through a repeated hour, so two instants an hour apart counted as one value and one instant written in two zones could match nothing.

- **A `Period` whose endpoints carry a DST zone compares instants, not wall clocks.** Through a repeated hour a one-hour period read as empty, and a valid one could be refused as backwards; `no_overlapping_periods` and the mock's non-overlap guarantee missed overlaps there and reported false ones.

- **The temporal and versioned kits' reads are marked as reads.** `effective_on`, `timeline`, `history` and `as_of` now run under the read-only guard and are listed as read-only in the operation catalog, so an MCP projection that includes only reads now includes them.

### Security

- **Audit rows and versioned corrections record every principal of a delegation.** `actor_id` stays the nearest actor; `actor_ids` holds the whole chain, and a correction gains `subject_id`; its spec must store and read back all three. **Required migration** on Postgres (see docs); until then those writes fail closed.

- **A delegated correction's `actor_id` is the agent that made it** (**behaviour change**). Rows written by 0.8.0 to 0.9.0 hold the user it acted for there; later rows hold the agent, and the user is in `subject_id`.

- **An idempotency key reused by another delegation chain or tenant runs its own operation.** A claim is scoped to the subject and its whole actor chain, and a tenant-aware Postgres store scopes it by tenant, as the other stores do; direct callers' claims are unchanged.

- **A delegation key's agent is checked, and cannot drop the operator's.** It must be another eligible principal (a `service` one with `authz_route` set), checked at issue and rotation. Over MCP it is chained under the operator's agent; with enforced grants that needs `may_act(operator, agent)`.

- **A delegated identity cannot open a realtime connection.** WebSocket, Socket.IO and SSE refuse an identity with an actor at connect and at reauth (`delegate_denied`), since a connection is the principal's own stream, mailbox and cursors.

- **Tenant provisioning reaches the tenant it provisions.** `ObjectStorageTenantProvisioner` creates an onboarded tenant's bucket on that tenant's backend with a routed client, and works with a plain one; `PostgresSchemaTenantProvisioner` refuses a routed client, as the Mongo provisioner already does.

- **A delegated call on a scope-guarded route is scoped for every principal in its chain.** `AuthzDocumentScopeWrap` scopes each actor too: a denied actor refuses the call, its row filters narrow the subject's, and an enforced delegation grant is checked. So does the sensitive-resource check.

- **A delegated caller cannot manage the account it acts for.** Issuing or revoking an API key, minting a session, logging out, changing the password, and switching or leaving a tenant refuse a delegated identity (`delegate_denied`).

- **Work started by a delegated caller keeps its delegation.** Temporal headers carry the actor chain, as does the Inngest `_forze` envelope where `bind_identity_from_event` binds it, and an MCP subject that is already delegated keeps its chain under the server's agent.

## [0.9.0] - 2026-09-28

### Added

- **An operation can leave an audit row, and only the metadata it declared.** `Audited(spec=AuditSpec(...))` records allowed, denied and failed outcomes with the actor from the authenticated identity; an undeclared metadata key raises. `forze_kits.integrations.audit` stores the trail as a document collection.

- **A deployment can refuse to boot with development settings.** `ProductionPosture` declares rules over your own settings (required, HTTPS-only, loopback, development-only values) and refuses at `check_wiring` and when the runtime is built, naming keys, never values. An unset environment is production.

- **A guarantee can hold at commit.** `UniqueTogether` and `NonOverlapping` take `holds="commit"`: a transaction may pass through a violation it resolves, such as an insert at the top of a positioned list. Postgres keeps it with a `DEFERRABLE INITIALLY DEFERRED` constraint; the in-memory store checks it at commit.

### Changed

- **A pre-0.7 `$fields` filter says it belongs under `$values`.** A value operator, a literal on the right, a non-path value, or a string naming no field is refused with that pointer. A string or string enum that names a field still reads as a field compare, the one shape that cannot be caught.

### Fixed

- **The HTTP cleartext-credentials warning reads what is actually sent.** A credential in `default_headers` (`Authorization`, `Cookie`, an API key) now counts; an `HttpAuthConfig` that sends no header does not.

- **Returning to a scope on an operation binder no longer drops what was bound to it.** A second `bind_outer()` or `bind_tx()` on one binder kept only its own steps, silently discarding earlier guards and the transaction route.

- **A deferred Postgres constraint no longer satisfies a guarantee that holds after every write.** Startup accepted a `DEFERRABLE INITIALLY DEFERRED` constraint, and the in-memory store then refused writes the database took. It is refused now, naming the deferral.

## [0.8.0] - 2026-09-27

### Added

- **A refusal about a row can stop saying whether the row exists.** `DenialPosture(mode="non_disclosing", resource_types=…)` on `build_runtime` renders a denial and a not-found about a covered type as one `404` on every transport, while the server keeps the real kind. Off by default, since clients see the change.

- **A read by id can require the row to belong to the caller.** `get` and `get_many` take `owned_by=OwnedBy(field=…, value=…)`: a foreign row is not found, exactly like a missing one, on every backend, and a locking read never locks it.

- **A spec can declare that writes for one key do not interleave.** `SerializedBy(key=…)` among a spec's guarantees serializes writes per owner, in memory on `forze_mock` and through a transaction-scoped advisory lock on Postgres. Mongo refuses the declaration.

- **An aggregate can be effective-dated.** `AggregateKit(..., temporal=TemporalPolicy(...))` adds `valid_from`/`valid_to`, `effective_on(key, on)` and a paginated `timeline(key, start, end)`. It requires the matching `NonOverlapping` guarantee and composes with `versioned` for the bitemporal case.

- **A store can keep periods under one key from overlapping.** `NonOverlapping(key=…, period=…, bounds=…, where=…)` is enforced in memory by `forze_mock` and by an `EXCLUDE USING gist` constraint on Postgres, which startup validates and prints the DDL for, never creating it.

- **An aggregate can be corrected instead of overwritten.** `AggregateKit(..., versioned=VersionedPolicy(corrections=spec))` keeps a version chain: `correct(...)` retires the current version and inserts its successor in one transaction, and `history`/`as_of` read the chain.

- **A spec can declare what its store must guarantee.** `DocumentSpec(guarantees=(UniqueTogether(...),))` states a property of the data: a backend that cannot keep it refuses at wiring, `forze_mock` enforces it, and Postgres and Mongo check the live catalog at startup, printing the DDL they need.

- **An outbound OAuth2 grant can be acquired, not just stored.** `build_authorize_url`, `read_authorization_callback` and `complete_authorization` in `forze_identity.oauth` cover the redirect and the callback, and `OAuth2TokenClient` exchanges the code and serves every later rotation.

- **An outbound HTTP operation can send a form body and read its own error responses.** `body_encoding="form"` sends a form, `error_type=` validates a rejected response into `details["response_error"]`, and `HttpAuthConfig(kind="basic")` presents HTTP Basic. A credential over plaintext HTTP warns at wiring.

- **An outbound route can declare that data leaves your trust boundary.** `HttpServiceConfig(egress_sensitive=True, acknowledge_data_egress=True)` makes the egress a reviewed wiring fact, and declaring it without acknowledging fails closed. The inference configs share the gate.

- **An in-process agent's tools can be your operations.** `operation_tools(registry, include=…)` projects allowlisted operations into tool definitions, and `dispatch_tool_use` runs a call through `run_operation`, so tenancy, permissions, deadlines and audit apply. Read-only by default.

- **A model call can stay inside the plane.** `HttpInferenceConfig(protocol="openai_chat")` or `"anthropic_messages"`, with a `PromptTemplate`, calls a chat model under the operation's deadline, tenant, credentials and egress declaration, can constrain output to a schema, and simulates through `MockInferenceAdapter`.

- **An application can catalogue the identity planes it actually wires.** `forze_identity.spec_contributions(planes=["authn"])` narrows the contribution, so an authn-only app passes `build_runtime(specs=…)` reconciliation. No arguments still catalogues all three.

- **A read model may declare the fields its backend produces.** Mapping a name to `None` in `DocumentSpec.derived_read_fields` marks it derived whatever its shape, and `SpecSeed(derived=…)` supplies it, so a view-backed aggregate is readable from `forze_mock`.

- **A view-backed aggregate can be simulated.** `MockDepsModule(derived_values=MockDerivedRegistry().on(spec, source))` supplies a derived field per row, including rows a workload creates, so an invariant can run over such an aggregate under `forze_dst`.

- **A span of time says which end is in force.** `Period(start, end, bounds)` in `forze.base.primitives` carries its bounds convention, `"[)"` by default, into `contains`, `overlaps` and `intersects`, and `no_overlapping_periods` in `forze_dst.invariants` asserts it over a history.

### Changed

- **A document query port must accept `owned_by`.** `DocumentQueryPort.get` and `get_many` gained the keyword; the shipped adapters and the mock take it, and an implementation of your own fails when a caller passes it.

- **Idempotency claims are stored under per-caller keys.** A key claimed before the upgrade does not replay after it, so a retry spanning the deploy runs again, and an `encrypt_result` record sealed before it no longer opens.

- **A Postgres exclusion-constraint violation is `conflict`.** It was `precondition`, unlike the unique violation it sits beside.

### Fixed

- **A locking read by id takes its lock when the document has a read cache.** `get(pk, for_update=True)` on a cached spec returned the cached row, or a row fetched without a lock, and locked nothing; it now always reads the database.

- **A container sandbox run killed at its memory ceiling could come back as `exited`.** The daemon does not always flag that kill, so a `SIGKILL` under a memory ceiling that the adapter did not send is now `killed_oom`, and a non-zero exit's detail names its status.

- **An outbound HTTP reply came back as a bare `BaseModel`.** `HttpServicePort.invoke` now also takes the operation spec itself and returns the model it declares, so a strict type checker reads the response's fields. The name form is unchanged.

- **Two mapping fields on one deps module could not be passed together under a strict type checker.** A shared key type variable rejected every `StrEnum`-keyed argument but the first, as in `PostgresDepsModule(client=…, rw_documents=…, searches=…)`.

- **A parallel DST sweep died under a coverage session.** `parallel_sweep` failed before any seed ran under `pytest --cov`; it now runs on a `multiprocessing.Pool`, which pickles only the caller's `run` and the seeds.

- **`HttpAuthConfig` with no token sent `Authorization: Bearer None`.** A config declaring no token still sent the header; it no longer does, and the same fix covers `HttpClientLifecycleStep.auth_token` and Meilisearch's `api_key`.

### Security

- **Two principals using one idempotency key no longer share its claim.** A claim is scoped to the tenant and the acting principal, so a caller reusing another's key runs its own operation instead of being served that caller's stored result. A custom store inherits `ClaimPrincipalMixin`.

## [0.7.0] - 2026-09-10

### Added

- **Untrusted code finally has a tier that can hold it.** `forze_sandbox.container` runs a sandbox route in a container with no network, no capabilities and a non-root user; the workspace crosses as tar, a memory over-run comes back as `killed_oom`, and killing a run reaps everything it started.

- **Sandbox routes can bound the child.** Memory, CPU and open-file ceilings plus a dropped user move a `SubprocessSandbox` route to the `process` isolation tier; the kill takes the child's process group, and `run_stream` yields output as it arrives.

- **Code can run out-of-process under governance.** The sandbox plane ships `SandboxSpec`/`SandboxPort`, a `SubprocessSandbox` that declares no isolation, and a scriptable mock. Any provenance but `trusted` fails the boot on an adapter that cannot contain it, and resolved secrets are masked out of captured output.

- **Realtime connections have a shipped resolver.** `build_ws_connection_resolver` and its Socket.IO twin verify one credential ladder — reauth payload, cookie, `Authorization`, opt-in query — filling principal, tenant, device and `expires_at`. Cookie mode will not build without attesting the Origin allowlist.

- **The in-memory mock can survive a restart.** `MockStatePersistence` plus `mock_state_lifecycle_step` load a snapshot at startup and write one at shutdown, so an MVP on `forze_mock` keeps its data without a container. Off by default; one process, no crash durability, still all in RAM — a snapshot, not a database.

- **Mongo implements the inbox.** `MongoDepsModule(inboxes={...})` with `MongoInboxConfig` registers a consumer-side dedup store: one atomic `_id` upsert per message, no index migration, riding the ambient transaction so the mark rolls back with the handler (exactly-once effect).

- **Mongo tenant offboarding holds a lock.** A `dropDatabase` authorised by a stale read could destroy a tenant onboarded in between, so teardown now takes a document lock kept outside the database it drops, and onboarding reads back its own marker. Both refusals are `concurrency`, so a retry treats them as transient.

- **Mongo implements tenant provisioning.** `MongoDatabaseTenantProvisioner` onboards by writing a marker document into the tenant's own database, so a connection missing rights there fails the onboarding rather than a customer's first request. Teardown is opt-in and refuses a shared, system or unmarked database.

- **Mongo implements the rotating-credential store.** Mongo cannot block on a document, so exclusion is a lease rather than a row lock and the plane needs no replica set. A worker inheriting an expired lease re-exchanges only where the previous holder demonstrably never reached the provider.

- **Mongo implements the HLC checkpoint.** One `$max` upsert persists the runtime's clock high-water mark, riding the ambient session so it commits with the rows it stamps. No index to migrate.

- **Mongo implements durable execution.** `MongoDepsModule(durable_step=…, durable_run=…, durable_schedule=…)` registers the step-memo journal, the run store (claims, leases, fences, run control) and the cron schedule store. The run collection needs a partial unique index on `idempotency_key`.

- **Mongo implements idempotency, co-located.** `MongoDepsModule(idempotencies={...})` with `MongoIdempotencyConfig` registers a store whose `commit` rides the caller's session, so the result record and the business writes commit atomically; claims and releases run detached.

- **Archives carry a logical identity beside the byte one.** Each data file records an order-independent `content_digest` of its rows; `compare_content` says whether two builds are the same content, refusing files it cannot compare; `run_manifest` binds a run's report to the commit and lockfile that produced it.

- **Three reflection gates in `forze.testing`.** `assert_pure_module` (import allowlist plus a named forbidden list), `assert_scope_first` (every Protocol method takes the ownership key first: named, typed, positional-only, undefaulted), `assert_operation_namespaces` (edges disjoint). Each refuses an empty discovery.

- **Identity spec selections are enumerable and validated.** `identity_document_names(specs=...)` returns validated names for a selection of identity documents; five feature groups ship beside it (grant resolution's nine tables, decisions, delegation, password lifecycle, tenancy), each pinned to its factory by test.

- **`AuthzDocumentScopeWrap` can explain empty pages** (`explain_empty=True`): no policy restriction makes an empty page `no_match`; otherwise one single-row probe of the caller's own filters decides `not_permitted` vs `no_match`. The probe's rows never reach the caller; off by default.

- **`bypass_paths` on `InvocationMetadataMiddleware` and `SecurityContextMiddleware`** — exact mounted paths neither middleware runs for, so a liveness probe is answered before the runtime scope opens instead of returning 500. Pass `DEFAULT_HEALTH_PATHS`; the empty default changes nothing.

- **`attach_readiness_route` can probe the dependencies, not just the drain gate.** Naming client dep keys in `probes` adds a per-dependency breakdown and a `degraded` 503 when one is unreachable — an un-drained process whose database is gone used to answer `ready`. `probe_timeout` budgets each probe.

- **`RuntimeSettings` holds the argument lists of `bootstrap_logging` and `bootstrap_telemetry`** — log level, render mode, the telemetry exporter and the version parts behind a computed `full_version`. It defaults the render mode to JSON, because a settings object exists where something is being deployed.

- **Every integration now ships a `<Backend>Settings` model** holding the endpoint, credentials and client knobs a deployment sets, and exposing both the connection string its lifecycle step takes and the backend's own config object. A missing endpoint is refused by name when read, and `ssl=True` verifies.

- **`forze.base.settings.EndpointSettings`** — the `host`/`port` pair and the URL-authority grammar every scheme built from it needs: IPv6 bracketing, port joining, blank-host refusal. Subclassed by the URL-building integration settings, which keep their own scheme and query parameters.

- **`MIN_SECRET_BYTES` is exported from `forze_identity`** — the 32-byte floor the authn kernel and signers already enforced, now readable by an application validating its own settings, so a short secret fails at boot naming the variable. `RenderMode` joins `LogLevel` in `forze.base.logging`.

- Four helpers each integration package used to carry its own copy of are now core and exported: `log_server_error`, `drain_domain_events`, `resolve_result_snapshot`, `domains_from_create_payloads`.

- **Empty pages can say why they are empty.** Every page value object and kits response DTO carries an optional `abstention` — `no_match`, `ambiguous` or `not_permitted` — so a gated read can tell "nothing matches" from "rows you may not see". No reason by default; hits beside a reason, or an unknown one, refused.

- **Every backend the published skill described now has code you can run.** Thirteen backends were named in prose and imported nowhere; each now carries a wiring block the corpus gate resolves against the installed packages.

### Changed

- **`yckms_lifecycle_step` and `build_socketio_server` accept a `SecretStr`** for their token and Redis DSN, matching every sibling lifecycle step, and unwrap it at the boundary — so `YcKmsSettings` and `SocketIOSettings` feed them without an unwrap at the call site. Plain `str` is unchanged.

- **`forze[fastapi]` now needs `fastapi>=0.138.0`** (was `>=0.137.0`). The startup checks walk the app's real routes through `fastapi.routing.iter_route_contexts`, which 0.137.0 does not export — importing `forze_fastapi.middlewares` raised `ImportError` there before the app ever started.

- **BREAKING — `forze[mcp]` moves to FastMCP 4 and MCP SDK v2.** `build_mcp_server`, `register_tools` and the auth classes are unchanged, but the SDK's protocol fields are snake_case now, so client code reading `tool.inputSchema` wants `input_schema`. Pin `fastmcp<4` to stay on the 3.x line.

- **BREAKING — the 21 published Agent Skills are merged into one, `forze-skills`.** Installing now brings a routing index over 45 lazily-read reference files; per-skill install is gone, because it left every cross-link dangling. Re-run the installer, then delete the stale directories by hand.

### Removed

- **BREAKING — 16 `routed_<backend>_lifecycle_step` helpers are gone.** Each only forwarded to `routed_client_lifecycle_step`, which callers now use directly, passing the step name explicitly. Postgres and Temporal keep theirs: those add a capability and schedule bootstrap.

- **BREAKING — the per-package logger-name enums are gone** (`Forze<Backend>Logger`, `FORZE_<BACKEND>_LOGGER_NAMES`, 26 packages). Name a package root instead and the stdlib hierarchy reaches every logger beneath it. Core `ForzeLogger` is unchanged.

- **BREAKING — unreferenced public helpers are removed**: `Logger.notset`, `ForzeSocketIOAdapter.include_routers` (loop over `include_router`), `has_query_values`, `has_query_fields`, `MockRoutedStateRegistry.state_for`, plus 20 unused internals.

### Fixed

- **An idempotency claim reclaimed by a duplicate can no longer be committed or released by the operation it displaced.** The claim records the invocation that took it, wired like the tenant provider — no port signature change. Postgres needs one optional nullable column; the other stores need no migration.

- **The Firestore integration suite runs again.** Its pinned emulator image had aged out of the registry, so all 104 tests errored on `manifest unknown` and took the coverage and conformance gates with them. Bumped to the newest tag the registry serves.

- **A durable run or schedule registered for a tenant nothing is bound to now lands where that tenant looks for it.** The relation came from the binding while the tag and the scoped id came from the argument, so on a per-tenant relation the row was written somewhere nobody reads and the schedule never fired.

### Security

- **A bound tenant can no longer reach another tenant's durable run.** `begin`, `renew`, `load` and the terminal writes enforced no tenant; they now reach the bound tenant's runs and untagged ones. Postgres, Mongo and the mock.

- **`required_tenant_isolation` now covers `durable_step`, `durable_run` and `durable_schedule`**, which previously passed wiring below a declared floor. An explicit tenant contradicting a bound one is refused (`authentication` / `tenant_mismatch`).

## [0.6.0] - 2026-08-15

### Added

**Statement origin — a second tenancy floor, set by what wrote the statement.** `StatementOrigin` (`structured` → `none`, `compiled` → `namespace`, `raw` → `dedicated`), declared per route via `TenancyRouteSpec.origin` / `TenancyRouteGroup.origin` and enforced at freeze in `validate_module_tenancy`, independently of `required_tenant_isolation`. New `required_isolation_for_origin`, `validate_origin_isolation`, `ORIGIN_ISOLATION_FLOOR_CODE` (`"statement_origin_isolation_floor"`). Nothing that wires today changes: `structured` is the default and no shipped config declares an origin.

**Governed dynamic read** — an opt-in read plane for statements whose text is data (catalog SQL, a semantic layer's output, a report builder). New `contracts/dynamic_read/` + `ctx.dynamic_read.query(spec)`, resolvable in `QUERY` operations; Postgres and mock adapters:

- `DynamicReadSpec` (`row_cap=10_000`, `max_statement_bytes=65_536`, `capture_statements=False`) and `DynamicReadPort.run` / `.select(return_type, …)`. No pagination, no `In`/`Out` generics, no encryption field — both shapes are runtime data, so a sealed column returns ciphertext. Per-call `DynamicReadOptions` clamp down only; exceeding `row_cap` raises rather than truncates.
- `PostgresDynamicReadConfig` runs every statement in a `READ ONLY` root transaction on its own connection, with `SET LOCAL statement_timeout` / `search_path` / `ROLE`, streamed over the extended query protocol. No SQL is parsed anywhere: writes, multi-command strings and cross-schema reads are refused by Postgres. Taxonomy `dynamic_read_write_refused`, `_statement_invalid`, `_multi_statement`, `_permission_denied`, `_timeout`, `_row_cap_exceeded`, `_statement_too_large`, `_row_type_mismatch`, `_role_unavailable`.
- **Wiring fails closed:** `provenance` (`"trusted"`/`"untrusted"`) has no default; routes declare `origin="compiled"`, so any tier below `namespace` raises `statement_origin_isolation_floor`. Also `dynamic_read_untrusted_unconfined` (untrusted with no role or routed client) and `dynamic_read_shared_role_across_tenants` (per-tenant `query_schema`, shared static `role`).
- `PostgresSchemaTenantProvisioner` refuses a shared static role beside a per-tenant schema (`tenant_role_shared_across_schemas`), a role already bound elsewhere (`tenant_role_already_bound`), and one that already reaches somewhere — `LOGIN`, `SUPERUSER`, `BYPASSRLS`, or membership of another role (`tenant_role_not_confinable`). Teardown drops the schema first and reports a surviving dependency as `tenant_role_still_depended_on`.
- `MockDynamicReadRegistry` answers routes from a handler; engine-enforced refusals are declared in the divergence catalog rather than faked. New `dynamic_read` conformance plane and `data-events/dynamic-read.md`.

**Postgres `COPY` bulk load** — the engine-native bulk path, 20–79× faster than the multi-VALUES `INSERT` it replaces (10⁴–10⁵ rows × 6–20 columns, interleaved A/B):

- `PostgresClientPort.copy_rows((schema, table), columns, rows, *, binary=False, column_types=None) -> int` — sync or async row iterables, joins the caller's `transaction()`. Raises `copy_row_invalid` with the server's COPY line/column, or `copy_type_mismatch` for a binary type mismatch. In binary mode a `str` for a declared `json`/`jsonb` column is refused (text and binary want opposite Python types there).
- Analytics `append` on Postgres executes over `COPY`; port contract, encoding, field encryption and `AnalyticsAppendResult` unchanged. **`PostgresAnalyticsConfig.max_append_rows` default rises 10 000 → 100 000.** ClickHouse and BigQuery caps unchanged.

**Temporal, deliberately thin** — package identity fixed at connection, codec, interceptors, schedules and lifecycle (no workflow model, ever):

- `TemporalClientPort.native` returns the configured SDK client (codec, interceptors, rpc metadata included) — the escape hatch the durable contracts' docstrings already promised.
- `TemporalStartOptions` (retry policy, execution/run/task timeouts, id-reuse policy, memo, search attributes, start delay) on `TemporalWorkflowConfig.start_options`, plus a field-by-field per-call override on the Temporal adapter's `start`. Unset fields are omitted from the SDK call; a non-positive timeout raises `configuration`. `DurableWorkflowCommandPort.start` is unchanged, and schedules do not inherit these.
- `temporal_worker_lifecycle_step` + `DEFAULT_WORKER_GRACEFUL_SHUTDOWN`: supervised crash restart, optional `max_consecutive_crashes`, drain registration, and a 10 s activity window bounded by the runtime's drain deadline (the SDK's default is zero). Refuses a worker registering neither workflows nor activities. New *Run a Temporal worker* recipe.
- `ExecutionContextInterceptor(auto_heartbeat=True)` beats at a third of each activity's `heartbeat_timeout`; off by default. Details-bearing heartbeats stay raw SDK.

**Durable run control** — cancellation for the self-hosted function tier, cooperative only (no hard kill in the contract):

- `DurableRunAdminPort.request_cancel(run_id)` records an unfenced ask; only the fence-holding runner lands it (`PENDING` stops at once, `RUNNING` at the holder's next lease heartbeat, a dead holder's run at recovery without re-invoking the body). New `DurableRunRecord.cancel_requested_at`/`cancel_refused_at`; `DurableRunStorePort` gains `mark_cancelled`/`mark_timed_out`/`refuse_cancel`. Tenant-scoped like `list_runs`.
- Sagas decide by the pivot: before it, compensate and land `CANCELLED`; at or after it, refuse and complete forward. New `SAGA_CANCELLED_CODE` / `SagaProgress.step_cancelled_error`.
- New `DurableRunControlAware`/`DurableRunControlCapabilities`; `DurableFunctionRunner.request_cancel` fails closed without `supports_cancel`.

**Breaking:** `DurableRunStatus` gains `CANCELLED` + `TIMED_OUT`; `max_run_duration` now lands `TIMED_OUT` instead of `FAILED` (same `error` text). Telemetry gains the `cancelled` / `timed_out` / `unrecorded` outcome labels; an attempt whose terminal write never lands is now `unrecorded`, not `completed`. `DurableRunStorePort.renew` returns `DurableLeaseRenewal(held, cancel_requested)`, not `bool`. **Migration, required before deploying:** `ALTER TABLE <durable_run> ADD COLUMN cancel_requested_at timestamptz, ADD COLUMN cancel_refused_at timestamptz;`

**Three CI ratchets**, each with a declared, shrink-only exemption table and wired into `just quality`:

- **Docs floors** (`[tool.docs_floors]`, `just docs-check`) — every public `DepKey`/`*Spec` under `contracts/` must be mentioned in `pages/docs`, the nav must resolve both ways, and every relative doc link must land. New `reference/spec-registry.md`.
- **Conformance manifest** (`[tool.conformance_manifest]`, `just conformance`) — every `DepKey` is claimed by exactly one plane, legs carry `@pytest.mark.conformance(plane=…, engine=…)`, and a manifested leg that ran on no shard fails CI. `test_forze_inference` and `test_portability` join the CI matrix. New `forze_dst.conformance` scenarios and `forze_kits.integrations.realtime.conformance`.
- **Mock catalog gate** (`[tool.mock_gate]`) — every operation in the driving example's catalog must resolve and answer against seeded data, and every plane it serves must be one the seed plan fills.
- **Nightly DST matrix** (`.github/workflows/nightly.yml`, `just dst-nightly` / `dst-nightly-all` / `dst-nightly-cells`) — the flagship scenarios over 65,536 seeds per cell across four `FaultProfile` environments declared in `tests/support/dst_flagship.py`, with cells derived from the profiles. The verdict fails on a missing cell, an empty band, a violating seed, a declared reachability target the band never drove, or an undeclared result.

**Operation progress** — long-running work gets an observable shape, kit-level (`forze_kits.integrations.progress`), no core-contract changes:

- `job_record_spec()` (one `JobRecord` per job — status, fraction, message, `heartbeat_at`, `durable_run_id`; DDL documented), the declared `job.progress` realtime event, `ProgressReporter` (coalescing window, monotonic clamp, `track()`), `JobProgressProjector` (`find_stalled`/`count_stalled`) and `progress_spec_contributions()`.
- `job_staleness_lifecycle_step()` + `instrument_job_progress()` export `forze.jobs.stalled`, `forze.jobs.stalled.oldest_silence` and `forze.jobs.staleness.scan_age`. Ticks ride the ephemeral lane, status transitions a dedicated outbox route (`progress_outbox_spec()`); a job is task-grained and spans many durable runs (`waiting` is non-terminal).
- `ArchiveExporter`/`export_archive` and `rebuild_search_index` take an optional `progress=` reporter.

**Observability last mile** — SDK setup, probes, and a deployable Grafana stack; no instrumentation change (no metric added or renamed):

- **`bootstrap_telemetry()`** (new `forze.base.telemetry`, new `observability` extra) — resource identity with a per-process `service.instance.id`, OTLP http/protobuf exporters, millisecond views on `forze.operation.duration`/`forze.durable.run.duration`, and a `TelemetryHandle` with `flush()`/`shutdown()` for the drain path. Defers per signal to an application-installed provider (`on_existing_provider="error"` to refuse instead); config stays on the standard `OTEL_*` variables. Plus `millisecond_histogram_views()`.
- **Probes** — `attach_liveness_route` (`/livez`, drain-blind by design) beside `/readyz`; `probe_listener_step(runtime, port=…)` serves both from any non-HTTP process on stdlib `asyncio`; `attach_metrics_route(router, reader)` for single-process pull scrapes (needs `opentelemetry-exporter-prometheus`; refuses a non-`PrometheusMetricReader`).
- **Docs and assets** — new *The Grafana stack* recipe (compose + `config.alloy` + label/sampling/bucket doctrine + K8s probe mapping), four provisioned dashboards, a Prometheus alert pack, and a new `reference/metrics.md`. Parity-tested against `src/` in both directions, expressions included.

**Cross-backend conformance** — shared batteries for the counter, graph, storage, inference, search, idempotency, field-encryption and realtime-cursor planes; each change below holds on every backend of its plane:

- **Counters** are signed 64-bit: new `COUNTER_MIN_VALUE`/`COUNTER_MAX_VALUE` and `validate_counter_value`; an out-of-range `reset` raises `counter_value_out_of_range`.
- **Storage:** `delete`/`abort_upload` are idempotent; a same-key `copy`/`move` is refused with new `SELF_COPY_CODE`; mock `list` is lexicographic; S3 maps `NoSuchUpload` to `not_found`. **Behaviour change:** the mock models bucket existence (new `MockState.storage_buckets`) — `list` over a never-written bucket now raises unless `missing_ok=True`; reads never provision, the four write paths do.
- **Inference:** a `max_batch_size` cap sub-batches `predict_stream`; a spent budget raises new `inference_budget_exhausted` pre-flight; scalar outputs wrap via new `scalar_output_field`. `FULL_INFERENCE_CAPABILITIES.supports_async_jobs` is now `False`.
- **Search:** `delete_all()` on an unprovisioned index is a no-op; a Meilisearch `invalid_request` raises `precondition` with the engine's message.
- **Graph:** new `normalize_property_filter` and `MockGraphManagementAdapter`; `UUID`/`datetime`/`Decimal` property filters match everywhere; a `None` filter value matches nothing; duplicate vertex keys raise `graph_vertex_conflict` (Neo4j needs `ensure_schema()`).
- **Idempotency:** key reuse with a different payload raises `conflict` on the Redis store too.

**Secrets lifecycle plane** — versions, change feed, hot reload, durable rotation, leases; `SecretsPort` unchanged:

- `resolve_versioned`/`current_version` and `SecretsAdminPort.put` (new `secrets_admin` dep key); per-backend `SecretsCapabilities` fail closed.
- Change feed and hot reload: `SecretChanged`/`SecretsChangeSource`, `SecretsPollWatcher`, Kubernetes-aware `DirectorySecretsChangeSource` (optional `watchfiles` accelerator), `SecretsHotReloadBinder`; `SecretRotated` notifications ride outbox → pub/sub.
- Durable rotation: `SecretRotator` (staged, fenced, CAS promotion) with `forze_postgres.PostgresRotationTarget` and `forze_mongo.MongoRotationTarget` (dual-user by default; degraded single-user/role modes are explicit opt-ins). Leases: `DynamicSecretsPort`, `forze_vault.VaultDynamicSecrets`, `SecretsLeaseManager`.
- Counterparty-rotated credentials: `RotatingCredentialStorePort` (`rotating_credentials` key; new codes `credential_burnt`/`credential_exchange_timeout`/`credential_persist_lost`), `forze_postgres.PostgresRotatingCredentialStore` sealed at rest by default (plaintext needs `acknowledge_plaintext=True`); `forze_kits` adds `CredentialSweeper` and `RotatingCredentialsAdminPort.due_for_refresh`. **Migration:** the documented DDL gains `CREATE INDEX ON <relation> (tenant_id, updated_at)`. **Behaviour change:** the mock's store is tenant-aware per scope — resolve with `deps.resolve_simple(ctx, …)`.
- New `forze.base.primitives.StripedAsyncLocks` and `RoutedTenantClientBase.cached_tenant_ids()`; walkthrough in `examples/recipes/secrets_rotation/`.

**DST evidence** — quantitative verdicts, Adya-complete isolation battery, mock↔real fidelity:

- `forze_dst.stats`: exact Clopper–Pearson detection bounds in every clean verdict, plus a stdlib-only survival kernel (Kaplan–Meier, log-rank, Fisher exact); new `dirty_write`/`intermediate_read` anomaly cases with Adya labels.
- `forze_dst.conformance` fidelity matrix and corpus transfer against real Postgres (`just dst-fidelity`/`dst-transfer`); `forze_dst.misuse` corpus; detection-time campaigns (`forze dst campaign`); mechanical depth extraction (`forze_dst.depth`).
- Falsifiability witnesses and horizon accounting: `Simulation(witnesses=…, horizon=…)`, `mine_witnesses`/`replay_witnesses`/`account_invariants`, `named`/`name_of`, `FaultRule.at_call`. **Behaviour change:** `no_unclosed_transaction()` violations report their own name.
- **Honest denominators** — every clean-run bound states what it divides by, and names its weakest per-invariant denominator. New `stats.coverage_deficit`/`format_coverage_deficit`, `stats.familywise_level`/`format_family_verdict` (opt-in `--dst-family-verdict` / ini `dst_family_verdict`), `stats.flip_margin`, `stats.format_withheld_verdict`, `CoverageStats.shape_counts`/`deficit`, `ConfidenceReport.at_risk_runs`/`unmeasured_exposure`/`shape_counts`/`redundant_seeds`/`weakest_exposure`, and `invariants=` on `HorizonProbe`/`ConfidenceProbe`. Seed redundancy is a warning, never a corrected denominator.

**Serve your real app on in-memory backends** — the mock composes with real modules, and the whole app can be served on it:

- **Hybrid deps contexts** — keyword-only `fallback=` on `Deps.plain`/`Deps.routed`/`Deps.routed_group` marks registrations that yield to a real one instead of colliding, order-independently; `MockDepsModule` marks everything, so "real Postgres, mock everything else" is one deps list. Two real (or two fallback) registrations still raise. New `FallbackReport` from `ProviderStore.fallback_report()`, also on `check_wiring(...).fallbacks`; its `catch_all` names keys where an unregistered route silently reaches the fallback.
- **`forze_mock.server`** (new `mock-server` extra) — `forze mock serve module:attr` serves a declared `MockApp` over `MockDepsModule`, with a `/_mock` control plane offering `reset`, `seed`, `state/{store}`, `fault`, `latency`, `disarm`, `time`, `emit`, `health`. Refuses without `FORZE_MOCK_SERVER=1` or a fallback-marked mock module; the control plane needs a `MockSession` only the builder mints. New `MockState.clear()`.
- **`forze_mock.seeding`** — deterministic seed data across four planes: `SpecSeed`/`SearchSeed`/`StorageSeed`/`QueueSeed` on one `SeedPlan`, with fixtures (`load_fixtures`) and inferred references (`SeedPlan.links`, `SearchSeed(ids_from=…)`). `rng_seed` + `SeedPlan.instant` reproduce a plan byte-identically across processes. Needs `polyfactory` (in the `dst` extra).
- Recipes `mock_server/` (with a container), `mock_workspace/`, `realtime_sse/served.py`; docs: *Serve your app on the mock*.

**Server-side CSRF gate on cookie ingress, on by default** — `CookieTokenAuthn.csrf` (new `CookieCsrf`): an unsafe-method request using the cookie must prove a same-host or `allowed_origins` origin via `Origin`/`Referer`, refused as 403 `csrf_rejected`; `allow_missing_origin=True` admits header-less non-browser cookie clients, `csrf=None` opts out.

**README rewritten** around a runnable, transport-free slice — new test-backed `examples/hexagon/` (domain → port → wiring, no web framework), what the library deliberately does not do, how to write an adapter for an unshipped backend, and a stability statement in place of the SemVer boilerplate; banner now served from the repo.

### Changed

**`build_runtime` takes its modules like every other argument** (**breaking**) — the variadic `*modules` becomes a normal `deps_modules` parameter, so a list no longer has to be spread and can finally be named: `build_runtime(module)`, `build_runtime([a, b])` and `build_runtime(deps_modules=[a, b])` all work. `deps`, `lifecycle_modules`, `lifecycle_steps` and `specs` accept one item or many on the same footing. **Migration:** two or more modules passed positionally become one sequence — `build_runtime(a, b)` → `build_runtime([a, b])`; `concurrent_lifecycle=` is now `lifecycle_concurrent=`.

- `specs=` accepts every contribution to merge — the author's own registry, each `AggregateKit.spec_contributions()`, `forze_identity.spec_contributions()` — instead of one pre-merged registry. Merging happens into a fresh registry, so no argument is mutated; a frozen registry is still accepted alone but is refused inside a list.
- `shutdown_step_timeout`, `cursor_token_signer` and `cursor_token_cipher` join the pass-throughs; setting any of them no longer means abandoning the assembler and composing `ExecutionRuntime` by hand.

### Fixed

**A routed tenant resolver was never found, so authenticated requests bound no tenant** (**behaviour change**) — `TenancyDepsModule`/`local_identity_deps` register `TenantResolverDepKey` routed while `TenancyDeps.resolver()` looked it up plain-only, so tenant-aware adapters failed closed with `tenant_required`. `resolver()`/`manager()` now take an optional route (the authn spec's name), falling back to plain. `MockDepsModule` registered both tenancy keys with the wrong factory arity; new `route_simple_stubs`/`ConstantSimpleMockFactory` fix it.

**Numeric exactness fails closed on every backend** (**behaviour change**) — a value a backend cannot hold exactly raises `precondition` instead of being silently degraded: Firestore refuses non-finite and double-overflowing numerics (previously persisted as `inf`/`nan`); BigQuery `Decimal` parameters pick `NUMERIC` vs `BIGNUMERIC` by value, arrays widening to their neediest element; Mongo refuses a `Decimal` beyond BSON decimal128 exactness; DuckDB refuses non-finite and beyond-`DECIMAL(38)` values (previously rebound as a rounded `DOUBLE`).

**Storage fails closed on an absent container** — two changes:

- `StorageQueryPort.list` returns a `StoredObjectPage` (`objects`, `total`, `container_missing`) instead of a `(objects, total)` tuple, and the kit's `ListedObjects` gains `provisioned`, so `missing_ok` no longer hides that the container is absent (**breaking**). **Migration:** unpack sites become `page.objects` / `page.total`.
- A missing bucket is `configuration`, not retryable `infrastructure` (**behaviour change**) — S3, GCS and the mock agreed on a kind the egress policy reads as retryable, so a saga step or consumer loop retried an unprovisioned bucket forever. Client-facing behaviour is unchanged; only the retry decision moves.

**Graph reads no longer answer short or silently absent** — two changes:

- `GraphNodeSpec`/`GraphEdgeSpec` refuse a `key_field` declared as `bool`/`int`/`float` (`graph_non_string_key_field`); on Neo4j every keyed read of such a vertex answered as absent (**behaviour change**). `Decimal` is allowed: it persists as text, and that text is the key (`Decimal("1.50")` ≠ `Decimal("1.5")`). **Migration:** use a `str`/`UUID` key and keep the number as an ordinary property.
- `neighbors(to_vertex_kinds=…)` applied `LIMIT` before the kind filter and could come back short or empty; the filter now runs in Cypher. A bounded call returns an arbitrary — but full — page.

**Realtime and streaming hardening** (**behaviour change**):

- `build_realtime_mailbox` takes a required `retention=` — a `MailboxRetention` window or `MailboxRetention.unbounded(reason=…)` — and a declared window with no `realtime_mailbox_retention_lifecycle_step` behind it is refused at build (`realtime_mailbox_retention_unwired`) (**breaking**). **Migration:** pass `retention=` at every build, including the Socket.IO/SSE `mailbox_factory`.
- WebSocket: fail-loud closes are bounded (a non-reading client can no longer block them); with no `allowed_origins`, a browser upgrade carrying cookies must be same-host; a connection without device identity keys its ack cursor by the stable `ws:{principal}` instead of forcing a full replay every reconnect.
- Socket.IO: an unset `require_tenant` follows `bind_tenant_from_headers`, dropping untenanted signals instead of emitting them to the global room; a drain-gate refusal mid-bridge stops the loop without acking, counting or poisoning (new shared `forze.application.execution.is_draining_refusal`); the AsyncAPI document and the dispatch surfaces share one set of wire constants (new `REALTIME_ACK_EVENT`/`REALTIME_REAUTH_EVENT`) and the document gains the `realtime.reauth` operation.
- Commit-stream dead-lettering is exactly-once for any producer: a DLQ copy of a message without a `forze_event_id` header carries a deterministically minted dedup id (a re-produced copy could previously process twice); sealed envelopes stay byte-identical.

**Routed consumers survive credential rotation** — `guarded=True` now works across the SQS, RabbitMQ and Kafka facades (previously the first call raised). `evict_tenant` moves an in-flight `RoutedSQSClient.consume` onto fresh credentials at the next poll instead of tearing it down; `RoutedRabbitMQClient.consume` re-acquires the tenant's client per fetch, so it polls where single-tenant `RabbitMQClient.consume` keeps a push subscription. Non-retryable resolution failures still raise out of the stream. **Migration:** `SQSRoutingCredentials.secret_access_key` is now `SecretStr` — read it via `get_secret_value()`.

**The spec inventory closes the routeless-provider blind spot** (**behaviour change**) — declaring a `spec_registry` installs a resolve-time guard (new `inventory_route_guard`): an uncatalogued route on an inventoried plane is refused at first use, whatever the provider's shape; `allow_unregistered=True` downgrades to one warning per route.

**DST `audit()` no longer claims a detection bound for invariants the sweep could not falsify** (**behaviour change**) — such witnesses are new `InvariantStatus.UNEXERCISABLE` and `audit()` fails naming the missing capability. New `forze_dst.oracle.config_capabilities`; `invariant_accounting`/`account_invariants` accept `config=`.

**A plateau-stopped DST `coverage()` sweep printed an exact fixed-`n` bound over a stopping time** (**behaviour change**) — the early stop chooses `n` from the runs the bound summarizes, so the sweep now states its stop reason and prints no bound; `stats.format_withheld_verdict` renders it, and `ConfidenceReport.data_dependent_stop` carries the design. `audit()` is byte-identical (it has no plateau).

**Log scrubbing masks pluralized, numbered, and compound-`authorization` names** — `tokens=…`, `"api_key2": "…"`, `authorization_value: …` previously leaked; the email and userinfo-DSN rules are length-bounded, removing a quadratic-cost path on adversarial log text.

**Postgres `update_many`/`touch_many` could attach one document's returned diff to another** when the server reordered `RETURNING` rows; diffs are now keyed by document id.

**Postgres pool exhaustion answers 429, not 409** (**behaviour change**) — `psycopg_pool`'s `PoolTimeout`/`TooManyRequests` map to `throttled` with code `pool_acquire_timeout` instead of `concurrency`; a wait that ends with the pool unable to connect at all is `infrastructure` with code `database_unreachable`. The server's own `too many connections` is unchanged. Both stay retryable at egress, but neither is retried in place by the `occ` policy, which retries `concurrency` only.

**The mock-server recipe's container never built** — unresolvable version metadata, an unfiltered build context, and a missing `authn` extra; both images are now pinned by digest.

Also fixed: a paged Temporal schedule listing dropped schedules when a `limit` was reached mid-page (cursors stay opaque, but a mid-page one is no longer a bare Temporal token — do not decode it); Mongo counter allocations cost one round trip instead of two, and a legacy document appearing after the counter exists is left in place rather than retired; local KMS seals each wrap under a one-shot HKDF subkey, so the AES-GCM ~2^32 ceiling never accrues against a master key (earlier envelopes stay readable).

## [0.5.1] - 2026-07-25

### Added

**Inference seam** — typed model invocation behind one port; local artifact, served endpoint or cloud runtime is a wiring fact.

- Handlers call `ctx.inference.model(spec)` for single, batch (all-or-nothing) or streaming prediction; a read-plane port, output validated against the declared type.
- Backends: local (off-loop loader, fail-closed warmup), programmable mock, KServe-V2/MLflow over HTTP and SageMaker (extras `inference-http`, `inference-sagemaker`); per-backend capabilities fail closed.
- Remote routes require `acknowledge_data_egress=True` and honour all four tenant-isolation tiers (`dedicated` needs `RoutedInferenceHttpClient` / `RoutedSageMakerRuntimeClient`); upstream error bodies are withheld from errors and logs, and SageMaker pins botocore to one attempt unless configured. Simulation masks inference inputs unless a spec sets `capture_inputs=True`.

**Portability** (`forze_kits.integrations.portability`) — carry an application's system-of-record state to any other wired backend.

- `export_archive` / `import_archive` move the document, blob, graph and counter planes through a backend-agnostic archive (ids preserved, revisions reset); `migrate` fuses the same pipelines port-to-port with no artifact, re-sealing under the target's keys.
- Scope is declared, never inferred; a quiesced claim requires an attested report, and per-tenant exports omit identity specs unless asked. Archives are plaintext unless a sealer is passed; gzip by default, zstd via the `zstd` extra.

**Export foundations** — every plane an application binds is enumerable, quiescible and streamable; an export refuses anything undeclared.

- **Spec inventory** — apps register specs via `build_runtime(specs=…)`, reconciled against the wired deps at startup; identity and aggregate kits contribute theirs. A drift signal, not a gate.
- **Quiesce** — `quiesce()` stops admission, waits out in-flight work, polls the outbox/durable/stream-group planes to rest, and returns settled/attested verdicts; without closing the gate it is a health check that cannot attest.
- **Counter backends** — Postgres, Mongo and Firestore join Redis. Counter operations never join the caller's transaction — **breaking** for custom implementations of the three client ports; Postgres needs an app-migrated counter table.
- **Graph streaming reads** — capability-gated keyset streaming of vertices and edges (Neo4j, mock). **Breaking** for custom graph query ports: two new protocol methods.
- **Search-index rebuild, relay drain, provenance** — `rebuild_search_index` backfills idempotently (standalone or on a kit); the outbox relay can drain on shutdown; analytics specs declare projected vs system-of-record. Read-only admin ports expose outbox depth and counter partitions.

**Realtime transports & operational hardening** — three transports (Socket.IO, SSE, raw WebSocket) behind one versioned wire contract, at the durable plane's operational bar.

- **Versioned wire protocol** — envelope, cumulative ack and handshake are normative; every transport negotiates the version at connect and refuses unsupported ones. The catalog also renders as an AsyncAPI 3 document.
- **SSE and raw WebSocket egress** — replay plus live tail on both; SSE resumes from `Last-Event-ID` and acks over POST; the WebSocket route adds governed command dispatch. Topic subscriptions authorize fail-closed.
- **Fail-closed WebSocket scopes** — middlewares refuse raw WebSocket scopes unless the exact mounted path is allowlisted. **Breaking** for apps mounting raw WebSocket routes behind them.
- **Transport-neutral kernel** — mailbox/cursor seams, replay/ack helpers and presence move into the core realtime integration; `forze_socketio` re-exports every established name unchanged.
- **Supervised loops, retention and perimeter** — a shared background-loop runner (jittered restart backoff, crash ceiling) under all realtime loops; stream trim and mailbox age sweeps bound growth; a poison ceiling bounds redelivery; WebSocket Origin allowlist and continuous credential-expiry enforcement with a built-in reauth command. **Breaking**: `BackgroundLoopControl` moved out of `forze_kits.lifecycle` (unreleased).

**Identity & authn ergonomics**

- **Cookie-mode authn routes** — `AuthnCookieCarrier` via `attach_authn_routes(cookies=…)`: login/refresh set and rotate HttpOnly cookies and strip token strings from bodies; logout expires both idempotently.
- **`SecurityContextMiddleware(anonymous_paths=…)`** — exact paths where an authentication-kind failure binds no identity instead of 401ing; other failure kinds still return the error response.
- **`AuthnDepsModule(eligibility="allow_all")`** — declared opt-out of the policy-principal gate for token-only deployments; unknown values are refused at wiring.
- **Self-hosted durable→registry bridge** — `operation_durable_handler` / `register_operation_functions` auto-bridge `DurableFunctionSpec.operation` like the Inngest tier (`durable_input_invalid`, `durable_output_invalid`).
- **Routed-client ergonomics** — the tenant-scoped accessor is public (`client_scope()`, replacing `_client_scope`) and works in both registry modes; `RoutedNeo4jClient` pins `guarded=True`.

**Keys, crypto & mock conformance**

- **Self-hosted KMS backend** — `forze_kms.local.LocalKeyManagement` wraps data keys under operator-provided 32-byte master keys, no cloud or extra needed; the multi-key map carries the rotation overlap, a rotated-away key id fails closed, and a one-way `fingerprint` spots fleet drift.
- **Every mock field plane seals** — document, graph, search (hub/federated, snapshots), analytics and procedures resolve the same fail-closed encrypting codecs as real backends, via an opt-in synchronous key seam for computation-only backends. **Behavior change** for mock suites asserting raw stored values; text queries no longer match sealed content.
- **OpenBao compatibility** — `forze_vault` works with OpenBao (verified 2.6.1); the integration suite runs against any compatible engine via `FORZE_VAULT_IMAGE`.

**Docs & agent skills** — a "Portability" page (spec inventory, plane dispositions, quiesce, export/import/migrate), cookie-mode authn and `anonymous_paths` documented, new `forze-realtime` / `forze-inference` skills, and a drift sweep realigning the integration index, tenancy/encryption matrices and contracts index.

### Changed

**Breaking**

- **An edge kind identified by its endpoints now enforces that identity** — at most one edge per endpoint pair; a second create conflicts. **Migration:** kinds that allow parallel edges declare a key field and identify by it; ensure/update/keyed kinds unaffected.
- **Graph module specs are validated at construction** — duplicate kind names, endpoints naming unknown node kinds, or a key field missing from its read model fail at build, not first use.
- **Custom realtime signal sources must accept a stop signal** — the gateway, presence heartbeat and identity-expiry sweep are supervised, restart on crash with jittered backoff, and register as drainable.

**Behavior**

- **Background loops stop gracefully instead of being cancelled** — kits loops register per scope and stop between units of work; a commit-stream consumer commits processed offsets even when stopped mid-batch.
- **Realtime gateway defaults harden** — emits time out after 5 seconds; a realtime stream route declaring an encryption tier is refused at start.
- **Redis pub/sub subscribers reconnect by default** — a transport error resubscribes instead of silently stopping; opt out per client config.
- **An outbox relay with no destination is rejected** — naming a never-provided transport spec raises at construction instead of dropping the route.
- **`python-socketio` capped below 6** — the gateway and server builder depend on 5.x constructor surfaces.

### Fixed

**`Decimal` is a first-class filter and sort value across the query DSL** — the scalar union omitted it, and every backend showed it differently.

- Postgres compared nested JSON leaves as text and round-tripped numerics through `float`; Mongo matched nothing; Firestore could not write the field at all; the mock refused Decimal aggregates; Meilisearch indexed lexically.
- Filter values no longer guess locale commas, JSON string bounds cast per field, and non-finite bounds (`"NaN"`/`"Infinity"`) are refused everywhere. **Migration:** rebuild a Meilisearch index whose Decimal fields were indexed from an unreleased build.

**Sealed fields are refused as filter and sort keys on every backend, including the mock** (**breaking**) — filtering a randomized field is refused (searchable fields keep equality), sorting any sealed field is refused including a default sort; previously these returned wrong answers. A kind sealing its own graph key field is refused at spec construction, and Meilisearch `merge="federation"` decrypts through the same seam instead of returning ciphertext.

**A saga step interrupted at its own commit no longer reports a consistent rollback** — all three drivers (in-process, durable, Temporal) compensate nothing on `commit_ambiguous` and raise infrastructure-kind `saga.step_ambiguous` for operator reconciliation (non-retryable in the Temporal mapping); the durable journal keeps the classification across recovery and replay.

**Cross-process fingerprints: sets hashed in iteration order** — **idempotency was live-broken**: a byte-identical retry of a command with a set field was rejected as a payload-hash mismatch. Sets hash order-independently now; search-snapshot and federated-cursor fingerprints fixed with it.

**Persistence tenancy & fidelity**

- **Counter tenancy (Postgres, Mongo, Firestore)** — counters resolve through the bound tenant and fold the spec route into the stored key (shared relations no longer merge sequences); a pre-route sequence is carried forward. Contended Firestore allocation retries instead of surfacing a concurrency error.
- **Mongo history reads scope by tenant** — snapshots are stamped and reads filter strictly, closing a tagged-tier cross-tenant read. **Migration:** a pre-upgrade snapshot carries no tenant and is invisible to every tenant; backfill legacy history rows for pre-upgrade concurrency continuity.
- **Mongo could not store a document with a UUID or Decimal field** — write coercion applies on insert too; reads convert back exactly.
- **Meilisearch reads and filters match the shared seam** — filters validate operator-field fit and cast string ordering bounds to the field's scalar family (no lexical `price < "5"`); exact counts honor `attributes_to_search_on`; `merge="rrf"` sorts only tie-break the fused score (sort-primary = `merge="federation"`). The filter renderer's `read_model` is now **required**.
- **Postgres schema validation no longer rejects boolean columns or skips optional fields** — both union spellings unwrap, and exact type matches beat subclass overlap (`bool`/`int`, `datetime`/`date`).
- **Neo4j transaction-scope integrity** — a scope opens exactly one transaction, a routed transaction pins its client so a mid-scope credential rotation cannot swap it, and a tenant change mid-scope is refused (`neo4j_tx_tenant_conflict`).
- **Mock parity** — the mock graph matches Neo4j on four write-path guards (delete detaches edges, edges require endpoints, duplicate keys conflict, unknown kinds raise), and mock storage creates-or-replaces on unconditional overwrite per the port contract.
- **Object storage** — streamed (multipart) uploads apply their tags after completion instead of dropping them; listing a missing bucket can read as empty via `missing_ok=True` (the default still raises), and listing bounds its per-object HEAD fan-out.
- **A backward keyset page no longer dead-ends navigation** (**behavior change**) — a `before` page landing on the start of the set always carries a forward cursor, and `has_more` reports that same forward answer; every keyset-paging backend.

**Portability, quiesce & inventory**

- **A full-system scope must declare its tenant dimension** (**breaking**) — no default; a full export/migrate writes one archive section per declared tenant (archive format 2), counters included.
- **Imports confirm their tenant scope** (**breaking**) — a per-tenant archive imports against an explicit `tenant=`, and `expect_tenants=` anchors a full-system archive's section set so a tenant deleted from the manifest cannot vanish with checksums passing. The artifact is cross-checked against the manifest and the target's plan: an unlisted data file or an expected-but-unlisted plane refuses the import, and identity or field-encrypted specs require a sealer or an explicit plaintext acknowledgement.
- **Quiesce attests only what it observed** — unreadable planes block attestation; `QuiesceReport` carries `taken_at` (**breaking** for hand-built reports: now required) and the tenant partitions probed, and the export gate refuses an attestation not covering the scope's tenant set. Cleanly stopped loops flush the relay before the sweep (pubsub excepted).
- **Inventory registration refuses conflicting metadata** — a disagreeing re-registration raises; an empty registry over bound planes fails reconciliation; exporting against an empty inventory is refused.

**Realtime**

- **A cap-filled replay no longer lifts the ack clamp** — `iter_backlog` (replaces `has_entries_after`) drains past the cap in bounded rounds and never lets acks claim a partially delivered equal-HLC run; an unconfirmed drain ends the stream (WS close 1012), and `Last-Event-ID` resume is id-anchored on both transports. Cumulative acks no longer skip — then trim — undelivered mailbox entries.
- **SSE acks require `?device_id=...`** (**behavior change**) — the device-less fallback cursor was shared across a principal's tabs; device-less streams still work via `Last-Event-ID` resume.
- **A principal in two orgs could wedge the ack path forever** — the per-device cursor id includes the tenant, and the compare-and-advance loop is bounded (`realtime_cursor_advance_stalled`).
- **An untenanted signal no longer stops the gateway for every tenant** — a per-signal error bounded by the poison ceiling, not a process-terminal verdict.
- **A non-JSON payload never tears down a transport** — `ErrorEnvelope.context` coerces to JSON-renderable values and the `encode_frame` / `jsonable_frame` helpers back every WS frame, SSE data line and Socket.IO error ack; a hostile binary frame closes cleanly.
- **Node-local presence on a multi-node backplane suppressed every live emit** — refused under a pub/sub Socket.IO manager instead of silently dropping.
- **Redis counter reset returned the previous value, not the new one** (**behavior change**); batch allocation uniformly accepts size one and rejects smaller on every backend.

**Broker & consumer delivery integrity**

- **Draining no longer parks in-flight work as poison** — a drain-gate refusal requeues without counting a delivery attempt and stops the loop; the offset-log consumer stops with its offset uncommitted for redelivery instead of dead-lettering a healthy message.
- **SQS FIFO poison and requeue handling** (**behavior change**) — a terminal nack retains a copy on the configured poison queue and deletes instead of wedging the group forever (if the copy cannot be sent, the original is kept and the group stays blocked; standard queues unchanged); `nack(requeue=True, count=False)` restarts the receive count.
- **The RabbitMQ pending map leaked on partial ack and after channel recovery** — channel reopen purges stale delivery tags; only confirmed acks/nacks are counted and settled.
- **Kafka** — a failed rewind is no longer read as a benign rebalance, and a poison marker carries decoded headers and message type so a forwarded sealed envelope stays decryptable for triage.
- **Permanent dependency faults are no longer retried forever** — a deleted or disabled KMS key maps to `CONFIGURATION` on AWS/GCP/Yandex (**behavior change**; ambiguous states like access-denied stay retryable), and the commit-stream supervisor retries retryable crashes indefinitely with a new `crash_alert_after` (default 5 min, `None` disables).

**Outbox, sync & boundaries**

- **JSON-boundary encoding** — transactional outbox staging, Meilisearch search-synced writes, BigQuery analytics ingest and Inngest events now encode to JSON at the seam instead of handing Python-mode maps (live UUID/datetime/Decimal) to JSON serializers. Postgres and ClickHouse keep the Python encode on purpose.
- **The search-sync outbox route declares `require_transaction`** — a marker flushed outside a transaction is refused (`core.outbox.flush_outside_transaction`); hand-rolled wiring without `bind_tx()` fails loudly.
- **Durable search sync works multi-tenant out of the box** (**behavior change**) — `OutboxSearchSync.bind_tenant_from_headers` defaults to `True`; set `False` for the strict posture.
- **`serialize_calls` survives the invocation deadline** — a deadline-abandoned worker is waited out before the next serialized prediction enters the model.
- **Analytics cursor limits use the hardened clamp** — DuckDB and BigQuery cursor pagination coerce and clamp `limit` like every keyset path.

**Leak-shaped fixes**

- **A rejected MCP tool argument no longer echoes the value back** — masked validation error (`mcp_invalid_arguments`) with field-level errors, independent of the host server's `mask_error_details`.
- **The CLI no longer prints local variables on a crash** — the pretty traceback stays, the locals panel (live credentials included) is dropped.
- **The log scrubber masks camelCase and PascalCase names** — `dbPwd` and `secretKey=` mask like their snake_case twins; mid-token runs like `backupwd` stay unmasked.

## [0.5.0] - 2026-07-13

### Added

**Reliability & durability**

- **Self-hosted durable execution (Postgres)** — crash-resumable durable functions and sagas: exactly-once step replay, lease-based recovery with heartbeat renewal, an execution deadline (`max_run_duration`, default 1h — a hung body is cancelled and FAILED instead of renewing its lease forever), multi-worker fencing, delayed and cron runs, and an opt-in read-only admin port. Wired via `forze_kits`; adds croniter.

- **HLC durable high-water mark** — the hybrid logical clock resumes from a checkpoint so a restart cannot re-issue a stamp; the outbox flush advances the mark in-transaction. Opt-in, node-global.

- **Postgres co-located idempotency store** — the dedup record commits inside the business transaction (exactly-once across a crash) when the store reports transactional commit; mock counterpart included.

- **CPU-offload seam** — `run_cpu` / `run_cpu_map` run blocking work off the event loop via a context-bound executor (bounded pool in production, inline and deterministic under simulation), honoring the invocation deadline.

- **Fencing-token capability for distributed locks** — a lock spec requiring fencing tokens fails closed at resolve against a backend that cannot mint them (Redis and the mock can).

- **Wiring check** — opt-in `check_wiring` dry-runs every registered operation against a throwaway context so a missing or misrouted dependency surfaces at startup. Diagnostic only.

- **Neo4j transaction manager** — enlists graph writes into the framework transaction scope (READ COMMITTED); not co-transactional with other backends.

**Querying & read models**

- **Nested-field projection & sorting** — dotted paths into nested models and string-keyed mappings work in projections and sort keys on all document backends. The mock now nests a dotted projection key instead of emitting it flat.

- **Materialized derived fields** — `DocumentSpec(materialized=…)` persists selected computed fields as filterable/sortable columns; in-place search accepts them too (Postgres, Mongo).

- **Lenient read & write-omit fields** — a read-model field may have no backing column and a write may omit columns, for expand/contract migrations; documents on all backends plus Postgres/Mongo search.

- **Streaming reads on Mongo & Firestore** — bounded-memory batched reads bring all three document backends to parity.

- **Procedures port** — `ctx.procedure.command(spec).run(params)` runs a spec-named parametrized statement (Postgres + programmable mock); a tenant-aware route fails closed at wiring unless the SQL binds the tenant.

- **Query parameters** — a typed contract binds query-scoped session settings (Postgres + mock); capability-gated, fail-closed.

- **Aggregate capability gate** — a backend that cannot compile aggregates (Firestore) rejects aggregate queries with a precondition error instead of failing at the driver.

- **Graph parity, schema provisioning & k-shortest paths** — `forze_neo4j` covers the full graph query/command ports with the mock verified equivalent by differential conformance tests; idempotent schema provisioning; native k-shortest paths, weighted opt-in via GDS and fail-closed without it.

**Search**

- **Bounded-memory result streaming** — search streams iterate the ranked set in keyset chunks, capability-gated per backend; unsupported backends fail closed.

- **Vector & hybrid search as a declared capability** — unsupported retrieval features fail closed via `SearchCapabilities`; pages gain an optional scores sidecar; fusion choice and filtered-ANN recall are declared per backend.

- **Facets & highlights** — declared on the spec, returned as optional page sidecars across mock, Meilisearch, Postgres and federated search; unsupported fields or topologies fail closed.

- **Thin federated merge** — `FederatedSearchSpec(thin_merge=True)` late-materializes the RRF merge (Postgres, Meilisearch); opt-in, identical results.

- **Search result caps** — `max_results` bounds an unbounded offset search and `highlight_scan_limit` bounds the PGroonga highlight scan; both opt-in.

- **Hub member from a standalone config** — `PostgresHubSearchMemberConfig.from_search_config` derives a hub leg from a standalone search config.

**Execution & handlers**

- **Two-phase prepare/apply handlers** — `prepare` runs read-only outside the transaction, `apply` inside it; a transaction route is required.

- **Transaction isolation as a fail-closed contract** — an operation declares an isolation level verified against the route's manager, never silently weaker; the current level is introspectable.

- **Cross-aggregate (system) invariants** — declarative `SystemInvariant` laws with detective and preventive (in-transaction rollback) enforcement in kits, compilable into a DST oracle.

- **Resilience control plane (`ResilienceAdminPort`)** — `inspect()` snapshots live per-policy/route state; `force_open` is a manual kill-switch where omitting the route covers the whole policy, and a clear also resets breaker state so recovery is immediate; `retune` hot-swaps a policy, re-validated by the wiring gate.

**Observability & encryption**

- **KEK replacement with a migration overlap** — a key directory may name a previous key: reads accept current and previous while writes use current, re-encryption sweeps move the data, and dropping the previous key restores the strict guard. Key-version rotation still needs no action.

- **Blob re-encryption sweep** — streams every object of a route down and back under a fresh data key, in place and in bounded memory, preserving metadata and reporting rewritten and skipped counts. The rewrite is conditional on the object being unchanged, so a concurrent delete stays deleted rather than being resurrected and a concurrent change is retried once from fresh bytes.

- **Cloud KMS backends (`forze_kms`)** — AWS, GCP and Yandex Cloud envelope-key backends behind the shared `KeyManagementPort` (extras kms-aws / kms-gcp / kms-yc), with transparent key-version rotation and per-tenant KEK provisioning through the same provisioner port as schemas and buckets.

- **One-call logging setup** — `bootstrap_logging` wires framework, integration and third-party loggers plus the uncaught-exception hook; opt-in sampling and dedup volume controls.

- **Unified integration logging** — previously-silent integrations now log under `forze_<pkg>` namespaces with typed logger-name enums; client connect/close logs at trace, off by default.

- **Sampled access logs** — FastAPI and MCP request logging is quiet by default: successes sampled one-in-N, errors always logged, health paths excluded. Successful requests are no longer all logged at INFO; a full mode restores that.

- **Per-port OTel spans & logging** — opt-in per-call CLIENT spans and per-call logging for every resolved port.

- **Signed / encrypted / context-bound cursor tokens** — an opt-in signer HMAC-signs every keyset cursor (or a cipher AEAD-encrypts it) and binds it to spec, tenant and filter fingerprint, rejecting tampered or replayed tokens. Off by default; enabling is a hard cutover — pre-existing cursors 400 once.

- **Per-item stream interception** — an around-stream interceptor capability wraps async-generator port calls: cooperative yield points, whole-stream logging, opt-in mid-stream fault injection.

- **W3C trace-context propagation** — a published event carries its span outbox→broker→inbox (opt-in; relational backends need a traceparent column); outbound HTTP injects it.

- **`EncryptionReach` ladder** — names the messaging encryption reach (none < at_rest < end_to_end); a required reach refuses a weaker route at resolve.

- **Bounded-memory object-storage streaming** — `download_stream` / `upload_stream` move large objects through fixed memory, including client-side-encrypted blobs via a chunked-AEAD format with ranged reads; legacy whole-payload envelopes still read.

**Realtime**

- **Server push + offline store-and-forward** — handlers publish realtime signals through messaging ports; the Socket.IO gateway bridges to tenant-scoped rooms, and durable signals are mailboxed and replayed per device on reconnect.

- **Tenant-aware & multi-node hardening** — per-tenant consume loops, a tenant-sharded outbox relay, TTL-backed presence with heartbeat, credential-expiry eviction, per-emit timeout.

- **BREAKING — realtime delivery envelope** — every Socket.IO frame is the uniform `{id, data}` envelope; clients read data and dedup by id.

**Transports & DX**

- **Offset-log stream consumption** — a fourth delivery model for partitioned offset-committed logs: commit-stream query/admin ports with capability gates, and a kits consumer that commits after inbox dedup (exactly-once effect). Mock adapters and a conformance battery included.

- **`forze_kafka`** — the first offset-log backend (kafka extra): produce, consume and admin (replay, lag), end-to-end encryption, tenancy.

- **Redis stream & pub-sub transports** — production Redis backends for stream and pub-sub specs with optional end-to-end encryption and tenant prefixes; the consumer-group adapter splits data and admin planes.

- **Streaming download route** — the generated download route streams by default with real ranged requests, conditional 304s and a new HEAD route, backed by three read-only storage kit ops. The ETag is now the backend etag; a buffered mode stays opt-in.

- **Top-level front door** — the most-used names re-export lazily from forze and forze_kits; the core never imports kits.

- **Less CRUD boilerplate** — `build_document_registry` derives DTOs when omitted; `document_facade` returns a typed per-call factory.

- **AggregateKit** — one declaration composes a governed aggregate's wiring: document CRUD, soft delete, search-index sync, invariant enforcement, in-transaction outbox flush, route projection, and DST-verifiable invariants. Its four primitives (outbox emit, search sync, soft delete, invariants) are usable standalone.

- **AggregateKit durable search sync** — opt-in `search_delivery=OutboxSearchSync()` routes index maintenance through the transactional outbox: an identity-only marker stages in the write's transaction and a consumer re-reads the committed row (idempotent, reorder-safe, no payload in the event), so a transient index failure converges instead of drifting. With soft delete composed, kit search reads exclude soft-deleted rows; the index must declare `is_deleted` facetable, checked fail-closed.

- **Shared error helpers** — one client-safe error envelope and kind-to-HTTP-status mapping shared by the FastAPI and Socket.IO edges.

- **Mock document adapter tenant scoping on every write** — the in-memory mock injects the tenant column on ensure/upsert/update/touch, matching Postgres.

- **Telegram Login Widget verifier** — verifies Login Widget callback data via Telegram's HMAC scheme with a freshness bound, emitting the canonical assertion.

**Deterministic Simulation Testing (`forze_dst`)** — new package

- **Point-at-a-real-app simulation** — one master seed reproduces a whole run (schedule, faults, latency, inputs, crashes, partitions) over real registries and runtimes, single-process or N-node, with no app changes; a violation minimizes to a reproducible counterexample.

- **Deterministic runtime & seams** — a simulation event loop and time source give byte-identical replay; time and entropy flow through bindable seams, and a determinism gate bans raw time/entropy outside them.

- **Faults, latency, crash & partitions** — declarative fault rules, latency profiles, crash-restart recovery over persisted mock state, and lossy or asymmetric network partitions.

- **Workloads, schedulers & coverage** — scenario rules and a fuzzer generate workloads; PCT and systematic schedulers explore interleavings; coverage-guided exploration and parallel seed sweeps scale the search.

- **Oracles** — built-in invariants (duplicate effects, mutual exclusion, linearizability, consistency models), transactional-isolation oracles up to serializability cycles, commutativity checks, reachability targets, and opt-in value-level checks.

- **Reporting & regression corpus** — causal-graph violation reports and timelines, failure bundles with honest replay semantics, a regression corpus, and a CLI (`forze dst run` / replay / coverage / topology / derive). Bundle serialization round-trips every config field (an introspective guard fails on any future field a serializer misses — it caught stream-fault rules and the pruning flag being dropped).

- **Mock substrate** *(behavior change)* — journalled transactions with MVCC isolation are now the mock default; in-memory outbound HTTP added.

- **Adapter conformance** — a backend-agnostic isolation-anomaly battery asserts the mock matches real Postgres and Mongo, including lock races, outbox→inbox delivery under crash, resilience stores under partition, and identity rotation/revocation under fault. The Mongo leg runs the full battery (abort-vs-block catalogued as a mechanism divergence); contract strengthenings can be engine-scoped, so a backend-specific one never masks a mock deviation.

### Changed

**Breaking — search**

- **Search pages split from the base pagination contract** — search page types now carry facets, highlights and the snapshot handle; the facet, highlight and snapshot value types move to the search contract.

- **`SearchFuzzySpec` is a frozen value object** — was a dict; edit-distance ratio defaults to 0.34 (validated 0.0–1.0), prefix-length removed. No shim.

- **Search options de-leaked** — the raw-Groonga override is removed, the PGroonga plan is adapter config only, candidate caps renamed, and hub/federated member keys move to a multi-source options type.

- **Search index provisioning moves to `SearchManagementPort`** — ensure-index and delete-all move off the command port onto the management port (Meilisearch).

- **Typed value-object configs** — search engine is a tagged union, federated merge takes a shared Rrf, warehouse analytics take a shared IngestSpec (Postgres drops the legacy schema field).

**Breaking — imports & DSL**

- **Application contracts surface consolidation** *(no runtime change)* — codecs move to base serialization, conformity and lock types move to their contracts homes, aggregate laws rename to `SumOf` / `CountAll`, and a `TenantSecretResolver` replaces the per-kind resolver callables.

- **Contract value types import from their contracts home** — resilience store types, lifecycle types, deps maps and outbox staging types import from `contracts.*`, not the execution layer.

- **`update_many` takes `Sequence[KeyedUpdate]`** — not id/rev/dto tuples; single-item update unchanged.

- **`GroupRef` (query grouping) → `GroupField`** — resolves the clash with the authz GroupRef.

- **PEL stream ports renamed** — `StreamGroup*` becomes `AckStreamGroup*` (ports and dep keys), pairing with the new commit-stream ports; behavior unchanged. Stream messages gain optional partition/offset fields.

- **Package restructures** — forze_dst splits into a facade over engines/oracle/artifacts; forze_mock moves adapters and factories (top-level imports unchanged); the notify router becomes a register-then-freeze builder.

**Behavior**

- **Lazy transaction acquisition** — default for Postgres/Mongo/Firestore: a scope defers connection checkout to the first operation; opt out per route.

- **Runtime owns its CPU-offload pool** — a runtime scope binds and closes a scope-lifetime thread pool; with nothing bound, `run_cpu` runs inline.

- **Search snapshots stream their pool** and expose an expiry; hub search defers heavy projection to per-page hydration.

- **Empty filter/sort maps are no-ops** on list/search requests; a structured-but-empty envelope is still rejected.

- **Sizing bounds clamp or reject instead of silently resetting** — an out-of-range batch size is rejected at wiring; a per-call chunk size clamps to the nearest bound.

- **Integration logger namespaces unified** — integrations log under `forze_<pkg>`; update log filters keyed on the old bare prefixes.

- **Hot-path micro-optimizations** — byte-identical output: faster normalization, cursor canonicalization, bulk decode and span construction.

- **Generated FastAPI routes omit null response fields by default** — opt out per attach call to restore explicit nulls; raw-Response routes unaffected.

- **`reencrypt_documents` returns a `ReencryptReport`** *(breaking)* — rewritten and skipped counts instead of a bare int; a row deleted between listing and its write-back is now skipped instead of aborting the pass.

### Removed

- **`msgspec` dropped; the codec layer is Pydantic-only** *(breaking)* — the msgspec codec and serialization module are removed; record models must be Pydantic. Storage value objects become frozen keyword-only attrs classes.

### Fixed

**Durable execution & broker failure paths**

- **Inngest event-supplied identity is not trusted by default** — the envelope's principal and tenant are attacker-controllable, so they no longer bind as identity unless opted in via bind_identity_from_event; payload decryption still uses the envelope tenant (self-authenticating).

- **Temporal saga failures fail the workflow** — a saga CoreException becomes a non-retryable ApplicationError; the deterministic clock survives a plain import through the workflow sandbox.

- **Inngest deterministic failures stop retrying** — malformed events, failed decrypts and non-retryable errors raise NonRetriableError; retryable kinds still propagate.

- **SQS: one poison message no longer poisons the receive batch** — per-message decode failures are isolated (left for redrive; deleted on FIFO so a message group cannot deadlock); FIFO per-message delay and over-length queue names fail closed. Opt-in `poison_queue_url` retains a raw copy (with provenance attributes) before the FIFO delete — the framework's one poison-destroying path; unconfigured destruction now logs a warning naming the knob.

- **RabbitMQ: opt-in dead-letter sink and redelivery counting** — a configured DLX makes a poison nack dead-letter instead of drop, and opt-in redelivery counting lets poison-parking fire past the broker's redelivered ceiling (requires publisher confirms; enabling the DLX on an existing queue requires recreating it). A consumer starting on a queue with no DLX now warns once per queue that poison messages would be destroyed.

- **Kafka admin reads no longer auto-create topics** — existence is checked against the all-topics listing, so querying lag for a mistyped topic returns empty instead of minting the topic.

- **Inngest function-level config** — `InngestFunctionConfig` forwards Inngest's native controls (retries, concurrency, rate limits, batching and more).

- **An unregistered durable run name cannot livelock recovery** — the run lands FAILED with the reason recorded instead of stranding every co-claimed run each sweep; the scheduler likewise isolates a corrupt cron expression. Externally cancelling a run's awaiter also tears down the body and heartbeat tasks instead of leaving them running detached.

- **A transient saga step error no longer triggers irreversible compensation** — a retryable kind (infrastructure, throttled, concurrency) is retried in place with backoff before anything is journaled, so a one-off blip cannot undo committed steps; compensation fires only on exhausted retries or a genuinely non-retryable failure, and a crash mid-blip replays completed steps from the journal. The same retry now guards transient errors during compensation itself.

- **A transient KMS failure during consume is retried, not poison** — decrypt failures classify through the egress policy: retryable kinds requeue (queue consumer) or crash for a supervised rewind-and-restart (commit-stream) instead of being dropped; tampering still parks.

- **The e2e-encrypted DLQ copy is decryptable and keeps its identity** — the dead-letter path re-sealed the decrypted payload tenant-unbound under a fresh id while keeping the old tenant header, so a DLQ consumer could not authenticate it and correlation was severed. The diverted copy is now the original sealed envelope byte-identical, and sealing without a bound tenant drops a stale tenant header rather than contradicting the envelope.

**Reliability — durability, shutdown, resilience**

- **Deadlines enforced at the database driver** — a bound deadline sets Postgres statement_timeout in-transaction and wraps Mongo ops in client-side timeouts, tighten-only, with an asyncio backstop.

- **Cancellation or a deadline tearing a commit is non-retryable** — both surface `commit_ambiguous` instead of a retryable error or a raw CancelledError, scoped to operation-owned tasks so a shutdown cancel still reaches consumer loops; during the body both still roll back and stay retryable.

- **Shutdown reliability** — the drain timeout cancels still-running ops and awaits their unwind before teardown; detached refresh tasks are cancelled; each shutdown hook gets its own timeout.

- **Spawned operations no longer escape drain; engine hops stay inside the admitted op** — nesting is decided by task identity, so a handler-spawned op is admitted, counted and drained, while the engine's own hops (prepare, hedges, post-commit, fan-out) adopt the operation and cannot be rejected mid-drain.

- **A failed after-commit effect no longer discards a committed result** — it is reported out-of-band to an after-commit error handler, now installable via build_runtime; fatal callbacks still re-raise.

- **Inbox dedup no longer treats the ordering key as an event identity** — the fallback is now header then message id (also for causation binding and notify event ids), so distinct headerless events sharing an ordering key both process. Pre-0.4 headerless relay messages degrade to at-least-once instead of silent drops.

- **Inbox exactly-once fails closed on a cross-client misconfiguration** — inbox processing asserts the store commits in the transaction, raising a configuration error instead of silently non-atomic dedup.

- **Resilience hardening** — the breaker classifies by downstream health (429/OCC do not trip it; timeouts count); state maps are LRU-bounded; a blanket policy retrying ambiguous failures is refused at build; a store outage fails open by default.

- **Streams under a port policy now feed the circuit breaker** — acquisition is breaker-gated (including force-open) and mid-stream infrastructure failures record with unary parity, so a stream-only-failing downstream trips the breaker for its unary siblings. Streams get no retry, hedge, timeout or bulkhead slot.

- **Resilience bookkeeping never fails a completed call** — hedge metrics count correctly, and limit, breaker and digest bookkeeping errors surface as metrics only.

- **Hedging requires an explicit safety basis** *(breaking, freeze-time)* — an idempotency wrap alone no longer passes the gate; every hedge must declare a safety basis or fail closed at freeze.

- **Bulkhead no longer over-admits after a cancelled shed** — a shed-then-cancelled waiter no longer releases a slot it never held.

- **Operation OTel spans stop painting expected 4xx failures red** — client-class domain errors record as failed on a clean span, so error-rate alerts track genuine faults.

- **Default resilience executor is per-event-loop** — a bulkhead never resolves a waiter parked on a foreign or closed loop.

- **Idempotency** — the dedup TTL default rises from 30 seconds to 24 hours to cover the redelivery horizon; a store failure after the business commit no longer fails the succeeded op.

- **Batch field decryption no longer stalls the event loop** — large encrypted result sets decrypt off-loop against a thread-safe snapshot, byte-identical output.

- **`require_tenant_id` raises authentication, not internal** — a missing bound tenant is caller-caused.

- **Hard-delete cache-invalidation failures surface at error level** — a deleted document served from cache is a correctness hazard; still best-effort so a cache outage cannot block a delete.

- **Opt-in guard against outbox dual-writes** — an outbox spec can require flush-inside-a-transaction as a checked precondition.

- **Outbox relay no longer dead-letters the backlog on a missing keyring** — the pass aborts with rows left pending; genuine decode poison still parks the row.

**Bounded memory**

- **Unbounded in-process caches gain bounded defaults** — the Postgres estimate lane, Redis breaker local cache, document-cache refresh fan-out and L1 registry sweep; all with escape hatches.

- **More read/list paths stream** — GCS listing pages, mailbox trim projects only ids, Postgres hub parallel search late-materializes, and update_matching batches by keyset instead of one unbounded UPDATE.

- **Streamed offline-mailbox replay** — replay emits page-by-page instead of loading a device's whole backlog.

- **Mongo ranked search late-materialization** — thin rank rows are sorted and paged before hydrating the page by id.

- **Analytics `run_chunked` truly streams** — DuckDB, ClickHouse and BigQuery consume the streamed query one window at a time.

- **`HttpConfig(max_response_bytes=…)`** — caps the in-memory response body; default off.

**Error taxonomy**

- **Client-caused errors no longer masquerade as 500s** — unsupported features raise precondition and malformed values raise validation, uniform across mock, Postgres, Mongo and Firestore; genuine server faults stay internal.

- **Bad query fields are a client error** — a sort or filter naming an absent field raises precondition; a spec's own bad default sort stays configuration.

- **Malformed `$and`/`$or` is a clean 400** — a combinator whose operand is not a list raises precondition instead of an AttributeError.

- **Cursor limits and token fields are validated at decode** — a non-integer page size is rejected and clamped to the 10,000 cap; a crafted token version or non-finite decimal/float raises validation instead of an uncaught 500.

- **Encryption fail-closed** — filtering a randomized-encrypted field raises precondition; encrypted-sort rejection covers every search backend. Declaring an encrypted field as an indexed search content field (randomized or deterministic-searchable — ciphertext never tokenizes) is now refused at spec construction with a configuration error, instead of sealing ciphertext into the index and silently matching nothing.

- **Consistent adapter errors** — the mock rev-conflict matches the real adapters; a missing dependency names what is registered; database errors classify on codes, not message text.

**Correctness & consistency**

- **Reverse cursor pages return the window adjacent to the cursor** — the document assembler, Postgres hub parallel cursor and Mongo search cursor kept the wrong over-fetch sentinel; all now delegate to one shared keyset windowing. Mongo search first-page prev cursor is now null like every backend.

- **Firestore cursor pagination works past page 1** — the anchor snapshot was passed in a list (a TypeError on any real token) and the before direction was inverted; a deleted anchor now fails closed as a stale cursor. The dead start_before_id client-port parameter is removed.

- **`$neq` / `$nin` / `$disjoint` include NULL rows on Postgres** — the renderer uses IS DISTINCT FROM semantics, matching mock and Mongo.

- **Tenant-aware Mongo text search works** — the prefilter (tenant tag, caller filters) was emitted as its own leading match stage, pushing the required first-stage `$text` to stage two, which the server rejects — every prefiltered text search failed. The first match now carries the text query and the prefilter together, across the offset, cursor, count and thin paths.

- **Decimal cursor keys order numerically** — keyset comparison coerces numeric types to Decimal (was string-comparing) and a Decimal sort key round-trips exactly.

- **Intercepted streams close deterministically** — the proxy wrap and both builtin stream interceptors now chain aclose to the backend cursor at close time instead of leaving release to garbage collection; abandonment logs nothing and mid-stream errors keep their classification.

- **Concurrency primitives hardened** — the in-flight lane shields the shared task from one caller's timeout, and the LRU registries no longer double-dispose or dispose under the lock.

- **Mock isolation matches Postgres at the default level** — READ COMMITTED conflict detection anchors on the version actually read, a duplicate-id create race raises conflict, and FOR UPDATE is honoured; verified against real Postgres.

- **DST systematic search is complete again — and honest about pruning** — the DPOR frontier zero-pads the choice prefix, so previously-unreachable schedules are explored; violation reports print a faithful reproduce line. Signature-pruning (a heuristic that can miss a violation behind an effect-equivalent prefix) is now switchable: `dpor_prune=False` walks the full choice tree, and the docs state the actual guarantee.

- **Regression bundles handle non-self-contained strategies honestly** — a bundle that cannot self-replay reports as a clear failure, and one bad bundle cannot crash the batch.

- **Mock outbox/inbox write-through is a catalogued DST divergence** — confirm a premature-visibility finding against a real broker or store. In-transaction outbox status transitions (claim, mark, retry, requeue, reclaim) now journal and revert on rollback, matching Postgres — the catalogued divergence covers visibility only.

- **Kafka commit-stream consumer is loss-free under poison and rebalance** — malformed payloads pause instead of raising, every pause or abort re-seeks to committed, a rebalance listener drops stale routing, and the supervised lifecycle restarts crash-loss-free.

- **Mongo datetimes decode timezone-aware (UTC)** — the client now reads BSON dates as aware UTC like Postgres and the mock (was: naive UTC, caller-visible). The naive reads also made history OCC's three-way check flag a stale-rev update that echoed an unchanged datetime as a false `historical_consistency_violation`; Mongo document history is now covered by integration tests end-to-end.

- **GCS conditional multipart completion covers the metadata stamp** — user metadata binds inside the same precondition boundary as the final write: the single-part rewrite carries it in the conditional write itself, and the compose paths pin the follow-up patch to the composed object's generation — a concurrent overwrite answers conflict instead of inheriting the completing caller's metadata.

- **Firestore write path is OCC- and tenant-safe** — patch does real rev-CAS in a transaction, deletes are tenant-verified, unsupported operators fail closed, creates fail closed on an existing id, and Firestore joins the cross-backend DSL parity harness.

- **CQRS read-only guard covers eager (factory-time) write-port acquisition** — an eager command acquisition in a QUERY factory hits the same guard as a call-time one.

- **Notifications route through the queue consumer** — redelivery dedups on the event id and poison messages park.

- **Outbox relay tenancy** — each claim's tenant is bound before publishing; a tenant-aware outbox on the plain relay fails closed.

**Field encryption & KMS**

- **Durable-secret entropy is a distinct type a seeded source cannot satisfy** — a replayable EntropySource splits from a CSPRNG-only SecretEntropy for nonces, tokens and keys, making a predictable secret unrepresentable; the permit_insecure_entropy flag is removed. A simulation seeds only the replayable seam.

- **`SystemEntropySource.random()` is CSPRNG-backed** — it drew from the global Mersenne Twister while advertising the system CSPRNG.

- **Strict mode for encrypted fields (`reject_plaintext`)** — opt-in rejection of non-ciphertext values in encrypted and searchable fields after backfill, reachable declaratively: the spec flag flows through every codec resolver (documents, search, graph, analytics, procedures), previously hardcoded off.

- **Plaintext data keys no longer reachable via repr** — the keyring's caches and frozen decryptor suppress raw DEK bytes.

- **Cached data keys honor a TTL** — a KEK rotation or revocation takes effect within the configured window; the crypto module now forwards the TTL and cache bounds to the keyring it builds.

- **Confused-deputy guard on decrypt** — with a tenant supplied, the keyring authorizes an envelope's key id against the tenant's own key before any KMS unwrap. The guard holds on decrypt-cache hits too — the sync pre-pass previously skipped it when the key was already cached.

- **Vault Transit signer picks up key rotation** — the cached public key re-fetches after a TTL, so a rotated key verifies without a restart.

- **A sealed field cannot reach the external search index in clear** — index sync re-seals the aggregate's decrypted read model under the search spec's own policy, so a search spec that omitted a field the document spec sealed published that field's plaintext to Meilisearch. The "same policy on both specs" rule was documented but unenforced; it is now checked wherever the two specs meet, failing closed.

**Identity & authorization**

- **OIDC verifier no longer re-fetches JWKS per request** — the verifier and its key provider are built once and reused, so the JWKS cache actually spans requests.

- **Nonce enforcement reachable through presets** — presets forward require_nonce to the token verifier; default off.

- **`ForzeJwtTokenVerifier` guards its session spec** — the same no-cache/no-history construction check as every sibling verifier, so a revoked session cannot be served from cache.

- **Authz grant resolution cross-checks the tenant** — a caller-supplied scope naming a different tenant is refused.

- **OIDC assertion records the validated audience** — the matched audience, not the first list entry.

- **`trust_tenant_header` no longer binds a tenant for anonymous requests on a resolver-gated app** — the raw header fallback applies only without a resolver, or for authenticated requests the resolver did not bind.

**Transport & agent surfaces**

- **MCP no longer leaks internal error details** — boundary errors render through the same egress-masked envelope as HTTP; caller-caused errors keep message and code. Resource templates now mask through the same shared helper as tools — a raw internal error no longer reaches the agent on a caller-owned server.

- **Pre-built pagination DTO is bounded** — `page` and `size` validate at the boundary (`size` capped at 10,000, matching the cursor clamp and the implicit find-many limit), rendering as the standard 422 envelope instead of admitting unbounded materialization.

- **MCP stops advertising idempotency it cannot honor** — the boundary binds no idempotency key, so the retry-replay claim is gone from tool descriptions.

- **MCP tool defaults run their default_factory per call** — uuid and timestamp defaults regenerate per call instead of freezing at registration.

- **Generated FastAPI routes render one 422 shape** — request-validation errors use the shared error envelope; raw ctx/input dropped.

- **A range request over a client-side-encrypted object serves the right bytes** — ranges resolved against the stored ciphertext size, so a suffix range (what media and zip readers ask for first) returned a window shifted by the envelope overhead under a plausible-looking content range, and a range past the plaintext end surfaced as a 400 rather than a 416. Ranges now resolve against the plaintext total.

- **`forze dst replay` survives a bad corpus target** — one unloadable target counts as a failure while the rest of the corpus replays.

**PostgreSQL**

- **Query parameters no longer leak across reads in a caller transaction** — each param-bound read now restores its session settings after the fetch, and analytics and procedure timeout and search-path settings do the same. Previously a procedure's per-tenant search path and statement timeout applied to every statement the caller ran afterwards. A failed restore never masks the original query error.

- **An OCC retry inside a caller transaction can no longer die on the aborted transaction** — on Mongo a write conflict aborts the whole server transaction, so the in-place retry failed with NoSuchTransaction; on Postgres a serialization failure or deadlock did the same, surfacing as masked infrastructure. Both now surface the original clean concurrency error for a whole-scope re-run; Postgres still heals a healthy zero-rows rev conflict in place.

- **`find_many` warns when its implicit 10,000-row cap truncates** — pass an explicit limit or paginate to read past it.

- **`update_matching` bounds its primary-key snapshot** — capped at one million rows by default and fail-closed beyond it; opt back into unbounded with None.

**Adapters & security**

- **Tenant-aware Mongo writes and Firestore queries no longer crash at the driver** — both gateways passed a raw UUID the drivers reject; the tenant is now stamped and filtered as the canonical string everywhere, with tenant-isolation integration tests on both backends.

- **Firestore updates no longer strip the tenant tag** — every gateway write re-stamps the tenant (patch previously full-set the domain image, hiding the row from all tenant-filtered reads); history snapshots are tenant-stamped and a cross-tenant get reads as not-found.

- **Temporal tenant scoping covers listing and handle ops** — signal, cancel, terminate, describe and every schedule op resolve ids through the create-time tenant prefix and refuse a foreign tenant's id; schedule listing filters to the tenant's prefix. Non-tenant-aware wiring is unchanged.

- **Object storage tenant isolation covers reads, not just writes** — every key-taking read, delete, copy and presign path now requires the key to lie within the active tenant's prefix.

- **A missing S3/GCS object classifies as not_found, not retryable infrastructure** — a caller miss is no longer retried or counted against the breaker, and download routes 404 on real backends as on the mock. Bucket-level 404s stay infrastructure, and the re-encryption sweep confirms the container still lists before counting a skip.

- **Object-storage reads no longer create the bucket** *(behavior change)* — listing was the only read path that created on demand, so listing a deleted bucket re-created it and answered "empty", making an absent container indistinguishable from an empty one and letting the re-encryption sweep report a "complete" rotation it never performed. Reads now raise on a missing bucket; only the write paths create on demand.

- **Meilisearch write path** — a failed task raises instead of reporting success, task waits are bounded, tenant-tagged writes and deletes are scoped, and windows crossing maxTotalHits fail closed with the index provisioned to match.

- **Neo4j keyed-edge identity & quantifier coercion** — a keyed-edge ensure now matches on the edge key so distinct keyed edges stay separate, hop quantifiers are integer-coerced before inlining, and walk and path parameters validate their bounds at construction. A filter key must be a plain identifier, since a crafted key could otherwise reach query text through the parameter name; the mock enforces the identical rule from a shared helper.

- **Neo4j transactions honour the per-tenant database** — an enlisted transaction bound to the client's static default and ignored the per-statement database, so under the namespace tier a tenant's transactional graph writes landed in the shared database and reads cross-contaminated. The transaction now binds to the tenant-resolved database of its first statement, and a second, different database inside one transaction is refused rather than silently redirected.

- **Credentials in a logged JSON or dict-repr body are masked** — the log scrubber matched only unquoted key/value forms, so in a serialized body every secret term but one went out verbatim wherever a request, response or webhook body was logged. Quoted keys are now derived from the same term vocabulary as the plain form, and the mask stops at the value so neighbouring fields survive.

- **Logger-name constants are importable as documented** — `FORZE_POSTGRES_LOGGER_NAMES` / `FORZE_REDIS_LOGGER_NAMES` (and their logger enums) are now re-exported from the package roots for `bootstrap_logging` wiring, matching `forze_http`.

- **`forze_redis` imports on redis-py 7 again** — version-specific typing aliases are self-owned; client-side caching fails closed below redis-py 8.

- **Redis idempotency store cannot be corrupted via the idempotency key** — the untrusted key is hashed, results live in a disjoint scope, and commit/fail are fenced compare-and-set. The key format change resets the in-flight dedup window once on upgrade.

- **Temporal default workflow id is a real UUID** — the factory stringified the function instead of calling it, so every unnamed start collided.

- **VK login** — the untrusted introspection envelope is no longer copied into claims.

- **Mongo** — the query renderer rejects dollar-prefixed field names; index introspection keeps string index directions verbatim.

- **FastAPI `X-API-Key`** — splits prefix:secret on the first colon; bare keys still authenticate.

- **Per-tenant routed clients** — fingerprint the full host list, so multi-host DSNs no longer raise.

- **Postgres** — schema validation accepts parameterized column types; search index-definition parsing is delimiter-aware.

- **Log scrubbing closes three leaks** — exceptions render scrubbed under sanitize_logs; assignment scrubbing covers credential-suffix keys, whole Authorization headers and user:pass DSNs; a non-string dict key no longer raises into the log site.

- **Scrubbing fragment lists reconciled** — pwd, passphrase, private_key and six more fragments that existed only as key heuristics now also mask in assignment form (`pwd=…`, `db_pwd=…`, `private_key=…`); a parity test keeps the value-form and key lists from drifting apart again.

- **Ranged reads over encrypted objects detect tail truncation** — the range path now verifies the terminal frame's authenticated final flag (riding an already-required fetch, no extra I/O), raising the same chunked-truncated error as streaming instead of serving truncated bytes as authentic; a spliced early final frame is refused too.

- **`configure_logging()` configures the root logger by default** — with no logger names it previously attached nothing and INFO logs vanished; an explicit list is still an allowlist.

- **Console renderer no longer shares mutable defaults across instances** — the renderer's class-level dict and list defaults were shared by every instance, so per-instance customization leaked between renderers; they are now per-instance factories.

- **Misc** — BigQuery empty-array params typed from annotations; timezone offsets validated; S3 multipart-ETag normalization; If-None-Match parsed per RFC 7232; outbound HTTP suppresses its default bearer when an Authorization header is set; GCS rejects reserved metadata keys.

## [0.4.1] - 2026-06-17

### Added

- **Mergeable quantile sketch** — a relative-error sketch answering any quantile and mergeable across streams, for fleet-wide multi-quantile latency, in windowed and unwindowed forms.

- **Hybrid logical clock** — a skew-tolerant causal clock over the ambient time source, with an optional drift guard.

- **Causal outbox ordering** — opt-in on the Postgres and Mongo outbox configs: events are stamped with a hybrid logical clock and claimed in causal order across replicas. Off by default; Postgres needs a new column, and legacy rows fall back to their creation time.

- **Fleet-wide adaptive-bulkhead congestion signal** — the AIMD latency signal flows through a pluggable digest store, defaulting to the existing in-process one, and Redis-backed storage lets the limit react to the fleet's p95 rather than one replica's. Opt-in.

- **Prioritized load shedding** — opt-in, making the bulkhead wait queue criticality-aware through a task-scoped criticality binding. A no-op until enabled, and requires a bounded queue.

- **Delay-based bulkhead** — a third bulkhead kind tuning concurrency from the latency gradient, needing no latency threshold. Mutually exclusive with the other kinds.

### Changed

- **Quantile estimators relocated** — the P² estimators moved from the resilience module to the shared primitives, alongside the new sketch, and are now public. The old module path is removed; internal resilience wiring is unaffected.

### Fixed

- **Typing annotations** — type-only imports moved under `TYPE_CHECKING` with forward references (including the runtime-optional OpenTelemetry types), so affected modules import cleanly without those optional dependencies installed and skip needless runtime imports.

## [0.4.0] - 2026-06-17

### Added

- **Envelope-encryption core** — an encrypted-envelope primitive, a bring-your-own-key management seam that keeps the key-encryption key in the backend, a field-encryption policy, and a fail-closed minimum-encryption floor ordered none < field < envelope. Adds `cryptography` to core dependencies. Opt-in, off by default.

- **Per-tenant keyring and wiring** — a directory port resolves a tenant to its key, a deps module composes the stack, and the mock ships a dev-only key backend.

- **At-rest sealing across persistence and transport planes** — each plane takes an encryption policy on its spec, fail-closed at wiring and tolerant of legacy plaintext.

- **Object-storage encryption** — opt-in per storage route on S3 and GCS; presigned URLs are refused on an encrypting route.

- **Document-field encryption** — declared per spec. Binding the record id into the authenticated data refuses bulk updates of a bound field, and a re-encrypt pass upgrades legacy ciphertext.

- **Searchable deterministic-field encryption** — AES-SIV, needing no KMS round-trip, so equality and membership filters rewrite to ciphertext; root rotation matches the previous root and re-encrypts. Trade-off: it leaks equality and frequency within a tenant.

- **Encrypted search reads** — the same policy object as the document spec, decrypting out of results across every read path.

- **Analytics and graph encryption** — sealed on write and decrypted out of every read and traversal. Encrypted columns are neither analyzable nor matchable; analytics rejects record-id binding, and graph binds the kind's key field.

- **Outbox and direct-messaging encryption** — at-rest or end-to-end for the outbox, end-to-end for queue, stream and pubsub specs, with tenant and event id bound into the authenticated data.

- **Durable-payload encryption** — opt-in on Temporal and Inngest with per-tenant keys. A Temporal worker must be built from the same encrypting client to decode.

- **Cache, search-snapshot, and idempotency encryption** — sealed when the underlying route encrypts. The in-process L1 cache stays plaintext in memory.

- **Vault Transit KMS** (`forze_vault`) — key management backed by Vault Transit, plus an idempotent provisioner that creates a tenant's Transit key.

- **BYOK access-token signing and JWKS** — a pluggable signer (HMAC by default, plus local-asymmetric and Vault Transit) and a route publishing JWKS. **Breaking:** the access-token service now takes a signer, issuing and verifying became awaitable, and the algorithm config field is gone.

- **Crypto and signing observability** — always-on instrumentation for both.

- **Declared-minimum tenant isolation, fail-closed at wiring** — every deps module accepts `required_tenant_isolation` over none < tagged < namespace < dedicated, enforced per route, and each integration declares its supported ceiling. Additive, with the `None` default unchanged.

- **Neo4j reaches namespace and dedicated isolation** — the database accepts a per-tenant resolver, and a routed client resolves per-tenant Bolt URI and credentials, failing closed on partial auth.

- **Tenant infrastructure provisioning** — idempotent provision and deprovision behind a port, with reference object-storage and Postgres-schema implementations. Opt-in.

- **Analytics per-tenant namespace routing and advisory binding** — query operations route into the tenant's namespace, and `tenant_aware` routes bind the tenant id and fail closed if unbound. Off by default.

- **Tenant-safe structured graph walk and raw gating** — a scoped walk runs an adapter-owned traversal that is tenant-scoped along the full path, and the raw query hatch is now disabled by default. **Breaking:** deployments using raw graph queries must opt back in.

- **Fluent query builder** — `Q.field("age").gt(18) & Q.field("name").like("a%")` lowers to the same filter AST as the mapping form. Additive.

- **Hierarchy operators** — `$descendant_of` and `$ancestor_of` over a tree-path field, using Postgres `ltree` where available and a text-prefix fallback otherwise, gated by a capability flag.

- **Aggregation operators** — `$count_distinct`, `$stddev`, `$var`, `$percentile`, and post-group `$having` on Postgres and Mongo (`$first`/`$last` deferred).

- **Full and array-of-arrays nested quantifiers** on every document backend; the previous capability gate is dropped. `validate_query_field_types` now runs in the gateway and the mock, rejecting mismatches.

- **Mixed-direction keyset pagination with per-key null ordering** — coherent null ordering across backends; old cursor tokens stay valid, and Mongo opts in via `computed_null_ordering`.

- **Query discovery metadata** — `build_query_discovery` projects a read model's filterable, sortable, and aggregatable surface as an OpenAPI extension plus an MCP line.

- **Tenant selector self-service** — `GET /tenants`, an activate endpoint that re-mints a tenant-scoped token pair, and a leave endpoint, via the new `attach_tenancy_routes`.

- **Tenant admin** (`forze_kits.aggregates.tenancy_admin`) — create, list members, invite, remove and deactivate. Ships unguarded, so bind authn and authz per op. **Breaking** for tenant-management implementers: two new listing methods.

- **Self-service API-key management** — issue, list and revoke under `/api-keys`, with the secret returned once. **Breaking** for API-key lifecycle implementers; the account table gains hint and label columns.

- **Delegation-aware API keys (user to agent)** — a key can bind a delegation actor, carried as an RFC 8693 `act` claim. **Breaking** for API-key lifecycle implementers; the account table gains an actor-principal column.

- **MCP boundary API-key auth** — an API-key verifier and identity resolver protect a FastMCP server with the identity plane and no OAuth flow, reads-only by default.

- **OpenAPI security from configured authn** — security schemes are derived from the configured authn requirement, and principal-requiring operations are flagged.

- **Authn plane** — an orchestrator with a full mock identity plane and routes for login, refresh, logout, change-password, deactivate and self-service password reset. Principal deactivation ships unguarded.

- **In-process L1 document cache** — sits ahead of the distributed cache, tenant-scoped with pluggable eviction and Redis invalidation push. Off by default.

- **Stampede protection and adaptive freshness** — singleflight on read-through misses, probabilistic early refresh, per-entry age and sliding TTLs, and a keyword `ttl=` on every setter.

- **New resilience strategies** — adaptive bulkhead over AIMD concurrency, adaptive throttle, tail-based hedging, and a token-bucket rate limit, all configured as per-port policies.

- **Invocation deadlines** — per-operation budgets; expiry raises a timeout, surfacing as 504.

- **Distributed rate limits** — a pluggable store, with a fail-open Redis implementation, lets replicas share one rate. Bulkheads and budgets stay process-local.

- **App assembly and deployment** — a runtime builder and ASGI lifespan, graceful drain defaulting to 10s, and fleet and serverless deployment profiles, the latter rejecting long-running operations.

- **Envelope headers and correlation propagation** — messages gain headers and a delivery count, the relay forwards the full envelope, and the inbox rebinds correlation and causation across broker hops.

- **Outbox ordering key** — per-aggregate ordering, mapping to an SQS FIFO message group or a stream partition key. Requires a new column.

- **Kits queue-consumer runner** — a consumer plus background lifecycle step giving inbox exactly-once, requeue, poison parking and envelope rebinding.

- **Stream pending-entry recovery** — claim and pending inspection over consumer groups. **Breaking** for stream port implementers.

- **Presigned object-storage URLs** — download and upload URLs on S3, GCS and the mock. **Breaking** for port implementers, since minting an upload URL is a CQRS write.

- **Object-storage metadata and access ops** — head, ranged and conditional download, copy and move, and object tags; generated routes honour range and conditional request headers. **Breaking** for the storage and client ports.

- **Resumable multipart uploads** — begin, presign-part, complete and abort behind a session port, guarded as a CQRS write and refused on object-encrypting routes.

- **Storage HTTP edge** — kit ops and generated routes for presigned download and upload and the full multipart session. Minting an upload URL is a command op, so bind authn and authz.

- **Server-side encryption at rest (SSE/CMEK)** — configured per storage route. A separate axis from client-side encryption, which it does not satisfy for an encryption floor. Off by default.

- **Catalog and registry ergonomics** — catalog entries carry idempotency-key and required-permission facts, duplicate merge keys raise with an override hatch, and registration is one step.

- **Generated-route mount ergonomics** — every route-attach helper accepts a resource name (mutually exclusive with a namespace) and per-path overrides. Additive.

- **Scoped, materialized patch authoring** — patches can be scoped to a namespace and folded into per-op plans. Merging now raises when a patch from one registry matches another's ops, which is **breaking** only in that case and can be opted out of.

### Changed

- **Queue consumer and outbox relay are now configurable classes** — the old module-level runner functions become classes with a run method; lifecycle steps keep flat parameters. **Breaking** for direct callers of the old functions.

- **Tenant-isolation tier model made coherent** — the ladder is none < tagged < namespace < dedicated, dropping the old relation rung; each integration owns its supported ceiling and namespace resolution is unified. Key and path formats are unchanged.

- **Argon2 hashing off the event loop** — hashing, verification and the timing dummy are now async on a bounded pool; the synchronous variants remain.

- **Performance (measured)** — the engine hot path roughly halved, document updates copy only changed subtrees, the Postgres and Mongo write paths cut round-trips, and lazy error context with opt-in tracing cut overhead.

- **RPC-style FastAPI routes use REST verbs and query params** — e.g. `GET /notes.get?id=`. **Breaking:** RPC clients must switch from posting to the operation path; REST and MCP are unchanged.

- **The singleton lifecycle step takes a lock spec, not a live port** — **breaking** for callers passing a resolved port.

- **Release-coherence sweep** — the relay logs the at-least-once to fire-and-forget downgrade, Temporal query/update/result deserialize into declared types, the API-key prefix is validated, and saga `step_failed` stays a domain error.

### Fixed

- **Tenant-isolation correctness and parity** — Postgres outbox and inbox enforce the declared isolation floor, a missing bound tenant fails closed consistently as an authentication error, and mock durable, graph, and document adapters now tenant-partition their stores.

- **Post-commit work survives task cancellation** — the after-commit drain runs as a cancellation-protected critical section and then re-raises; cancellation during the body still rolls back.

- **PGroonga search honors tenant isolation regardless of plan** — a tenant-aware search now always uses `filter_first`, overriding index-first plans that scanned cross-tenant rows and could truncate results.

## [0.3.0] - 2026-06-11

### Added

- **Generated FastAPI routes** — project a frozen registry's operations onto the app's router in either a REST or an RPC style, which must be chosen explicitly. Idempotency moves to the engine.

- **`forze_mcp`** — expose operations as MCP tools, read-only by default; commands must be opted in.

- **`forze_duckdb`** — in-process, query-only analytics over a Parquet, CSV, Iceberg or Delta lake on S3, GCS or local disk, with no standing warehouse.

- **Delegated identity (on-behalf-of, RFC 8693)** — the authenticated identity carries an acting principal, authorization enforces a least-privilege intersection, and explicit authority is checked through a delegation port.

- **Operation-level CQRS** — an operation declared as a query runs read-only: command ports cannot be acquired and the transaction opens read-only, enforced by the database. Untagged operations remain commands.

- **Operation catalog descriptors** — interface-agnostic request and response schema metadata, joined with the operation kind, for projecting operations onto MCP or HTTP.

- **Queryable-field policy** — per-aggregate filterable, sortable and aggregatable allow-sets, powering schema discovery and boundary enforcement. Direct port calls stay unrestricted.

- **OpenTelemetry traces and metrics** — every operation gets a span plus a counter and duration histogram. Opt-in, additive.

- **Declarative domain invariants** — an always-true rule enforced on both create and update, closing the merge-patch bypass that model validators allowed.

- **Saga / process orchestration** — declarative multi-step processes across aggregates with typed steps and reverse compensation before the pivot, run by an in-process executor outside any enclosing transaction.

- **DDD domain events and aggregate roots to outbox** — aggregates buffer domain events, and persisting one drains and dispatches them inside the operation's transaction.

- **End-to-end worked example** (`examples/recipes/order_fulfillment/`) — the first runnable, test-backed example: checkout saga to outbox to relay to inbox to downstream, plus compensation, on `forze_mock`.

- **Deterministic time and ids** — the clock and id generator read a context-active time source, so binding a frozen one makes every read deterministic with no call-site changes.

- **Resilience policy pipeline** — composable strategies validated into one policy and run through the context. Adds hedging and a distributed circuit breaker that fails open.

- **Inbox / consumer-side dedup** — a mark-if-unseen port, with a helper that marks and runs the handler in one transaction for an exactly-once effect. Adds a Postgres store plus mock.

- **Graph contracts plus `forze_neo4j`** — query, command and raw graph ports, a Neo4j async Bolt adapter covering CRUD, neighbours, expand, shortest path and a raw Cypher hatch, and an in-memory mock.

- **`forze_kits` — consolidated kit package** — kits, aggregates, mapping, DTOs, outbox/notify, secrets and scopes, absorbing the former patterns and secrets packages and several application modules.

- **`forze_http`** — outbound HTTP services declared by spec and resolved by name from the context, backed by httpx, with per-tenant routed clients.

- **`forze_meilisearch`** — async Meilisearch with offset search, index maintenance, and federated search using native or weighted reciprocal-rank fusion.

- **Transactional outbox, notify, and search-command** — outbox contracts with Postgres, Mongo and mock stores plus relay helpers, a notify kit, and a core search-command port for external-index maintenance.

- **Tenant routing** — declarative per-request backend targets across all integrations, with per-tenant client variants, routed lifecycle steps, LRU pool dedup, and a client registry.

- **Identity — IdP presets** — OIDC presets for Google, VK ID and Telegram Login, bootstrap wiring and PKCE helpers. Authn adds API-key rotation and single-use password invites.

- **Execution — freeze/resolve pipeline** — an authoring registry freezes, then resolves per scope, separating registration from resolution, with a matching lifecycle plan. Per-scope caches default on.

- **Codecs** — a default model codec, per-spec document codecs, optional read and ingest codecs, and trusted-row read validation.

- **Postgres / Mongo search** — Postgres strict and trusted read validation, PGroonga plan modes and hub parallel legs; Mongo text, Atlas and vector search over both offset and cursor paging.

- **Document adapters** — page-count ceilings on scan, stream and chunked commands (default 100 000, unlimited on request) with cursor-stall detection.

- **Durable workflow** — run status and description types, plus describing a run through the durable workflow query port.

- **`forze_temporal` secure connections** — `TemporalConfig` TLS, API key, RPC metadata, and data-converter override; defaults unchanged.

- **AWS — long-lived clients and credential chain (SQS/S3)** — one aiobotocore client is reused, and access key, secret, and region are optional (defaulting to the standard credential and region chain). Per-tenant routed creds still require explicit keys and region.

- **Vault — token renewal, metadata existence, health** — an opt-in self-renew loop, `kv_exists` via KV v2 metadata, and a standard `health()`.

- **`forze_fastapi` upload cap and attach-time validation** — chunked upload streaming under `max_upload_size` (default 64 MiB, `None` disables) with early Content-Length rejection.

- **`forze_socketio` error translation and identity** — handler exceptions become structured ack payloads honoring egress redaction, with an optional connect-time identity resolver.

- **Distributed-lock fencing tokens** (breaking for port implementers) — `DistributedLockCommandPort.acquire` returns an `AcquiredLock | None` carrying a monotonic fencing token. Backends without tokens return `token=None`.

- **Object-storage tags end-to-end** — `UploadObjectRequestDTO.tags` and an `include_tags` flag on head and list (`True` makes S3 pay for `GetObjectTagging`).

- **`IdempotencyPort.fail()`** (breaking for port implementers) — releases a pending claim on handler failure so legitimate retries are not rejected (Redis plus mock).

- **`AuthnFacade.deactivate_principal`** — the existing tested handler is now registered into `build_authn_registry`, exposed, and exported.

- **`forze_mock` parity** — strict transactions, queue and idempotency parity, consumer groups with real ack, keyset cursor pagination, and tenancy, dlock, search, durable, and identity adapters.

- **Shared base primitives** — bounded LRU registries, single-flight and cache lanes, a once-initialized cell, frozen mappings, and fingerprint helpers, all now public.

### Changed

- **Breaking: document write identity is an explicit argument** — `CreateDocumentCmd` no longer carries `id`/`created_at`; the write surface becomes `create(payload, *, id=None)`, `ensure(id, payload)`, and `upsert(...)` with `KeyedCreate`/`UpsertItem` value objects.

- **Breaking: storage CQRS split** — `StoragePort`/`StorageDepKey` split into query (`download`, `list`) and command (`upload`, `delete`) ports, resolved via `ctx.storage.query(spec)` / `.command(spec)`. S3 and GCS factories are renamed.

- **Breaking: coordinators to adapters** — `DocumentCoordinator` to `DocumentAdapter`, the cache and outbox coordinators likewise, and `DistributedLockCoordinator` to `DistributedLockScope`; `forze.application.coordinators` is removed.

- **Breaking: codecs unified on `ModelCodec`** — document, search, and analytics paths materialize through spec-owned codecs, and document kernel gateways require explicit codecs (build via `read_gw`/`doc_write_gw`).

- **Breaking: frozen `attrs` integration configs** — all integration wiring configs are frozen `attrs` (no dict or `TypedDict`); module-level validators are removed (validation at construction or `.validate()`), and some timeout fields move to `timedelta`.

- **Breaking: `ensure_bucket` is create-if-missing on both backends (S3)** — both now create idempotently and race-safe (was a not-found). Use `bucket_exists()` for existence assertions.

- **Breaking: `nack(requeue=...)` semantics aligned (SQS)** — `requeue=False` no longer deletes the message but leaves it for the redrive policy, and `requeue=True` means immediate redelivery. Apps relying on nack-to-drop must `ack`.

- **Breaking: `workflow_id_template` to `workflow_id_base`** — the schedule field is passed verbatim (Temporal appends the fire timestamp); renamed across contract, adapter, and mock with no alias.

- **Idempotency reshaped to engine-level result idempotency** — `IdempotencySnapshot` is replaced by an interface-agnostic `IdempotencyRecord(result: bytes)`, and a new `IdempotencyWrap` hook returns the stored typed result early. FastAPI middleware reads `Idempotency-Key`.

- **OCC retry routed through the resilience pipeline** — Postgres, Mongo, and Firestore write gateways drop their own retry library for the shared `occ_retry` policy. Attempt counts are unchanged.

- **Write gateways — unified OCC/history validation** — Postgres and Mongo share one history-OCC mixin, and a missing history snapshot now raises a retryable precondition error on both.

- **Async contract protocols standardized on `Awaitable[X]` returns** — remaining `async def` Protocol ports converted (type-only; call sites unaffected). Async-generator methods are unchanged.

- **Transaction nesting contract** — nested scopes are savepoints, isolation and read-only are honored only at root, and a conflicting nested read-only raises. `TransactionHandle.id` is removed and it gained `read_only`.

- **Unbounded-read protection unified on the implicit cap** — Mongo and Firestore gain an implicit find limit (default 10 000, `None` disables), and the hard "filters or limit required" precondition is dropped.

- **Analytics SQL pagination wraps in a subquery** — `apply_limit_offset` wraps Postgres and ClickHouse too, and a negative limit or offset now raises.

- **`forze_mock` adapters are stricter (potentially breaking for tests)** — the password verifier actually compares, authz and scope deny by default, and a duplicate-id create raises a conflict.

- **Graph contracts (evolving, pre-1.0)** — dual-addressing `EdgeRef.by_key`/`by_endpoints`, a single-path `shortest_path` plus new `k_shortest_paths`, and a config-raising spec validator.

- **Execution-context lifecycle tripwire, import-linter, kernel consolidation** — constructing an `ExecutionContext` mid-operation warns, plane layering is now lint-enforced (14 contracts), and kernel-client boilerplate folds onto shared lifecycle helpers.

- **Internal package layout** — integration `kernel` to `kernel.client`, `execution` to `lifecycle/` plus deps sub-modules, and registry/planning/facade/run move under `forze.application.execution.operations`. Package-root imports are unchanged; direct internal-module imports must update.

- **Performance** — hookless operations skip body-stage scaffolding (~30%), per-scope caches reuse gateways/adapters/codecs, and JSON logs render via `orjson`.

- **Misc** — Postgres streaming uses a server-side named cursor; outbox uses bulk insert on conflict plus stale-processing reclaim (default 5 min) and `requeue_failed`; `forze[oidc]` now bundles `httpx`.

### Deprecated

- **`forze_identity.oidc`** — `OidcTokenVerifier.enforce_issuer_and_audience` now defaults to `True`, so construction requires both an issuer and an audience unless explicitly opted out.

### Removed

- **Dead public surface removed** — the `forze[arango]` extra, `AccessTokenService.try_decode_token`, `ISSUER_FORZE_JWT`, `EffectiveGrantsAdapter`, the GCS head and listed-object aliases, `PostgresQualifiedName.from_string`, an internal fingerprint module, and a never-honored `delete_many` batch size.

- **`python-dateutil` core dependency** — dropped; `datetime_to_uuid7` parses ISO-8601 via the stdlib `datetime.fromisoformat` (a trailing `Z` is accepted).

- **`forze[casbin]` extra** — dropped (no integration shipped).

- **`forze_identity.local` (breaking)** — use `forze_identity.builtin.local`; local verifiers and factories are no longer exported from authn or tenancy.

- **`forze_identity.builtin.telegram`** — the Telegram Mini App `initData` HMAC preset, superseded by Telegram Login OIDC under `forze_identity.builtin.idp.telegram`.

- **Execution** — `forze.application.coordinators`; the registry, planning, facade, and running modules; `OperationRunner`; and `lifecycle_graph_from_sequence` (use `steps_graph_from_sequence`).

- **Validation helpers from public APIs** — Postgres and integration `validate_*_conf` helpers; validation now lives on the config types. Also dict/mapping coercion for the configurable Postgres document specs.

- **Codecs** — `RecordMappingCodec`, the Pydantic and Msgspec codec families, `codec_for_model`, and public helper functions (use `ModelCodec`/`default_model_codec`); plus `SearchSpec.row_codec` and the effective-row-codec accessor (use `read_codec`).

- **Relocated to `forze_kits` (breaking)** — former `forze_patterns`, `forze.application.{composition,kit,handlers,mapping,dto}`, and `forze_secrets` now live under `forze_kits`; `Mapper`/`MapperFactory` stay on `forze.application.contracts.mapping`. `OutboxDestination` is now the discriminated `OutboxDestination.queue(route=…, channel=…)`.

### Fixed

- **Package error mappers were dead code in 12 integrations** — `ChainExceptionMapper` now flattens nested chains, so Postgres serialization and deadlock errors (plus Mongo and Neo4j conflicts) map to concurrency and OCC retry fires on real serialization conflicts.

- **Firestore transactions** — aborts map to concurrency, rollback happens on `BaseException`, `count_documents` joins the ambient tx, and a mismatched database raises configuration.

- **ClickHouse `run_query_all_pages` is one streaming execution** — a consistent snapshot with no growing-offset duplicates.

- **Redis pipelines fail loud on reads** — value-returning methods inside `pipeline()` raise `redis_read_in_pipeline`.

- **RabbitMQ robustness** — `close()` nacks and requeues pending unacked, poison messages are dead-lettered, and there is one delay queue per distinct delay. The same poison handling applies on SQS.

- **Outbox relay failure model** — codec-decode poison fails immediately; publish failures reschedule with backoff until `max_attempts` (default 5). Adds durable attempt-tracking columns and `mark_retry` (breaking for port implementers), and `requeue_failed` resets the counter. At-least-once delivery.

- **Outbox staging is per-route and per-task** — fixes a process-global flag dropping events and shared buffers. Adds per-route buffer, flushed, and peek accessors.

- **`GuardedLruRegistry` use-after-dispose race** — refcount transitions and eviction reads happen under the registry lock, and a dispose error during drain deregisters and propagates.

- **After-commit callbacks run to completion** — a failing post-commit callback no longer skips the rest; failures aggregate into one error.

- **Lifecycle steps are shut down exactly once** — per-scope started-state tracking ends double-shutdown on failed startup.

- **`finally` hooks observe before-hook denials** — before hooks run inside the try/finally, while `on_failure` stays handler-only.

- **OCC history validation hardened** — records are re-keyed by id and revision, and comparisons run in canonical space so no-op resends do not falsely conflict.

- **`Document.update()` re-validates the patched state** — it merges into a python-mode dump and re-validates, so semantic no-ops yield an empty diff, partial nested dicts and ISO datetimes are no longer raw, computed-field keys are excluded, and validators run on update.

- **Concurrent graph waves report all failures** — an exception group for two or more; a single failure raises directly.

- **Per-scope port cache works for per-call specs** — value equality with an identity fast-path first.

- **`kill()`/`kill_many()` verify row counts on every path** — all paths raise not-found on missing rows.

- **SQS message identity fixed (was breaking inbox dedup)** — `QueueMessage.id` is now the broker message id, and the receipt handle moves to the SQS-specific message type.

- **Postgres transaction options no longer leak across pooled connections** — read-only and isolation are emitted as `SET TRANSACTION …` inside the root tx.

- **Mongo write conflicts retry under OCC** — write-conflict and transient-transaction errors map to concurrency.

- **`forze_fastapi` middleware errors return proper status codes** — core exceptions in forze middlewares render the standard JSON error instead of a 500.

- **RabbitMQ/SQS receive and consume defaults** — bounded receive windows and a uniform idle-timeout consume (`None` is forever, finite is a clean stop).

- **`DistributedLockScope` no longer loses the lock silently** — a lost heartbeat is raised as concurrency at scope exit without masking the body's exception.

- **Notify consumer dedup** — the event id is derived deterministically from the broker message identity (was a random UUID).

- **All integration kernel clients** — `initialize()`/`close()` serialize on an internal lock, and partial-failure assignment is hardened (BigQuery, Postgres, Redis).

- **Analytics adapters** — chunked execution rejects a non-positive fetch batch size via a shared validator.

- **Misc fixes** — Postgres on-conflict targets, PGroonga index-first cap and exact count, Mongo bulk-upsert miss detection, Meilisearch federated finalization, identity duplicate/ambiguous-login detection, and sorted query params in the connection fingerprint.

- **`forze_temporal` and `forze[mcp]` workflow sandbox** — the sandbox runner passes `beartype` and `coverage` through, fixing circular-import failures and a coverage-induced test hang.

### Security

- **Password change revokes existing sessions (breaking by default)** — `change_password` revokes all sessions (refresh families plus session-bound access JWTs); opt out explicitly, and missing session ports fail at startup.

- **Rehash-on-login (opt-in)** — the Argon2 verifier persists parameter-upgraded hashes after login, OCC- and fire-safe.

- **`sensitive=True` spec marker keeps credentials off generated surfaces** — route, tool, and resource generators refuse sensitive specs at attach time, and shipped authn specs are marked.

- **Owner-override permission keys configurable and documented** — the admin bypass moves to `AuthzKernelConfig.owner_override_permissions` (defaults unchanged, an empty set disables it).

- **`tenancy_mode="global"` warns over tenant-partitioned stores** — grants are shared across tenants; set `require_invocation_tenant` for isolation.

- **OIDC nonce value binding** — `verify_id_token_nonce` (constant-time) plus `generate_nonce()`/`generate_state()`; the VK and Telegram exchanges accept an expected nonce.

- **Secret values masked in reprs framework-wide** — credential value objects become non-repr, and several ClickHouse and Inngest fields become secret. Direct readers must call the secret accessor.

- **Outbound HTTP does not follow redirects by default** — prevents custom credential headers from following a malicious 30x to an attacker host.

- **`AuthnDepsModule` rejects a token-verifier override without a resolver override** — a principal-collision hazard; it fails at startup naming the route.

- **Tenancy adapters enforce the cache/history guard** — a cached principal-to-tenant binding could otherwise keep a detached principal resolving after revocation.

- **Cursor pagination tokens validated as client input** — malformed, stale, or tampered tokens raise 4xx (was a 500), and values are restricted to JSON scalars.

- **Log message text is scrubbed** — string scrub rules apply to the rendered message after interpolation, not just structured extras.

- **Postgres sort direction whitelisted** (asc/desc only); S3 object tags URL-encoded; `OidcClaimMapper` rejects an empty issuer or subject.

- **5xx responses no longer leak internal diagnostics** — generic detail for status 500 and above, sanitized context restricted below 500, and configuration-kind details are no longer sent to clients.

- **Authz document-scope filters fail closed** — a scope port returning row filters with no DTO attribute to carry them raises configuration (was silently dropped to an unscoped query).

- **Raw-query tenancy hardening** — the raw graph query (`forze_neo4j`) fails closed in a tenant-aware module and binds the tenant; adds `ctx.tenancy.current()`/`require_current_id()`.

- **Missing authentication surfaces as authentication (401), not authorization (403).**

- **`builtin.local` API-key verification no longer 500s on non-ASCII input** — via a UTF-8 bytes comparison.

- **`asyncio.CancelledError` passes through exception interceptors** — it was converted to a core exception, breaking timeouts, structured concurrency, and graceful shutdown.

- **`forze_identity.authn` session enforcement (breaking)** — access JWTs carry a session id cross-checked against the session store. Pre-upgrade tokens without it fail until re-login (or register a stateless verifier override).

- **`forze_identity.authn` `change_password` requires the current password (breaking)** — it re-authenticates first, so a hijacked session cannot escalate to account takeover.

- **`forze_identity.authn` principal eligibility (breaking)** — the authn and credential lifecycle is gated on an active flag, deactivation cascades, and API keys enforce expiry.

- **`forze_identity.authn` login hardening** — a generic 401 for all failures, always running an Argon2 verify (anti-enumeration and timing).

- **`forze_identity.authz` fail-closed tenant isolation** — grant-resolution adapters refuse to construct when a tenant-scoped route has a non-tenant-aware binding or catalog port.

- **`forze_identity.oidc`** — resolves JWKS signing keys in a worker thread, so a cache miss does not block the event loop.

- **Secret-field redaction** — JWT signing keys and HMAC peppers become non-repr, and several Vault, S3, GCS, and HTTP routing credentials become secret or redacted.

- **`forze_fastapi` — `X-Tenant-Id`/`X-Forwarded-Host` not trusted by default (breaking)** — a raw tenant header is ignored unless trust is enabled, the forwarded host is gated likewise, and Scalar docs default to not persisting auth.

- **Input/identifier hardening** — Meilisearch filter attribute names validated, PGroonga terms quoted as literal phrases, SQS rejects absolute-URL queue names on tenant-aware adapters, object-storage keys validated, and tenancy rejects invalid hints and inactive tenants.

- **Misc** — BigQuery and GCS routed clients unlink the temp service-account JSON on close, and logging can scrub error message and stack and omit stacks from JSON logs.

## [0.2.0] - 2026-05-28

### Added

- **Execution** — `OperationRegistry`/`Handler` with stage hooks, `OperationRegistry.patch()`, and `run_operation`; `ResolvedOperationPlan` drives hooks, tx scopes, and after-commit dispatch.

- **Execution context** — nested resolvers `ctx.document`, `ctx.deps`, `ctx.tx_ctx`, `ctx.authz`.

- **Tracing** — `ResolutionTracer`/`RuntimeTracer` with `DepsPlan.with_tracing()` and dev runtime tracing.

- **Composition catalogs** — `DOCUMENT_OPERATIONS` (and search, storage, authn) under `forze_kits.*.catalog`, plus plan hooks.

- **Query DSL** — literal and field filters, `$not`, array quantifiers, text patterns, aggregate groups and truncation, and `QueryFilterLimits`.

- **Document and search** — `DocumentCoordinator`, `update_matching`/`ensure`, method-specific ports (`find_page`/`find_cursor`/…), federated search, `RowLockMode`, stream methods, and `default_sort`.

- **Durable functions** — contracts under `forze.application.contracts.durable.function` plus `run_durable_function`.

- **`forze_inngest`** (`inngest` extra) — Inngest adapter with registry-backed runs and a FastAPI serve.

- **Workflow schedules** — schedule contracts and Temporal Schedules via declarative schedule bootstraps.

- **Queue delayed delivery** — `enqueue`/`enqueue_many` accept a delay or a not-before time.

- **`forze_identity`** (plus `oidc` extra) — consolidated authn, authz, tenancy, and OIDC with `AuthnOrchestrator` and `AuthzPolicyService`.

- **Analytics** — `AnalyticsSpec`/`AnalyticsQueryPort` with Postgres, ClickHouse, and BigQuery adapters.

- **`forze_firestore`, `forze_gcs`, `forze_secrets`, `forze_vault`** — document, object-storage, and secrets integrations.

- **Postgres startup validation** — Pydantic-to-column compatibility and tenancy-wiring checks on `PostgresDepsModule`.

- **Scrubbing and logging** — `forze.base.scrubbing` (`sanitize`, `configure_logging(sanitize_logs=True)`).

- **Integrations** — Redis distributed locks, Pydantic and Msgspec model codecs, and optional kit domain mixins.

### Changed

- **Breaking — execution and composition** — `Usecase`/`UsecaseRegistry` replaced by `Handler` plus `OperationRegistry`. Register with `set_handler`, compose via patch and bind methods, freeze, then resolve per operation and context.

- **Breaking — `ExecutionContext`** — the doc, dep, transaction, and call-context accessors are renamed onto the nested resolvers.

- **Breaking — document and search ports** — result shape and pagination are chosen by method name (`find_page` vs `find_cursor`); `find_many_with_cursor` is removed.

- **Breaking — query DSL** — filter literals, field compares, and grouping move to new operator keys; the top-level time-bucket key is removed.

- **Breaking — identity** — legacy `forze_authnz` consolidated into `forze_identity`. `AuthnIdentity` is principal-only, `AuthnPort` returns an `AuthnResult`, and tenant hints are validated via a resolver port.

- **Breaking — authorization** — `AuthzPort.permits(...)` removed; use `AuthzDecisionPort.authorize(...)`. Import plan helpers from the authz hooks module.

- **Breaking — durable workflows** — contracts move under `forze.application.contracts.durable.workflow` with renamed types and dep keys.

- **Breaking — errors** — `forze.base.errors` removed in favor of `forze.base.exceptions`; the HTTP error-code header defaults to `core.<kind>`.

- **Breaking — tracing** — runtime tracing renamed to `forze.application.execution.tracing`; `Deps.merge()` no longer propagates tracer flags.

- **Breaking — FastAPI** — the endpoints and HTTP-transport packages are removed; the package now ships middleware, exception handlers, OpenAPI helpers, and security resolvers only.

- **Breaking — Mongo** — the client db and collection accessors and the gateway collection accessor are async.

- **Document/search pagination** — omitting sorts no longer emits an order-by-id when the read model has no id field; configure `default_sort` or pass explicit sorts.

- **Messaging contracts** — queue, pubsub, and stream messages are frozen attrs value objects, and specs require a model codec.

- **`forze_gcs`** uses native async storage; Postgres PGroonga match and weights follow index order; Postgres and Redis get safer batched writes and atomic mset.

- **Scrubbing/console** — the log scrub mask changed and traceback frames grew; Socket.IO bind takes an operation resolver; unhandled FastAPI route exceptions return a generic JSON 500.

### Removed

- **Execution** — `Usecase`, `UsecaseRegistry`, `UsecasePlan`, the bucket module, `facade_call`, and registry graph introspection types.

- **FastAPI** — the endpoints package, the HTTP-transport package, `ForzeAPIRouter`, and attach-based route helpers.

- **Authn and identity** — the monolithic authn adapter, the header authn resolver, `OAuth2Tokens`, and principal codec ports.

- **Query/search/domain** — deprecated predicate aliases, the legacy Postgres FTS and PGroonga search adapters, and `forze.domain.mixins` (use kit mixins).

### Fixed

- **`forze_fastapi`** — the exception handlers critical-log tracebacks for unhandled exceptions, and a deliberate causeless 5xx logs at error level.

- **Errors** — error details and FastAPI context responses no longer expose raw credentials or Pydantic validation input.

- **Postgres** — batched updates cast nullable cells correctly and read-only is set before opening transactions; empty FTS queries no longer emit invalid rank SQL.

- **Redis** — script result normalization avoids rare type-check failures. S3 user-metadata decoding is fixed and default keys use a fresh UUID v7. The API-key lifecycle unpacks prefix and secret in the correct order.

## [0.1.14] - 2026-04-08

### Added

- `forze.base.logging` — structlog-based logging (structured records, a trace level, Rich/JSON renderers, request/context binding, per-namespace levels, optional dual pretty-stderr plus JSON-stdout, and a global unhandled-exception handler). Replaces the previous Loguru stack.

- `forze_fastapi` — ANSI-colored HTTP status in access logs, plus an optional unhandled-exception handler and registration for non-core exceptions.

- `forze.application.contracts.workflow` — port protocols and specs for workflow engines (start, signal, update, query, cancel, terminate).

- `forze_temporal` — Temporal integration package: deps module and lifecycle, a workflow adapter implementing the command port, and client/worker interceptors propagating context and running payload codecs.

- `forze_fastapi.middlewares.context` — an ASGI middleware to bind call and principal context and emit call-context headers.

### Changed

- `Deps` replaces `DepRouter` — spec-based routing and the router module are removed; route selection now lives on `Deps` with plain and routed registration and updated merge and removal helpers.

- `DepKey`/`DepsPort` imports moved to `forze.application.contracts.base` — the old deps package (keys, ports, router) is gone; import the base types and drop router types.

- `DepsModule` wiring — integration packages now build `Deps` through module callables with routed registration; review each package's execution deps.

- Contracts — ports, specs, and dep keys updated across domains (document, search, workflow, cache, queue, pubsub, stream, tx), including renames and new overloads; search parse helpers removed; the mapper port relocated.

- `forze_fastapi` — HTTP integration reorganized under an endpoints package with attach helpers and route features for idempotency and ETag; the custom router and routing package are removed.

- `forze.base.logging` — a new logger API (configure, get-logger, message sub vs extras); migrate code that relied on Loguru-specific helpers.

- `forze.base.logging` — OpenTelemetry-aware processors, an exception-info formatter, configurable dim keys, and level-aware Rich console styling.

- `forze_fastapi` — idempotent routes do not record idempotency when the body is invalid JSON (422), so the key can be reused after fixing the body.

- `forze_fastapi` — a batch HTTP route registrar, plus an exclude-none option on the document, http, and search attach helpers.

- `forze.application.execution` — `UsecaseRegistry.finalize` supports an in-place mode.

- `forze.application.contracts.document` and adapters — optional return-new and return-diff on create, update, touch, and batch variants.

### Removed

- `DepRouter` and the deps package — use `Deps` routing and the base module for `DepKey`/`DepsPort`.

- `TenantContextPort` and the tenant contract module.

- `ActorContextPort` and the actor contract module — caller identity is modeled via the execution context, auth identity, and the FastAPI context middleware.

- The Loguru-based implementation and the `loguru` dependency, including the old configure-prefixes, render-message, and safe-preview helpers in favor of the structlog logger.

### Fixed

- `forze_postgres`/`forze_mongo` — document deps modules register each read-write route's read and query port from that route's read config, fixing incorrect reuse of the read-only route.

- `forze_postgres`/`forze_mongo` — tenant-aware write gateways include the tenant id in update and hard-delete predicates; Postgres still raises not-found when no row matches the scoped delete.

- `forze_postgres` — the FTS search adapter reads rows from the configured source relation and uses the index only for catalog metadata; empty-query FTS uses a valid order-by.

## [0.1.13] - 2026-03-15

### Added

- `hybridmethod` descriptor in `forze.base.descriptors` for class/instance dual methods.

- `Pagination` DTO with page and size fields for list and search request payloads.

- `DocumentDTOs` with list and raw-list keys for custom list request DTO types.

- `SearchDTOs` with read, typed, and raw keys for search facade DTO configuration.

- `build_document_list_mapper` and `build_document_raw_list_mapper` in document composition.

- `build_search_typed_mapper` and `build_search_raw_mapper` in search composition.

- `LoggingMiddleware` in `forze_fastapi.middlewares` for request/response logging with scope.

- `Logger.opt` for passing options (depth, exception) to the underlying logger.

- `UVICORN_LOG_CONFIG_TEMPLATE` and `InterceptHandler` in `forze_fastapi.logging` for uvicorn log-config integration.

- Storage application layer — upload, list, download, and delete usecases plus the storage facade, DTOs, and registry builder.

### Changed

- `OperationPlan.merge`, `UsecasePlan.merge`, and `UsecaseRegistry.merge` are now hybridmethods (callable on class or instance).

- The document and search endpoint-name overrides are renamed to path overrides, and the name-overrides parameter to path-overrides.

- Document and search facades now take a DTOs object instead of a read DTO, and the registry builders require it.

- `DTOMapper` now requires the source model type in addition to the output; update existing mappers.

- `MappingStep` is now generic over the source type; custom steps should specify it.

- `CoreModel` no longer includes `Decimal` in its JSON encoders; custom Decimal serialization must be handled elsewhere.

- The list and search request DTOs extend `Pagination`, so page and size now live in the request body.

- List and search usecases take the request DTO directly instead of a TypedDict with body, page, and size.

- Postgres and Mongo document adapters — write operations now return results via the read gateway.

- Logging — scope-based contextualization, a section helper for structured spans, and `safe_preview` replacing the old argument-safety helper.

### Fixed

- Document list endpoints now correctly pass pagination to the usecase.

- Logging format — escape the extras dict to avoid a key error, and exclude a redundant logger-name field.

### Removed

- `Pagination` and the pagination parameter from `forze_fastapi.routing.params`; use the request body instead.

- `Usecase.log_parameters` and the private argument-safety helper; use `safe_preview` from logging.

- `register_uvicorn_logging_interceptor`; use the log-config template in uvicorn's `log_config`.

## [0.1.12] - 2026-03-11

### Added

- A paginated list-documents endpoint in `forze_fastapi` with typed and raw variants, the list request DTOs, and a list usecase.

- Name overrides on the document and search routers for customizing operation ids and paths.

- `attach_document_routes` and `attach_search_routes` for attaching routes to existing routers.

### Changed

- `attach_search_router` renamed to `attach_search_routes` in the search router module. Update imports.

### Fixed

- Postgres bulk update — correct table alias in the returning clause, and English error messages for consistency errors.

## [0.1.11] - 2026-03-11

### Added

- Route-level HTTP ETag support in `forze_fastapi` with an ETag provider protocol, an ETag route, and a route-class factory.

- Per-route and per-router ETag configuration (enabled, provider, auto-304).

- A document ETag provider deriving the ETag from the document id and revision without response hashing.

- ETag and `If-None-Match`/304 support on the document metadata endpoint.

- A `get()` override on the router with ETag parameters.

- A route-feature protocol and a route-class composition engine for composable route behaviors.

- ETag and idempotency as standalone route features.

- A route-features parameter on the router's route methods.

- Document update validators now run even when the update produces an empty diff.

- A model-hash helper that normalizes `Decimal` for stable hashing; `CoreModel` adds `Decimal` to its JSON encoders.

### Changed

- The router now composes idempotency, ETag, and custom route features into a single route class, replacing the single-feature override pattern.

- The validation helper's default for forbidding extra keys changed from true to false; extra keys are now ignored by default.

- `Document.touch()` now returns a new instance via a model copy instead of mutating in place.

- The Postgres document gateway raises a conflict with a revision-mismatch code when history is disabled.

- The Postgres query renderer requires array column types for array operators.

### Fixed

- The document metadata endpoint path is corrected to `/metadata`.

- Cache operations in the Postgres and Mongo document adapters are now non-fatal, so failures are suppressed and primary operations succeed.

## [0.1.10] - 2026-03-11

### Added

- An error handler for `forze_mongo` mapping PyMongo exceptions to core error subtypes.

- Optimistic retry on Mongo write-gateway write operations for concurrency errors.

- A default adaptive retry configuration for the S3 client when none is provided.

### Changed

- Replaced the DeepDiff-based dict diff with a lightweight recursive implementation (a large speedup on diff and patch).

- Removed the `deepdiff` and `mergedeep` runtime dependencies from the core package.

- Cached the middleware chain in the usecase call to avoid rebuilding closures per invocation.

- Cached signature lookups in the error-handling decorators.

- Cached module lookups in the introspection helpers.

- Cached type-adapter instances per payload type in the Socket.IO emitter.

- The document update now uses a shallow model copy for scalar-only diffs.

- The S3 storage adapter list now fetches object metadata concurrently.

- Used list-extend over augmented assignment for middleware chain construction.

- Eliminated per-call signature binding; the operation name resolves once at decoration time.

- Postgres dict-row fetch uses a dedicated row-to-dict method.

- SQS queue-name sanitization uses pre-compiled regex patterns.

- RabbitMQ ack/nack acquires the pending-messages lock once per batch.

- Cached field-name lookups and narrowed the return type to a frozenset.

- Cached Postgres type normalization in the introspection utilities.

- Pre-computed query-operator sets as module-level constants in the filter parser.

- S3 list now exits pagination early when the limit window is fully collected.

## [0.1.9] - 2026-03-10

### Added

- Socket.IO integration package `forze_socketio` with typed command-event routing, usecase dispatch through the execution context, a typed server-event emitter, ASGI and server builders, and an optional extra.

### Changed

- Contracts refactor — removed the conformity protocols and their dep variants; port protocols remain the source of truth.

- Removed `forze.base.typing`; type checking is now enforced via mypy strict mode.

## [0.1.8] - 2026-03-10

### Added

- A strict content-type parameter (default true) on the router and route methods.

- Tenant context support in the S3 storage adapter.

- An `S3Config` TypedDict for abstracting botocore configuration in `forze_s3`.

- Socket and connect timeouts on the Redis config.

- Prefix validation on the S3 storage adapter.

- A Mongo document adapter with dependency factories and CRUD/query support.

- PubSub contracts and a Redis pubsub adapter with execution wiring.

- RabbitMQ integration package `forze_rabbitmq` with queue contracts, client and adapters, an execution module and lifecycle, and test coverage.

- In-memory integration package `forze_mock` with shared-state adapters and deps for document, search, counter, cache, idempotency, storage, queue, pubsub, stream, and the tx manager.

- SQS integration package `forze_sqs` with an async client and adapters, an execution module and lifecycle, an optional extra, and LocalStack coverage.

### Changed

- Search router — split building from attachment.

- Response body chunk processing in the idempotent route (performance).

- The Postgres patch-many loop now gathers concurrently (performance).

- Postgres document write operations avoid redundant reads (performance).

- The Mongo integration now mirrors Postgres composition with dedicated read, write, and history gateways and configurable revision and history strategies.

- RabbitMQ batch enqueue now publishes via a single channel scope and queue declaration per batch (performance).

### Fixed

- Tenant-context dep resolution in the S3 storage adapter (invoke the dep as a factory).

- Read-gateway fallback on cache failure.

- Deterministic UUID generation now uses SHA-256 instead of MD5 (security).

## [0.1.7] - 2026-03-08

### Changed

- The package is now published on PyPI instead of OCI.

- `register_scalar_docs` — the version parameter is renamed, and the docs page title now uses the app title.

## [0.1.6] - 2026-03-04

Execution and mapping refactor, middleware-first usecases, split search/cache/document contracts.

### Added

- A mapping module with `DTOMapper`, mapping steps, and a mapping policy for composable async DTO mapping.

- Document plan and create-mapper builders in the document registry builder.

- Namespaced document and storage operation values.

- A creator-id field constant in the domain constants.

- A search contract with read and write ports, specs, and a parser, plus a Postgres search adapter.

- A FastAPI search router and facade dependency.

### Changed

- Document and storage operations and the document facade move out of the facades package, which is removed.

- The effect, guard, middleware, and next-call types move from the usecase module to a middleware module.

- `Deps` adopts a constructor-based API; the builder methods are removed.

- `Usecase` now requires an execution context, and guards and effects are replaced by middlewares.

- `TxUsecase` removed; transaction handling moves to a tx middleware in the plan.

- The document facade provider now requires the registry and plan.

- The create and update document usecases use the async mapper instead of sync callables; the numbered create usecase is removed in favor of a numbered create mapper.

- Search specs split into public TypedDicts and internal attrs, with per-index source and ordered groups.

- Router subclasses must set the dep key as a class attribute when using the attrs decorator.

### Fixed

- The Postgres history gateway consistency error messages are now in English.

- The Postgres search adapter uses a correct attrs mutable default for its gateway cache.

- Postgres index introspection uses a lateral unnest and simplified detection.

- The Postgres error handler covers grouping errors.

## [0.1.5] - 2026-02-28

### Added

- A `scalar-fastapi` dependency and a Scalar docs registrar in `forze_fastapi.openapi`.

- An exception-handlers module in `forze_fastapi.handlers`.

- Stable OpenAPI operation ids on all document router endpoints.

- Public exports in `forze_postgres`, `forze_redis`, and `forze_s3` for the deps modules, client dep keys, and lifecycle steps.

- An idempotency dep key in the idempotency contract.

- A route-level idempotency route and route-class factory in `forze_fastapi.routing.routes`.

- A deps module and deps plan in the execution deps module.

- From-modules and from-steps factory methods for the deps and lifecycle plans.

- A lifecycle plan and step in the execution lifecycle module.

- An `ExecutionRuntime` combining the deps plan, lifecycle, and context scope.

### Changed

- `Deps` moved from the deps contract to the execution package. Update imports.

- Postgres, Redis, and S3 restructure — the dependencies package is removed and modules move to execution with attrs-based deps modules and lifecycle steps. Replace the old module helpers with the new deps modules.

- The router's from-deps factory now accepts a deps port and returns an optional remainder.

- The doc, counter, tx-manager, and storage port resolvers consolidate into a single resolver namespace class.

- The DTO spec is renamed to the document DTO spec. Update imports.

- Document router — request body params now use a body annotation with override annotations for a correct OpenAPI schema.

- The router and document-router builder no longer accept idempotency parameters; idempotency is applied via a custom route class and resolved from the context via the idempotency dep key.

## [0.1.4] - 2026-02-27

### Added

- A configurable revision-bump strategy in `forze_postgres` (database vs application) and a configurable document factory.

- A middleware protocol and chain composition in the usecase.

- An outbox feature module with buffer middleware and a flush effect.

- A middleware factory and middleware support in the usecase plan.

### Changed

- `TxContextScopedPort` renamed to `TxScopedPort` (the context requirement is removed). Update imports.

- The tx-scope-match decorator is removed; tx-scope validation is now handled by the execution context when resolving dependencies.

- The Postgres document adapter no longer requires the context; it uses the tx-scoped port instead.

### Fixed

- Duplicate guards, middlewares, and effects are now deduplicated by priority when merging usecase-plan operations.

## [0.1.3] - 2026-02-27

### Added

- A filter query DSL in `forze.application.dsl.query` — AST nodes, parser, and value coercion.

- A Mongo query renderer for compiling filter expressions to MongoDB queries.

- A buffer primitive in `forze.base.primitives`.

### Changed

- Application-layer restructure — the kernel splits into a contracts package (ports, specs, deps, schemas) and an execution package (context, usecase, plan, registry, resolvers). Update imports.

- Contracts flattening — top-level re-exports, with internal modules moved to underscored sub-packages.

- Tx contracts rename — the tx-manager port and related contracts move from the tx-manager module to the tx module. Update imports.

- Postgres filter builder — replaced the old builder with the DSL-based query renderer; the old builder is removed.

## [0.1.2] - 2026-02-26

### Added

- `forze.base.typing` with protocol conformance helpers.

- Domain document support in `forze.domain` built from the domain document model with name, number, and soft-deletion mixins and update-validator infrastructure.

- A document kernel in `forze.application.kernel` — pluggable usecase plans, a document facade factory, the document port with explicit read and write ports, and a document operation enum.

- An optional FastAPI integration package with routing helpers, idempotent POST support, and a prebuilt document router.

- Optional provider packages `forze_postgres`, `forze_redis`, `forze_s3`, `forze_temporal`, and `forze_mongo` with platform clients, gateways and adapters, and dependency keys.

### Changed

- Kernel — transaction handling and dependency resolution refactored around the execution context and kernel deps; the tx-manager and app-runtime ports are removed from the kernel ports.

- Postgres filter builder — filter input accepts only canonical operator names; aliases such as `==`, `ge`, and `in_` are no longer accepted and raise a validation error.

- Infrastructure previously under `forze.infra` moved into optional packages; core `forze` no longer ships Postgres, Redis, S3, or Temporal implementations.

### Fixed

- Correct UUIDv7 datetime conversion in `forze.base.primitives.uuid` so round-trips preserve timestamp semantics.

## [0.1.1] - 2026-02-23

### Added

- Initial DDD/Hex contracts — ports, results, errors.

### Fixed

- Packaging metadata for PyOCI classifiers.

[unreleased]: https://github.com/morzecrew/forze/compare/v0.17.0...HEAD
[0.17.0]: https://github.com/morzecrew/forze/compare/v0.16.2...v0.17.0
[0.16.2]: https://github.com/morzecrew/forze/compare/v0.16.1...v0.16.2
[0.16.1]: https://github.com/morzecrew/forze/compare/v0.16.0...v0.16.1
[0.16.0]: https://github.com/morzecrew/forze/compare/v0.15.1...v0.16.0
[0.15.1]: https://github.com/morzecrew/forze/compare/v0.15.0...v0.15.1
[0.15.0]: https://github.com/morzecrew/forze/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/morzecrew/forze/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/morzecrew/forze/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/morzecrew/forze/compare/v0.11.1...v0.12.0
[0.11.1]: https://github.com/morzecrew/forze/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/morzecrew/forze/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/morzecrew/forze/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/morzecrew/forze/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/morzecrew/forze/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/morzecrew/forze/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/morzecrew/forze/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/morzecrew/forze/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/morzecrew/forze/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/morzecrew/forze/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/morzecrew/forze/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/morzecrew/forze/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/morzecrew/forze/compare/v0.1.14...v0.2.0
[0.1.14]: https://github.com/morzecrew/forze/compare/v0.1.13...v0.1.14
[0.1.13]: https://github.com/morzecrew/forze/compare/v0.1.12...v0.1.13
[0.1.12]: https://github.com/morzecrew/forze/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.com/morzecrew/forze/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.com/morzecrew/forze/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.com/morzecrew/forze/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/morzecrew/forze/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/morzecrew/forze/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/morzecrew/forze/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/morzecrew/forze/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/morzecrew/forze/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/morzecrew/forze/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/morzecrew/forze/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/morzecrew/forze/releases/tag/v0.1.1
