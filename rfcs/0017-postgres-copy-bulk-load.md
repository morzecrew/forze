# RFC 0017 — Postgres `COPY` bulk load

- **Status:** 🚧 **P1–P3 landed 2026-08-12**; P4 blocked on its pair. W1 (`PostgresClientPort.copy_rows`, text + binary) and W2 (analytics ingest over `COPY`, cap re-rationalized, Postgres default 10k → 100k) ship with the full acceptance battery against a real server. **P4 (W4, BigQuery load jobs) is not started and cannot be: §2 requires it to land with RFC 0030 P2 as that publish's stage step, and 0030 is still Draft — building it now would produce a surface with no consumer, which is the outcome its own demand gate exists to prevent.**

  **Battery 7 — measured, interleaved same-runner rounds, median of per-round minima** ([tests/perf/test_forze_postgres/test_pg_perf_copy_rows.py](tests/perf/test_forze_postgres/test_pg_perf_copy_rows.py)). Decision 7 made W2 conditional on this; it confirms:

  | rows × cols | `COPY` | multi-VALUES | `execute_many` |
  |---|---|---|---|
  | 10⁴ × 6 | **8.0 ms** | 207.9 ms (26.1×) | 633.2 ms (79.4×) |
  | 10⁴ × 20 | **22.1 ms** | 740.5 ms (33.5×) | 714.6 ms (32.4×) |
  | 10⁵ × 6 | **76.5 ms** | 1955.1 ms (25.5×) | 5835.1 ms (76.2×) |
  | 10⁵ × 20 | **277.9 ms** | 5724.4 ms (20.6×) | 5631.9 ms (20.3×) |

  **Two things the design did not anticipate, both found by running against the server rather than reading the driver.** *(a)* A binary-mode type mismatch does not arrive as a data error at all: wrong declared types derail the fixed-width framing, so the server reports `ProtocolViolation` (SQLSTATE 08P01, "insufficient data left in message") — an `OperationalError`, which the generic arm mapped to a **retryable** class, so a caller would have retried a load that could only fail identically. `copy_type_mismatch` is now keyed off the COPY line context rather than off the exception family. *(b)* Text and binary want **opposite Python types for `json`/`jsonb`** — text parses a `str` and rejects a mapping; binary wants the mapping and dumps a `str` as a *quoted JSON string*, no error, wrong data. Flipping `binary=True` for speed therefore silently changed what landed in the column, so a `str` for a declared json column is refused with the fix named. Decision 3 said mismatches fail loud; this was the one that did not.

  Also fixed while building it: the argument-binding exception decorator attaches every bound argument to the error, which for this method is the entire dataset — a failed 10⁶-row load would have put 10⁶ rows of user data into an exception and from there into the logs. `copy_rows` maps its own failures with details that name the target and the failing line, never a value.
- **Scope:** Give the Postgres integration its engine-native bulk-load path. Two tiers, matching where loads actually happen: **(W1)** a `copy_rows` surface on the kernel `PostgresClientPort` — safe-identifier, bounded-memory, async-iterable rows — for the raw-client zone (runtime-created tables, pipeline executors); **(W2)** the analytics ingest adapter switches its execution from one multi-VALUES `INSERT` to `COPY … FROM STDIN`, contract unchanged, which dissolves the parameter-limit rationale behind the 10k row cap. A streaming contract addition (`append_stream`) is recorded demand-gated, not built. Postgres only: ClickHouse already batches columnar-natively; BigQuery load jobs are a different mechanism, recorded with a trigger.
- **Related:** The gap analysis (2026-07-31) and RFC 0015 — together they cover the runtime-read and runtime-write halves of the same product story; this RFC is the write half's throughput. The db-client concurrency model (single connection in flight; pool outside) governs the copy's connection tenure. The adapter-testing rules from the UUID/Decimal write-gap and JSON-boundary incidents shape the battery: rich types, real backend, always.
- **Origin:** Loading cleaned CSV-scale datasets (10⁵–10⁶ rows) into per-project Gold schemas. Today's options: `execute_many` (psycopg pipelined, still per-row protocol overhead) or the analytics adapter's single multi-VALUES `INSERT`, capped at `max_append_rows=10_000` — a cap that exists because the extended protocol tops out at 65 535 bind parameters, so ~6 columns × 10k rows is already near the ceiling. `COPY` has no parameter ceiling and is the engine's designed bulk path. It is reachable today via `bound_connection()` — which means every app hand-rolls identifier quoting, chunking, and error handling at the exact spot where a mistake writes garbage at scale.

---

## 1. W1 — kernel client surface

```python
class PostgresClientPort(Protocol):
    ...
    def copy_rows(
        self,
        target: tuple[str, str],            # (schema, table) — composed via psycopg.sql, never formatted
        columns: Sequence[str],             # composed identifiers, same rule
        rows: Iterable[Sequence[Any]] | AsyncIterable[Sequence[Any]],
        *,
        binary: bool = False,
        column_types: Sequence[str] | None = None,  # feeds Copy.set_types when binary
    ) -> Awaitable[int]: ...                # server-reported row count
```

- **Identifier safety is the point, not a nicety.** The one thing `bound_connection()` hand-rolls worst is quoting a runtime `(schema, table)` into the `COPY` statement. `copy_rows` composes `target` and `columns` through `psycopg.sql.Identifier` — the same discipline the tenant provisioner already uses for `CREATE SCHEMA` — so the raw tier's most dangerous string never gets f-formatted in app code again.
- **Bounded memory by construction.** `rows` may be an async iterator; the adapter drives `Copy.write_row` row-by-row and never materializes the input. A pipeline can stream decode→transform→copy without holding the dataset.
- **Text format default, binary opt-in.** Text-format `COPY` lets psycopg adapt and the server cast — forgiving on `int4`-vs-`int8`-grade mismatches, which is the right default for runtime-created tables whose exact column types the caller may not own. `binary=True` (+ `column_types` → `set_types`) is the opt-in fast path for callers who control both sides; a type mismatch there fails loud with a mapped error, never a coerced value.
- **All-or-nothing, loudly.** A bad row aborts the entire `COPY` — that is engine behavior and it is kept, not papered over: no skip-bad-rows mode, ever (silently dropping rows is the ETL sin the rest of the framework exists to prevent). The one DX obligation: Postgres reports `CONTEXT: COPY t, line N` on data errors, and the mapped error **must surface the line number and column** — at 10⁶ rows, "invalid input syntax" without a line is a debugging session; with one it's a `sed -n` away.
- **Composes with the client's existing semantics.** Inside `transaction()` the copy joins the caller's transaction (rollback removes every row — battery-pinned); standalone it follows the client's autocommit convention. `statement_timeout` applies to `COPY` and the interaction is pinned, not assumed. The copy holds its connection for the full duration — under the single-connection rule that means a long copy serializes behind/ahead of neighbors exactly like any long statement; noted in the docstring, no new machinery.
- Error taxonomy: `copy_row_invalid` (validation — data error, carries line/column context), `copy_type_mismatch` (validation — binary-mode dumper/column disagreement), existing timeout/serialization mappings reused.

Deliberately absent: `COPY TO` export (portability exports via `find_stream` → JSONL by design; no consumer), `FREEZE` (requires create-in-same-txn choreography; niche), CSV-file passthrough (`copy_rows` takes rows, not files — parsing stays the caller's concern, where the dialect knowledge lives).

## 2. W2 — analytics ingest rides `COPY`, contract untouched

`AnalyticsIngestPort.append(rows)` keeps its exact contract — typed rows, codec-encoded, field-encryption applied, all-or-nothing, `AnalyticsAppendResult` — and the Postgres adapter swaps its execution from the multi-VALUES `INSERT` to `COPY` over the same encoded output (encode via the existing codec first, then `write_row` the encoded mappings; sealed columns therefore carry their envelope bytes through `COPY` unchanged — battery-pinned).

The cap's meaning changes and its docstring must say so: with the 65 535-parameter ceiling gone, `max_append_rows` is no longer a protocol necessity but a **per-call latency/memory guard** — it stays (a governed route should still refuse a surprise 10⁷-row call), its default can rise (proposed: 100k), and raising it per route is now an honest tuning knob instead of a protocol cliff. Mock and every other engine's adapter are untouched — this is an execution-strategy change behind an unchanged port, so the existing ingest conformance suite and the mock ≡ PG differential are the regression harness, run as-is.

**W3 (recorded, demand-gated):** `append_stream(rows: AsyncIterator[Sequence[Ing]])` as an additive, capability-gated contract method (`supports_stream_ingest`, fail-closed) for loads that shouldn't materialize — the `find_stream`/`predict_stream` twin on the write side. Gated on a named consumer with a registered-route bulk ingest; Linecust's loads target runtime tables (W1 territory), so no consumer exists today.

**W4 (2026-08-03 — the recorded BigQuery trigger has fired):** BigQuery load jobs (GCS→BQ file loads). Previously listed in §3 with the trigger *"a consumer whose BQ ingest volume makes streaming-insert pricing or quotas bite"*; an upcoming DWH-shaped consumer (the RFC 0028–0030 family's origin) is exactly that, so the item is promoted from out-of-scope to a workstream — still demand-gated on the consumer being confirmed, not on the shape being re-argued.

Why it stays a different mechanism, and what that means for the design: the current BigQuery ingest path is `insertAll` streaming inserts capped at `max_append_rows=10_000`, which for warehouse-scale loads is the wrong tool three ways — per-row pricing, quota pressure, and a streaming buffer that interferes with the DML a transform then wants to run against the same table. A load job is file-based and asynchronous (stage to GCS → submit → poll), so it is shaped like RFC 0004's batch plane rather than like `append`, and the honest surface follows that shape:

- **Not** hidden behind `AnalyticsIngestPort.append` — a method whose contract is "rows accepted when I return" cannot silently become a job submission with a poll loop. The plane's existing capability discipline applies: a separate, capability-gated entry point whose asynchrony is visible.
- Reuses the storage plane for staging (`forze_gcs`), so the file-writing half is not reinvented — the same "pass by key, not path" posture RFC 0021 adopted.
- `WRITE_TRUNCATE` load into a staging relation is BigQuery's atomic single-relation replace, which makes this the natural **stage step** for an RFC 0030 publish rather than a competing ingest path. W4 and 0030 P2 should land together or W4 has no good consumer.
- Not built here: streaming-buffer interaction rules beyond documenting them, and the Storage Write API (a third mechanism again — recorded, no trigger).

## 3. Out of scope, with reasons on record

- **ClickHouse** — `insert_rows` already chunks columnar-native inserts (`insert_batch_size`); that *is* its engine-native bulk path. Its route cap follows W2's re-rationale (guard, not protocol limit) as a docstring fix, nothing more.
- **A new contract-level `BulkLoadPort`** — foreclosed. The governed bulk surface already exists (`AnalyticsIngestPort`); the ungoverned one already exists (client port). A third plane between them would be indirection without a guarantee to its name.

## 4. Acceptance battery (real PG throughout — "test adapters with the real backend" is the standing rule this plane was born from)

1. **Rich-type round-trip at 10⁵ rows**: UUID, `Decimal`, tz-aware `datetime`, `None`/NULL, `dict`→`jsonb`, `bool`, `str` with `\t`/`\n`/backslash content (text-format escaping is exactly where hand-rolled COPY breaks), via both text and binary modes. The UUID/Decimal write-gap and Decimal-union incidents are the reason this item leads.
2. **Failing row mid-load**: row *N* invalid → error carries line context; transaction rolled back; table contains **zero** new rows (all-or-nothing pinned, no partial load observable).
3. **Caller-transaction composition**: copy inside `transaction()` + rollback → no rows; commit → all rows.
4. **`statement_timeout` fires mid-copy** → mapped timeout, connection healthy and reusable afterward.
5. **Binary mismatch**: `column_types` disagreeing with the table fails loud with `copy_type_mismatch`, zero rows.
6. **W2 conformance unchanged**: full existing analytics-ingest suite (incl. field-encrypted columns and the mock ≡ PG differential) green over the COPY execution path with no test edits — the strongest available proof the contract didn't move.
7. **Perf evidence, measured not asserted** (perf-gate methodology: same-runner, interleaved A/B): `COPY` vs multi-VALUES vs `execute_many` at 10⁴ and 10⁵ rows × {6, 20} columns; numbers recorded in this RFC's status block. Expectation to confirm, not assume: COPY wins by multiples at 10⁵; if it doesn't, W2 doesn't ship on vibes.
8. **Identifier hostility**: schema/table/column names containing quotes/spaces/keywords round-trip via composition (the reason `target` is a tuple, pinned).

## 5. Phases

- **P1 — W1 `copy_rows` (text mode) + taxonomy + battery 1–4, 8. ✅ Landed 2026-08-12.**
- **P2 — binary mode + `column_types` (battery 5); perf evidence (battery 7) recorded. ✅ Landed 2026-08-12.**
- **P3 — W2 ingest switch + cap re-rationale + battery 6; ClickHouse cap docstring alignment. ✅ Landed 2026-08-12.** Battery 6 ran unedited, which is the proof the contract did not move; the two column shapes the switch could plausibly break — a field-encrypted column and a `jsonb` column — had **no test** despite §2 calling both "battery-pinned", so they were written. Both also pass against the old `INSERT` execution, which is what makes them a contract test rather than a description of `COPY`.
- **P4 — W4 BigQuery load jobs. ⛔ Blocked, not started.** §2 pairs it with RFC 0030 P2 ("W4 and 0030 P2 should land together or W4 has no good consumer") and 0030 is still Draft, as are 0028/0029 ahead of it. Building the load-job surface now would ship exactly the consumer-less mechanism its own demand gate was written to prevent. Battery item to add when it unblocks: a 10⁶-row GCS→BQ load lands atomically into a staging relation, rich types intact, job failure surfaced with the job's own error rather than a generic mapping.

## 6. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Two tiers, no third plane: kernel `copy_rows` for the raw zone, `COPY` behind the unchanged `AnalyticsIngestPort` for the governed zone; `BulkLoadPort` foreclosed | locked |
| 2 | `target`/`columns` composed via `psycopg.sql` — string-formatted identifiers never appear in the surface or its callers | locked |
| 3 | Text format default; binary opt-in with explicit `column_types`; mismatches fail loud. **Amended 2026-08-12:** "fail loud" needed enforcing, not just declaring — binary silently stored JSON *text* as a quoted string, so a `str` for a declared json column is refused | locked |
| 4 | All-or-nothing preserved; no skip-bad-rows mode ever; data errors must surface `COPY` line context | locked |
| 5 | `max_append_rows` re-rationalized as a latency/memory guard (Postgres default raised 10k → 100k), not removed — a governed route still refuses surprise mega-calls. BigQuery's 10k is untouched: there it *is* a wire limit (`insertAll`), not a guard | locked 2026-08-12 |
| 6 | `append_stream` contract addition demand-gated (no current consumer); `COPY TO` export not built (portability uses JSONL by design) | locked |
| 7 | W2 ships only if the interleaved A/B evidence confirms the win — perf claims are measured, never assumed. **Satisfied 2026-08-12: 20–79× across the grid** (status block) | locked |
| 8 | **W4 promoted 2026-08-03** — the BigQuery-load-job trigger fired (DWH consumer, RFC 0028–0030). Load jobs get their own capability-gated, visibly-asynchronous surface; never hidden behind `append`; staged via the storage plane; paired with 0030 P2 as its stage step. Storage Write API recorded, no trigger | proposed |
