# RFC 0011 — Mongo transfer leg: second-backend breadth for the misuse-corpus transfer run

- **Status:** 📝 Draft — **deliberately parked.** This RFC exists so the work is specified, not because it should run. Its pre-registered trigger (inherited from the fidelity-transfer experiment) has not fired: the PG transfer run produced zero divergence at every isolation level, so a second backend currently answers no open question. Execute only when a trigger below fires, or as an explicitly-decided breadth purchase.
- **Scope:** run the misuse corpus's transferable mutants against real MongoDB the way the fidelity-transfer experiment ran them against real Postgres — a `tests/integration/test_forze_mongo/test_mongo_misuse_transfer.py` harness over the existing backend-agnostic transfer seam (`forze_dst.conformance.transfer`), a second verdict column on the Fidelity page, and `ground_truth` confirmation per mutant on a second engine. No framework work: the seam, scripts, and renderer are already backend-parameterized; this is harness + rows.
- **Related:** the fidelity-transfer experiment (which this extends; it explicitly deferred Mongo as "v2: the SI-applicable subset" and rejected breadth-for-its-own-sake, and its landing note records the trigger as not met); the DST fidelity matrix (already covers Mongo at battery level — mock≡PG≡Mongo on the 13-case Adya set, which is the *phenomenon-level* Mongo evidence we do have); the unsimulatable-invariant detector (orthogonal — invariant accounting does not touch the mock↔real plane).

**Framing.** The transfer experiment's unit of evidence is a *found bug on a real engine*. PG established that for all 19 transferable mutants. A Mongo leg re-asks the same question against an engine with a different concurrency model (document-level atomicity, WiredTiger snapshot semantics, no server-side serializable) — which is only informative where the mutant's manifestation touches that model. Per the transfer experiment's domain-bound finding, roughly half the transferable corpus manifests through no isolation phenomenon at all; for those mutants a Mongo run is pure re-confirmation. The honest posture is therefore: specify the leg, name its triggers, and run it when a trigger fires — not before.

---

## 1. Triggers (any one un-parks this RFC)

1. **A PG transfer divergence appears.** Any future corpus growth or code change breaking the current 37/37 zero-divergence state — the moment mock↔real disagreement exists, a second engine tells us whether it is engine-specific or mock-structural.
2. **An SI-sensitive mutant lands.** A new corpus instance whose manifestation depends on snapshot-isolation semantics where Mongo and PG plausibly differ (e.g. multi-document transaction write conflicts, snapshot read staleness across documents) — then the two engines are two *different* questions, not one question twice.
3. **The predictor analysis leaves its degenerate branch.** The transfer experiment's Fisher table gains a divergent cell on either plane — cross-backend rows become the natural next discriminator.
4. **A deliberate breadth decision.** "The Fidelity page should say two engines" is a legitimate product call; it just must be made as one (rows, not new questions — the cost is real and the claim gain is presentational).

## 2. Design (all decided, nothing open)

- **Harness:** mirror `test_pg_misuse_transfer.py` — testcontainers Mongo, collections mirroring the PG tables (event_log, catalog_rows, receipts, submissions, oncall, …), the same 37 `TransferScript`s driven through `run_transfer` with a Mongo-backed deps module. The scripts are backend-agnostic by construction (Conductor weaves + port calls); only the fixture layer is new.
- **Subset rule (from the transfer experiment's v2 sketch):** the SI-applicable subset runs with real teeth; mutants whose manifestation is isolation-free still *run* (cheap, and absence of engine-specifics is itself recorded) but their rows are labeled as confirmation-only in the rendered table — the claim scoping must not inflate.
- **Level mapping:** Mongo has no per-transaction isolation ladder; scripts that declare levels run at Mongo's native snapshot semantics and the record notes the mapping (the fidelity matrix's existing Mongo column already established this vocabulary).
- **Artifacts:** `just dst-transfer` grows a `--backend mongo` leg (or a `dst-transfer-mongo` recipe) regenerating a second verdict table on the Fidelity page; divergences go through the same reviewed-catalog discipline — unexplained = failure.
- **`ground_truth` semantics:** stays REAL once *any* real engine manifests the bug (PG already did); the Mongo column is per-backend manifestation data, not a second gate on the enum.

## 3. Non-goals

- Graph/other backends (the transfer experiment deferred them; nothing has changed).
- Battery-level Mongo work — the DST fidelity matrix already covers it; this RFC is corpus-level only.
- Any framework/seam change. If the leg needs one, that is a finding, not scope.

## 4. Cost estimate

One focused session: the collection fixture layer (the PG harness took the same shape), a recipe, one docs table regen. No statistical protocol — transfer is verdict-per-script, not campaigns.

## 5. Decisions

| # | Decision |
| --- | --- |
| 1 | **Parked, not rejected.** The pre-registered trigger inherited from the fidelity-transfer experiment has not fired — the Postgres run produced zero divergence at every isolation level — so a second backend currently answers no open question. Executing it anyway would buy breadth, not evidence |
| 2 | Harness only. The transfer seam, scripts and renderer are already backend-parameterized; if the leg turns out to need a framework change, that is a **finding**, not scope creep |
| 3 | Subset rule: mutants whose manifestation touches no isolation phenomenon still run (they are cheap, and their absence of engine-specifics is itself a recorded fact) but their rows are labeled **confirmation-only**, so the claim scoping cannot inflate |
| 4 | Mongo has no per-transaction isolation ladder: scripts declaring a level run at Mongo's native snapshot semantics and the record notes the mapping, reusing the fidelity matrix's existing Mongo vocabulary |
| 5 | `ground_truth` stays `REAL` once *any* real engine manifests the bug; the Mongo column is per-backend manifestation data, never a second gate on the enum |
| 6 | Divergences go through the same reviewed-catalog discipline as every other differential — an unexplained divergence is a failure, not a new catalog row |
