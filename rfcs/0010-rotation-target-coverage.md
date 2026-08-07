# RFC 0010 — Rotation-target coverage: closing the credential-rotation gap across backends

- **Status:** 🚧 In progress — **P1 and P4 executed (2026-07-26)**, P2/P3 remain demand-gated as the rollout intends. Shipped: the shared conformance battery (`tests/support/rotation_targets.py`), its Postgres instantiation (replacing the hand-rolled tests), `MongoRotationTarget` with 17 unit tests and the battery green against a live server, `MongoClientPort.command_dispatch_bound`, a Mongo URI credential-swap helper, and the per-backend doctrine table in the credential-rotation docs. **The P1 bound gate was answered empirically before any adapter code was written**: `maxTimeMS` *is* accepted on `updateUser`, the server kills a command that outruns it (`ExecutionTimeout`, code 50), and the killed write does not land — so MongoDB is admissible under §4's backend-enforced rule. Two findings the battery forced, neither guessable: an `updateUser` carrying `pwd` **recomputes the user's SCRAM mechanism set back to the server default** (so a narrowed mechanism list cannot be used to build an unauthenticatable principal, and does not survive a rotation), while `authenticationRestrictions` *do* survive it and are MongoDB's true NOLOGIN analogue. The battery is 6 parameterized checks plus a verify-gate case each backend provisions itself; case 6 gained a **positive control** (the same minimal bound must succeed once the stall is lifted) without which it would pass against a target whose apply is simply broken.
- **Scope:** `RotationTargetPort` is a backend-agnostic contract consumed by a generic durable rotator, and only Postgres and Mongo implement it. This RFC (1) triages every credential-holding integration into one of five rotation doctrines — **bespoke target**, **leases**, **platform IAM / ambient identity**, **operator-rotated**, or **counterparty-rotated** (owned by the rotating credential store) — so "no target" is always a decision, never an omission; (2) defines a **shared rotation-target conformance battery** every future target must pass against its live backend, including the apply-latency-bound obligations the secrets-lifecycle hardening settled; (3) phases the targets that earn one. No contract changes: the port (`compose`/`apply`/`verify`) already fits every viable backend shape.
- **Related:** The plane being extended — the secrets lifecycle plane (rotator kit, pending-ref staging, verify-before-promote, `PostgresRotationTarget` dual-user alternation, `SecretsLeaseManager`). Decision #14 there (leases *replace* rotation per backend) and decision #12 (dual-user default; MySQL dual-password noted as mapping onto the same port). The secrets-lifecycle review hardening (see §4) fixed the rotator/target safety split — every obligation it placed on the Postgres target transfers verbatim to every target this RFC plans. Conformance precedent — the adapter-conformance harness ("verify the mock") and the audit discipline of enumerating the isomorphic second axis rather than proving one backend and assuming the rest. Out of scope but adjacent — the rotating credential store (counterparty-rotated credentials: ones a *third party* rotates at us, the inverse of this RFC's direction).
- **Origin:** Review question on the secrets-lifecycle branch (2026-07-26): "Is it true that it's not conformal across backends? i.e. we need to support the same for all other databases we have — ClickHouse, MongoDB and so on?" The contract is conformal; the *coverage* is not, and nothing today records which backends are missing a target on purpose.

---

## 1. The gap, stated precisely

The rotator workflow is backend-agnostic: it mints, stages at `<path>.pending`, and promotes through the secrets store; everything backend-shaped lives behind `RotationTargetPort` (`compose`/`apply`/`verify`). Postgres and Mongo ship the only implementations (Mongo landed with this RFC's P1). Every other integration that resolves a long-lived credential through `SecretsPort` — ClickHouse, Redis, RabbitMQ, Neo4j, Kafka, Meilisearch, the cloud storage/queue/analytics backends — currently rotates by `fingerprint_ttl` floor plus out-of-band ops: an operator changes the credential in the backend and writes the new secret by hand, with no verify gate and no overlap discipline.

That is not automatically wrong — for some backends a bespoke target is the wrong tool — but today the difference between "deliberately lease-based" and "nobody built it" is invisible. This RFC makes the doctrine explicit per backend.

## 2. The five doctrines

1. **Bespoke target** — the backend holds named principals with settable passwords (or mintable keys) and the framework's client can administer them. The Postgres pattern transfers: alternate between two equally-privileged principals (or mint-new/delete-old for key-based stores), verify with a real authenticated call, promote, let the previous credential drain.
2. **Leases** — a Vault database-engine (or equivalent) plugin exists: short-TTL per-issuance credentials *are* the rotation (the secrets-lifecycle decision that leases replace rotation per backend). Building a target for such a backend duplicates a better mechanism.
3. **Platform IAM / ambient identity** — the credential is a cloud IAM artifact (AWS access keys, GCP service-account keys). The strongest posture is *no static secret at all* (IRSA, workload identity, metadata server); where static keys persist, rotation is an IAM-API operation, not a data-plane one. AWS's two-active-keys-per-user model maps 1:1 onto `compose`/`apply`/`verify` if demand ever justifies a target.
4. **Operator-rotated** — the credential is minted in a vendor dashboard with no usable admin API from our client (SaaS API keys). The framework's job ends at hot reload: the operator writes the new secret via `SecretsAdminPort`, the change feed propagates it.
5. **Counterparty-rotated** — the *third party* rotates the credential at us as a side effect of use (single-use OAuth refresh tokens: each exchange burns the old token and hands back a new one). This is the inverse of every doctrine above — we neither schedule nor perform the rotation, we must *survive* it: persist-rotated-before-use, per-tenant serialization of the exchange, a burn-notice terminal state. No `RotationTargetPort` shape fits (there is nothing for us to `apply`), so this doctrine is owned by the **`RotatingCredentialStore`** plane, which composes the secrets-lifecycle primitives (versioned reads, CAS admin writes, the per-ref dlock, the change feed) rather than duplicating them. This RFC records the doctrine so the taxonomy is total; it deliberately plans none of the work.

## 3. Backend triage

| Integration | Credential shape | Auth semantics | Doctrine | Notes |
|---|---|---|---|---|
| `forze_postgres` | DSN, role password | connect-time; established conns survive | **target (shipped)** | dual-user default; single-role degraded opt-in |
| `forze_mongo` | URI, user password | connect-time; password change spares live sessions | **target (shipped)** or leases | dual-user via `updateUser`; Vault mongodb engine also exists — deployment picks |
| `forze_clickhouse` | user/password | **per-request (HTTP)** — every request re-authenticates | **target (P2)** | no Vault plugin; per-request auth makes the overlap window *more* critical: single-user mode breaks in-flight traffic instantly, so dual-user is effectively mandatory |
| `forze_redis` | ACL user/password | AUTH at connect; `ACL DELUSER` kills that user's connections | leases (Vault redis engine) or **target (P2)** | alternation works; revoke-kills-connections must be documented like the lease plane's hard edge |
| `forze_rabbitmq` | user/password | connect-time (AMQP handshake) | leases (Vault rabbitmq engine) or **target (P2)** | dual-user + per-vhost permission mirroring |
| `forze_neo4j` | user/password | connect-time (bolt) | **target (P3)** | role-mirrored dual users need Enterprise RBAC; Community is effectively single-user → degraded mode only |
| `forze_kafka` | SCRAM credential | handshake | **target (P3)** | `alterUserScramCredentials` via admin API; two principals need mirrored ACLs |
| `forze_meilisearch` | API key | per-request | **target (P3, key-mint variant)** | keys are mintable objects: `compose` returns a freshly minted key, delayed revoke deletes the old one — no alternation needed |
| `forze_s3` / `forze_sqs` (AWS) | access-key pair | per-request (SigV4) | **platform IAM** | prefer ambient identity; AWS's native two-key model is a ready-made alternation if a target is ever demanded |
| `forze_gcs` / `forze_bigquery` / `forze_firestore` | service-account key | per-request | **platform IAM** | prefer workload identity; SA keys support multiple concurrent keys |
| `forze_temporal` / `forze_inngest` / `forze_http` | API key / mTLS / bearer | per-request or handshake | **operator-rotated** | hot reload is the deliverable; a target only if a mint API is wired |
| third-party OAuth grants (ride over `forze_http`) | single-use refresh token + access token | per-request bearer; refresh **burns** the old token | **counterparty-rotated → `RotatingCredentialStore`** | the provider rotates at us; the deliverable is crash-consistent survival, not a target |
| `forze_duckdb` | none / lake creds | — | n/a → platform IAM | embedded engine; lake credentials are the cloud-IAM case |
| `forze_vault` | its own token | — | out of scope | the store's own credential; token renewal already handled by the client |

Two cross-cutting facts the table encodes, which each target's docs must restate on its own axis (the enumerate-the-isomorphic-axis discipline):

- **Connect-time vs per-request auth.** The connect-time re-resolution doctrine (established connections survive; signals only accelerate draining) holds for Postgres/Mongo/RabbitMQ/Neo4j/Kafka. It does **not** hold for per-request backends (ClickHouse HTTP, Meilisearch, SigV4): there, the overlap window is the *only* protection, and degraded single-principal modes should be refused rather than merely documented where feasible.
- **Revocation blast radius.** Some backends kill live connections on principal deletion (Redis `ACL DELUSER`) — the delayed-revoke grace is load-bearing there, exactly like the lease plane's hard edge.

## 4. What the secrets-lifecycle hardening proved — obligations every target inherits

The review hardening of the secrets-lifecycle branch (fourteen adversarial rounds against the rotator and the Postgres target) settled the **rotator/target safety split**, and the split is what makes this RFC cheap per backend:

**The rotator owns, generically — every target gets these for free:**

- the per-ref distributed lock with heartbeat, plus **ownership probes** (`DistributedLockCommandPort.reset`) immediately before every backend write, so a known-lost lock aborts before the write, not at scope exit;
- the promote fences — staging-version fence, CAS promote (`SecretsAdminPort.put(expected_version=...)`), post-promote confirm-and-converge;
- the delayed-reconfirmation chain with the **certificate invariant**: a confirm round may complete only at the fixpoint (verify succeeded at a version re-read afterwards and found still canonical); every other exit keeps the chain alive and raises.

**A target owns exactly three things, and only the first two are safety-critical:**

1. **A truthful `apply_latency_bound`.** The delayed-reconfirmation window is validated against this bound at wiring (`reconfirm_after` must strictly exceed it, fail-closed), so the bound must cover a stale apply's *whole possible lifetime* — enumerate every queue the operation can sit in before the backend-side clock starts (pool checkout, connection establishment, client-side retry), then add the backend-enforced execution bound. Postgres: `pool_checkout_allowance + apply_statement_timeout`. Three sub-rules, each one a fixed review finding:
   - **The bound must be backend-enforced.** Only a server-side kill (`SET LOCAL statement_timeout`, `maxTimeMS`, `max_execution_time`) makes "hasn't committed within the bound" mean "never will". Client-side cancellation is *not* a bound — abandoning a request does not stop a write already at the server. A backend that cannot server-side-bound a late write (see the table below) cannot honestly declare one, which is itself an argument for its leases doctrine.
   - **No opt-out.** The bound-carrying knob is required-positive; an optional safety knob whose `None` case bypasses its own wiring validation is two bugs, not one.
   - **Client-side components validate against configured truth.** A checkout allowance is checked against the client's *exposed* acquire timeout (not an independent estimate), at target construction *and* re-checked at apply time (initialization can finish after construction). Understating is a configuration error, not a doc caveat.
2. **A verify that authenticates as the pending principal for real** — a live connection/request as that principal, not a metadata read.
3. `compose` mechanics (alternation or key-mint) — correctness, not safety.

Per-candidate bound mechanisms (to be confirmed in each phase, not assumed):

| Backend | Backend-enforced apply bound | Status |
|---|---|---|
| Postgres | `SET LOCAL statement_timeout` in a detached root transaction | **shipped** |
| MongoDB | `maxTimeMS` on the `updateUser` command | **shipped** — verified against a live server: the command is killed (`ExecutionTimeout`) and the write does not land |
| ClickHouse | `max_execution_time` query setting | to verify in P2 |
| Redis | **none** — a stale `ACL SETUSER` sitting in a socket/replication buffer executes whenever the server reads it, with no server-side age check | strengthens the leases-first doctrine; a target here must document the residue and lean on the reconfirmation chain |
| RabbitMQ | management-API HTTP timeouts are client-side only | same caveat as Redis; verify in P2 decision point |
| Kafka / Meilisearch / Neo4j | unknown — establish before design | P3 gate: no bound story, no target |

## 5. The conformance battery (the actual "conformal" ask)

One reusable, backend-parameterized integration battery — the rotation analog of the adapter-conformance harness — so every target ships with identical proof, not a hand-rolled test:

1. **compose alternates/mints** and preserves every non-credential fact of the current secret (endpoint, database, options);
2. **apply is idempotent** — applied twice, the pending credential still verifies;
3. **the verify gate holds** — an unauthenticatable pending credential (wrong password / NOLOGIN / disabled key) fails the run *before* promote, and the primary secret is byte-identical afterwards;
4. **the overlap window holds** — after promote, the *previous* credential still authenticates (alternation) or keeps working until its delayed revoke (key-mint);
5. **a full durable rotator run** completes end-to-end against the live backend, twice, alternating principals (or minting two generations);
6. **the declared apply bound is real** — `apply_latency_bound` is non-`None` and positive, rotator wiring accepts it only when `reconfirm_after` strictly exceeds it, and the backend-enforced mechanism actually kills a slow apply (provoke it against the live backend where the mechanism allows: e.g. an artificially tiny statement timeout fails the apply instead of committing late);
7. **client-side latency components validate against configured truth** — an allowance below the client's exposed acquire/connect timeout is refused at construction and again at apply time, before any statement executes.

Cases 3–4 are the two properties whose absence is an outage; case 6 is the property whose absence silently voids the reconfirmation physics (§4). The Postgres tests already prove all seven and become the battery's first instantiation.

## 6. Rollout

| Phase | Deliverable |
|---|---|
| 1 | ✅ **Done** — battery extracted to `tests/support/rotation_targets.py`, Postgres re-instantiated on it, `MongoRotationTarget` added and green against a live server |
| 2 | `ClickHouseRotationTarget` (dual-user; refuse single-user — per-request auth) · Redis/RabbitMQ decision point: ship targets **or** land the Vault-engine lease recipes and record leases as their doctrine |
| 3 | Demand-gated: Kafka SCRAM target, Meilisearch key-mint target, Neo4j (Enterprise) target |
| 4 | ✅ **Done** — per-backend doctrine table in the credential-rotation page, with the server-side-bound rule stated as the line that decides the hard cases; cloud backends covered as the platform-IAM row rather than a separate page |

Each phase lands with mock parity where applicable and the battery run against the real backend (the mock-horizon rule, applied prospectively as the secrets-lifecycle plane did).

## 7. Non-goals

- No contract changes: `compose`/`apply`/`verify` + the defaulted `apply_latency_bound` property already cover alternation and key-mint shapes.
- No MySQL target (no `forze_mysql` integration; the secrets-lifecycle plane already notes `RETAIN CURRENT PASSWORD` maps onto the port when one exists).
- Not the identity plane's API keys or the KEK/BYOK plane — different lifecycles, already owned elsewhere.
- **Not counterparty-rotated credentials** (single-use OAuth refresh tokens and kin). That is doctrine #5's separate direction of causality — the `RotatingCredentialStore` plane owns that contract, its crash-consistency ordering, and its Postgres adapter. The only coupling this RFC accepts: it must build on the secrets-lifecycle substrate (versioned reads, `SecretsAdminPort` CAS, per-ref dlock, change feed) instead of introducing a parallel one, so the two planes can never duplicate mechanism.

## 8. Decision log

| # | Decision |
|---|---|
| 1 | Coverage is closed by **doctrine, not uniformly by code**: every credential-holding backend gets an explicit doctrine (target / leases / platform IAM / operator-rotated); "no target" must be a recorded decision |
| 2 | A shared **conformance battery** is the definition of a done target; the Postgres tests seed it |
| 3 | Per-request-auth backends must not ship a single-principal degraded mode where the backend can avoid it — the overlap window is their only protection |
| 4 | Where a Vault engine exists (Mongo, Redis, RabbitMQ), leases are a first-class alternative and a deployment-level choice; the RFC does not force a target |
| 5 | Cloud IAM backends default to ambient identity; static-key targets are demand-gated future work |
| 6 | Every target declares a truthful `apply_latency_bound`, **backend-enforced** (server-side kill); client-side cancellation is not a bound, and a backend with no server-side bound story does not get a target without a recorded residue argument (or gets the leases doctrine instead) |
| 7 | The bound-carrying knob is required-positive with no `None` escape, and client-side latency components are validated against the client's *exposed* configuration at construction and re-checked at apply time |
| 8 | The battery grows two cases (bound realism, configured-truth validation) — seven total; the Postgres tests prove all seven and seed the shared kit |
| 9 | Counterparty-rotated credentials are a fifth doctrine owned by the `RotatingCredentialStore` plane, which composes the secrets-lifecycle primitives; this RFC records the doctrine and plans none of the work |
