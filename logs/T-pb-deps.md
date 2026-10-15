# T-pb-deps — capability probes on wrapped ports, and test hygiene

Fixing backlog items 4, 6 and 8 of the pre-existing-bugs list on `fix/pb-deps`, based on
`origin/main` (5e654880e). No RFC: the backlog is the spec.

A port deps resolution hands out can be wrapped twice over. A command accessor wraps it in
`GuardedWritePort`; `resolve_configurable` wraps it in `PortProxy` subclasses (interception,
runtime tracing, OTel port spans, a resilience port policy). Each forwards through
`__getattr__`, which a `@runtime_checkable` check never consults, so a protocol `isinstance` of
the wrapper is false for a backend that implements the protocol.

## Plan

Commits, in this order:

1. `backend_of(port)`, one shared helper, and the dlock resolve on it (item 6).
   - `src/forze/application/execution/port_proxy_base.py` → `src/forze/application/contracts/deps/proxy.py`: `PortProxy` moves down to contracts, beside `GuardedWritePort`, so a contracts-level helper can unwrap it (entry 2).
   - `src/forze/application/contracts/deps/keys.py`: `backend_of` unwraps the guard and the proxies down to the port the factory built.
   - `src/forze/application/contracts/deps/__init__.py`: exports both.
   - the four proxy subclasses, `forze_mock/seeding/apply.py` (its own unwrap loop becomes `backend_of`), one test: imports.
   - `src/forze/application/contracts/dlock/deps.py`: probes `backend_of(port)` rather than resolving a second time.
   - test: `cache_ports=False` with a per-instance capability report.
2. `execution/context/transaction.py:205` alone, the fail-open one (`TransactionallyEnlistable`).
3. `contracts/stream/deps.py:82` (`CommitStreamGroupAware`).
4. `forze_kits/integrations/portability/export.py:487` and `migrate.py:282` (`GraphEdgeExportAware`), one coherent group.
5. `integrations/document/cache.py:189` (`SupportsInvalidationPush`).
6. `integrations/graph/streaming.py:40` (`GraphStreamingAware`).
7. Item 8, test hygiene: temporal `pytestmark`, Mongo `with_options`, the mailbox warning, `_free_port` into `tests/support`.

Each fix is red first, with the port passed through the real wrapper stack (a `DepsRegistry`
with OTel port spans or port logging), and carries its own `[Unreleased]` entry.

## Site census

A scan of every `isinstance(x, P)` where `P` is a `@runtime_checkable` protocol defined under
`src/` found 35 checks (34 lines). Six can see a wrapped port; the rest cannot:

- **wrapped, fixed here:** the six above.
- **never a resolved port:** step factories (`plans.py` ×5, `registry/validation.py`, `forze_kits/aggregates/authn/factories.py`), tracers and modules (`deps/registry.py` ×3), an interceptor (`interception/proxy.py`), a spec (`guarantees/capabilities.py:224`).
- **plain or simple deps, which nothing wraps:** the transaction manager (`transaction.py:377`, `resolve_simple`), the durable run admin (`run_admin.py:154`, `resolve_simple`), the breaker store (`resilience/executor.py`), crypto ciphers, KMS and key directories (`crypto/codec.py` ×3, `crypto/keyring.py` ×7, `storage/adapter.py`).
- **checked before wrapping:** `guarantees/capabilities.py:233`, called only by `resolve_configurable` on the raw port.
- **built by the caller, not resolved:** the realtime conformance mailbox (`realtime/conformance.py:270`).

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T13:00:00Z
attempt: 1
claim: the backlog leaves open whether the shared helper unwraps the wrapper chain or probes members through it with getattr + callable
evidence: src/forze/application/contracts/deps/keys.py:84-90 — the guard refuses a probe inside a read-only operation; src/forze/application/execution/port_proxy_base.py:76-82 — a proxy forwards every name, wrapping callables (a probe's call is traced, spanned and run under the resilience policy); a getattr probe also answers yes for any object with a dynamic `__getattr__`, a `MagicMock` included
action: decided
proposal: ASSUMED — unwrap. `backend_of(port)` follows the guard's target and each proxy's `inner` to the port the factory built, and the caller asks `isinstance(backend_of(port), P)`. It judges the backend's own type, the same answer an unwrapped port gets today; it needs no list of member names per protocol; a guard in a read-only operation is not tripped by a question that writes nothing; and a capability report is read off the backend, not through a span or a retry. Calls that do work still go through the wrapped port.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T13:01:00Z
attempt: 1
claim: two of the sites are in `forze.application.contracts`, the bottom application layer, and `PortProxy` lives in `forze.application.execution`, which contracts may not import
evidence: pyproject.toml:929-937 — layers hooks > execution > integrations > contracts; src/forze/application/contracts/stream/deps.py and contracts/dlock/deps.py are among the sites; port_proxy_base.py imports only `inspect` and `attrs`
action: decided
proposal: ASSUMED — move `PortProxy` to `forze.application.contracts.deps.proxy`, beside `GuardedWritePort`, unchanged, and update its five importers. No shim at the old path: the module was never exported or documented.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T13:30:00Z
attempt: 1
claim: the not-enlisted guard fails open, and the question is whether that makes it a security fix
evidence: src/forze/application/execution/context/transaction.py:205 — `isinstance(resource, TransactionallyEnlistable)` of an `OtelSpanPortProxy` or `InterceptingPortProxy` is false, so `assert_enlisted` returns; reached from src/forze_kits/integrations/inbox/consumer.py:210
action: decided
proposal: ASSUMED — a correctness fix, `🐛 fix(execution)`, with a `Fixed` entry. What fails open is a wiring check on the operator's own deployment: an inbox on another pool commits its dedup mark apart from the handler, so a message can be handled twice. No caller crosses a trust boundary, nothing is disclosed, and no authorization decision depends on it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T14:40:00Z
attempt: 1
claim: the intermittent "coroutine `_loop` was never awaited" warning in `TestRetentionIsPaired` comes from a sibling test, not from the one it is reported under
evidence: src/forze_kits/integrations/realtime/lifecycle.py:552 — `asyncio.create_task(_loop(), ...)`; with `PYTHONTRACEMALLOC=10` the unawaited coroutine is allocated here, from `await step.startup(ctx)` in `test_a_sweep_that_never_starts_does_not_vouch` (test_realtime_mailbox.py:515 before the fix); that test patches `asyncio.create_task` to raise, so the `_loop()` it was handed is never closed and warns whenever the garbage collector reaches it — reported 3/3 file runs, 0/3 with that test deselected
action: decided
proposal: ASSUMED — fixed in the test: its refusing `create_task` closes the coroutine before raising. Not in `lifecycle.py`: startup is a coroutine, so the real `create_task` always has a running loop and cannot fail the way the double does.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T14:41:00Z
attempt: 1
claim: `_free_port` has four named copies and three more under other names
evidence: tests/integration/test_forze_inngest_integration/_harness.py:29 — `free_tcp_port`, which binds `0.0.0.0`; before the fix also `_closed_port` (test_jwks_partition_fail_closed.py:36), the same body, and an inline copy in the Temporal integration conftest (lines 102-105)
action: decided
proposal: ASSUMED — `tests/support/ports.py::free_port` replaces the four copies, `_closed_port` and the inline Temporal one. `free_tcp_port` stays: it binds every interface, which the Inngest dev-server container may need to reach the host, and narrowing it to loopback is not a hygiene change.
```

**Drift count: 0.**

## Self-audit — 2026-10-15

Scope: the whole branch against `5e654880e`, 7 commits, 36 files, +448/−129. The spec was
re-read first: backlog items 4, 6 and 8, their "Verified on origin/main" notes and the task
brief.

Checks run:

- **Census re-run.** An AST scan of every `isinstance(x, P)` with `P` a `@runtime_checkable`
  protocol under `src/`: 35 checks, each classified in the census above. The six that see a
  wrapped port are fixed.
- **Red first.** Each new test failed before its fix: the item 6 test both bare and wrapped
  (`dlock.fencing_unsupported`), the inbox test (`DID NOT RAISE`), the stream, export,
  migrate, L1 push and graph-capability tests.
- **Sabotage of `backend_of`.** Returning the port unchanged fails 12 tests; skipping the
  guard fails 5, including the three read-only dlock tests; unwrapping one proxy only fails 8.
  Every site is caught by its own test in all three.
- **Suites.** `just test tests/unit`: 17976 passed, 16 skipped. Integration for the touched
  planes (Mongo inbox, Neo4j streaming and portability, all of Redis, the routed Temporal
  client, `test_portability`): 266 passed. Core and mock integration: 11 passed.
  `just quality -s`: every gate green.

Findings:

1. **Fixed: the same wrapped-registry setup in five test files.** Extracted to
   `tests/support/port_wrappers.py::with_port_wrappers`. The inbox test now stacks both
   wrappers instead of taking one per parameter, which also proves the loop unwraps more than
   one layer.
2. **Fixed: the item 6 changelog entry understated the bug.** A second backend could be
   judged capable as easily as incapable, so the check could also accept a backend that does
   not fence. The entry now says the check judged the second one.
3. **Fixed: no page told app authors that their own protocol checks fail under port spans or
   port logging.** `running-in-prod/observability.md` now names `backend_of` there.

Residue:

- `durable_run_control_capabilities` and the other simple- and plain-dep sites stay plain
  `isinstance`. Nothing wraps those today; if simple deps ever get proxies, they need
  `backend_of` too.
- The commits were rebuilt twice to fold audit fixes into them (test-only and changelog-only
  diffs). The suites above ran on the final head; the intermediate commits were tested when
  first written, not after the rebuild.
- `free_tcp_port` in the Inngest harness is still a separate copy, as logged.

## Review round — 2026-10-15

An independent review found 4 LOW findings and nothing higher. Fixes to existing commits are
`fixup!` commits; the two sibling helpers are a new commit, which also carries this section.

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T16:00:00Z
attempt: 1
claim: `backend_of` is a public route to the unguarded write port inside a read-only operation, where `forze.testing.unguarded` refuses
evidence: src/forze/application/contracts/deps/keys.py:130 — `port = port._target`, with no read-only check
action: decided
proposal: ASSUMED — app authors get `implements(port, protocol) -> bool`, which answers for the backend and hands nothing out; docs and the changelog point at it. `backend_of` stays exported, because forze_kits, forze_mock and the core's own capability reads need the object, and both docstrings now say it strips the guard in a read-only operation too. `implements` returns `bool`, not `TypeIs`: mypy refuses a protocol class where `type[P]` is expected (`type-abstract`), so the narrowing would cost a suppression at every call site.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T16:01:00Z
attempt: 1
claim: moving `PortProxy` into `forze.application.contracts.deps` put it in that package's `__all__`, a new public export the changelog did not mention
evidence: src/forze/application/contracts/deps/proxy.py:21 — `class PortProxy`
action: decided
proposal: ASSUMED — kept out of `__all__`: it was not exported from `forze.application.execution` either. Importers use `forze.application.contracts.deps.proxy`.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-15T16:02:00Z
attempt: 1
claim: an interceptor cannot show that the edge export still runs through the wrappers, because the mock's `find_edges_export_stream` is a plain method returning an iterator, which interceptors and OTel spans pass through
evidence: src/forze_mock/adapters/graph.py:802 — `def find_edges_export_stream(`, not `async def`
action: decided
proposal: ASSUMED — the wrapped tests add runtime tracing, which records sync calls too, and assert the op is in the trace (`tests/support/port_wrappers.py::traced_ops`). Replacing the call with one on `backend_of(...)` fails the export, migrate and L1-push tests.
```

Checks for this round: the `implements` test was red on import first; the sibling-helper tests
were red before their fix; the three mutations named in the review (`cache.py`, `export.py`,
`migrate.py` calling the backend directly) each fail their test.
