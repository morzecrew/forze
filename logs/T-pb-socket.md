# T-pb-socket — socket.io fixes from the pre-existing bug backlog

Fixing backlog items 5, 1, 9 and 10 on `fix/pb-socket`, based on `origin/main` (5e654880e).
The backlog is the spec; there is no RFC. Each item lands as its own commit with its own
`[Unreleased]` entry, in the order 5, 1, 9, 10, so the commits cherry-pick onto one branch in
that order.

## Plan

Files, one line each:

- Item 5 — `src/forze_socketio/connection.py` and `routing.py`: a connection acts only under
  the identity it authenticated. Regression tests in
  `tests/unit/test_forze_socketio/test_connection_session.py` (new), run on python-socketio's
  own server, and `test_routing.py`. `CHANGELOG.md` — Security.
- Item 1 — `src/forze_socketio/connection.py`: offline replay and ack run on the serving
  runtime's live context instead of opening a scope per connect. Docstrings of
  `attach_realtime_connection` and `_bind_connection`, the draining warning's text, and
  `pages/docs/integrations/socketio.md`. Tests that wire replay with an unopened runtime are
  moved onto an open one; a regression test for concurrent connects and one for the ack.
  `CHANGELOG.md` — Fixed, **Breaking**.
- Item 9 — `tests/unit/test_forze_socketio/test_connection_drain_wire.py`: rerun the flaky
  test in a loop and under load; fix only if it still fails.
- Item 10 — `tests/unit/test_forze_socketio/test_connection_drain.py`: pin the drain's
  outcome counts when a connection is already closing.

## Item 5 — a connection's identity ends with it

Security fix; detail kept minimal per `SECURITY.md`. A socket.io connection acts only under
the identity it authenticated.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T09:52:00Z
attempt: 1
claim: the brief says an anonymous connect overwrites both session keys with `None`; the command dispatch reads a key holding `None` differently from an absent one
evidence: src/forze_socketio/routing.py:375
action: decided
proposal: ASSUMED — a connect removes both keys before it authenticates; an anonymous one then stores neither, so its session reads exactly as a first connect's does, and an authenticated one stores both.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T09:58:00Z
attempt: 1
claim: `ForzeSocketIOAdapter`'s own connect handler stores an identity in the same session key, so the same outcome is owed there
evidence: src/forze_socketio/routing.py:482
action: decided
proposal: ASSUMED — that handler clears the key as the connect starts. It registers no disconnect handler: python-socketio keeps one handler per event and namespace, so an app's own would replace it, or be replaced.
```

Verified red first; sabotage-checked.

## Item 1 — replay and ack on the serving runtime's live context

`_ConnectionLifecycle.replay` and `on_ack` opened `runtime.scope()` per call. A runtime holds
one scope at a time (`RuntimeVar.set_once`), so overlapping connects failed with "Value is
already set", and under the serving runtime every replay failed. Reproduced before the fix:
with 50 concurrent connects on an unopened runtime 49 lost their replay; with the serving
runtime open, every replay and every ack failed.

How the other transports take their context: MCP passes `ctx_factory=runtime.get_context`
and holds the scope open in its lifespan (`src/forze_mcp/server.py:51`);
`ForzeSocketIOAdapter` takes a `context_factory(request)` the app writes. The connection
layer already holds the runtime, so it reads `runtime.get_context()` the way MCP's factory
does.

`_bind_connection` binds through `InvocationContext.bind_identity`, whose authn and tenant are
`ContextVar`s (`src/forze/application/execution/context/invocation.py:58-66`). Each socket.io
handler runs in its own task with its own copy of the context, so connections binding at once
on the one shared context each see their own; a new test interleaves two replays with
different principals and tenants and checks each read.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T10:14:00Z
attempt: 1
claim: the brief allows `runtime.get_context()` or a context factory like the adapter's and MCP's; a new factory parameter would replace `runtime=` and break every caller twice over
evidence: src/forze_socketio/connection.py:395
action: decided
proposal: ASSUMED — keep `runtime=`; replay and ack read `runtime.get_context()`, as MCP's `ctx_factory=runtime.get_context` does. The break is only that the runtime must be the serving one, with its scope open.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T10:16:00Z
attempt: 1
claim: with the scope closed `get_context()` raises `exc.internal("Value is not set for 'ctx'")`, which names nothing an app upgrading past the break could act on
evidence: src/forze/base/primitives/runtime.py:58
action: decided
proposal: ASSUMED — replay and ack raise `exc.configuration` coded `realtime_replay_scope_closed`, naming the serving runtime and its open scope. Replay stays best-effort, so a connect still succeeds and the error is logged; an ack raises it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T10:18:00Z
attempt: 1
claim: now that the replay runtime is the serving one, `draining=` could default to `lambda: runtime.draining`
evidence: src/forze_socketio/connection.py:1028
action: decided
proposal: ASSUMED — no default. A draining source makes the connection close transports through python-engineio's internals (`require_close_internals`) and turn connects away, which a fix should not switch on by itself. The attach-time warning stays, its text no longer calling the replay runtime a different one.
```

Tests that wired replay with an unopened runtime now take a `serving_runtime` fixture
(`tests/unit/test_forze_socketio/conftest.py`) whose scope is open, as an app's lifespan holds
it. `test_connection_ack_clamp.py` drops its `_ConcurrentRuntime` facade, which worked around
exactly this one-scope limit, and bounds its waits on the held replay (a replay that failed
made them hang to the 300 s timeout). `test_connection_drain.py` drops
`test_the_replay_runtime_draining_its_own_scope_refuses_nothing`: it covered a replay scope's
own teardown announcing a drain, and replay opens no scope any more; that connects are refused
only through `draining=` stays covered by `test_without_a_draining_source_nothing_is_refused`.

Verified red first on the item-5 commit: the 50-connect test (`test_connection_mailbox.py`),
the direct ack and replay tests and the closed-scope test (`test_connection_authn_scope.py`)
all fail; the 50-connect one takes 90 s to fail, spent logging 50 tracebacks.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T12:20:00Z
attempt: 2
claim: review: the entry above says an ack raises the configuration error, but a connect whose replay failed has delivered nothing, so its acks return at the delivered-floor clamp and never reach the context
evidence: src/forze_socketio/connection.py:549
action: decided
proposal: ASSUMED — the docs and docstring say what happens: each connect's replay logs the error and delivers nothing, and that connection's acks are ignored, since there is nothing for them to claim. Acks are not made to fail as well: the logged replay error already names the cause once per connect, and an ack raising from a socket.io handler only adds an unhandled-task log. A test pins it.
```

## Item 9 — the polling reconnect test

`TestReconnectAfterDrain::test_a_polling_client_following_the_javascript_rules_lands_on_the_other_replica`
timed out once in a full unit run before #560 merged; #560 made the drain wait until the client
has heard its first accept. Rechecked on this branch, after items 5 and 1:

- `test_connection_drain_wire.py` alone, 30 runs in a row: 30 passed.
- The whole `tests/unit/test_forze_socketio` directory, 5 runs with every one of the 16 cores
  held by a busy loop (killed by PID afterwards): 5 × 371 passed, 3 skipped, 30–33 s a run.

No longer reproduces, so nothing is changed and there is no commit for it.

## Item 10 — the drain's counts when a connection is already closing

`_still_closable` leaves alone a connection something else is closing, but the drain's counts
were all that changed without its `_open` check, and no test read them for such a connection.
`TestDrainStep::test_a_connection_something_else_is_closing_is_skipped_and_not_counted` marks
one of two connections as closing and pins the counts at one closed. With `_open` removed it
fails (`closed: 2`), and it is the only test in the socket.io suite that does.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T10:42:00Z
attempt: 1
claim: the branch rules give every commit its own `[Unreleased]` entry, and CONTRIBUTING → Changelog leaves test-only changes out
evidence: CONTRIBUTING.md:398
action: decided
proposal: ASSUMED — the item 10 commit changes tests only, so it carries no changelog entry; the repository's changelog scope decides what the changelog records.
```

## Self-audit — 2026-10-15

**Scope.** The whole branch against `origin/main` (5e654880e): 3 commits, 12 files,
+681/−227 lines (`src/forze_socketio/connection.py` and `routing.py`, seven socket.io test
files, the socket.io docs page, `CHANGELOG.md`, this log). Re-read the backlog entries for
items 1, 5, 9 and 10 and the user's decisions first.

**Checks run.**

- Red first: items 5 and 1 as recorded above; item 10's test fails with the `_open` check
  removed. Item 9 has no change to verify.
- Sabotage: item 5's checked; item 10's one mutant is killed by its new test only.
- Patch coverage, socket.io unit suite with branch coverage: 40 of 40 added `src` lines
  covered, including the `KeyError` branch of `_drop_identity` (an unknown sid's disconnect).
- `just test tests/unit`: 17,977 passed, 16 skipped, 13 deselected.
- The Redis integration tests that run `forze_socketio`
  (`tests/integration/test_forze_redis/test_redis_realtime_*.py` and
  `test_redis_connection_rebalance.py`): 9 passed.
- `just quality -s`: all 19 gates green.
- `CHANGELOG.md` validator: 150 problems before and after, all in released sections; the two
  new entries add none.

**Findings.**

1. Fixed. The module docstring of `connection.py` still said the disconnect handler "only
   updates presence", and `attach_realtime_connection`'s docstring did not say how long the
   stored identity lasts. Both corrected, in the item 5 commit.
2. Fixed. The item 5 entry cited `routing.py:481` for the adapter's write; it is line 482.
3. Fixed, in item 1. Three waits in `test_connection_ack_clamp.py` on the held replay were
   unbounded, so a replay that failed hung the test to the 300 s timeout instead of failing.

**Residue.**

- Item 1 shares one context, and with it the runtime's cached ports, across every connection's
  replay. That is the contract every other transport already relies on, since port factories
  must defer per-request reads, but no test here runs one cached store under two tenants.
- On the code before item 1, the 50-connect test takes about 90 s to fail, spent logging
  50 tracebacks. It passes in milliseconds on the fix.

## Independent review — 2026-10-15

A fresh-eyes review of the three commits found one MEDIUM and three LOW issues, all fixed as
fixup commits:

1. MEDIUM, item 5 (security). A further case of the item 5 guarantee. Fixed; verified red
   first; sabotage-checked.
2. LOW. This log, the Security changelog entry and the item 5 commit body carried more detail
   than the security policy allows; reduced to the guarantee.
3. LOW, item 1. `build_realtime_mailbox`'s docstring still described the connection layer's
   per-unit-of-work scope; it now names the serving runtime's context.
4. LOW, item 1. The docs and the `attach_realtime_connection` docstring said acks fail with a
   closed scope; they are ignored. Reworded, and pinned by
   `test_with_the_serving_scope_closed_replay_is_logged_and_acks_are_ignored`. Decision above.

The review's own probe of item 1's residue — one cached tenant-aware mailbox serving
concurrent replays and acks for two tenants with the same principal id — kept each tenant's
entries apart.

After the fixups: socket.io unit suite 374 passed, 3 skipped; `just test tests/unit` 17,980 passed, 16 skipped, 13 deselected;
`just quality -s` all 19 gates green.

**Drift count: 0.**
