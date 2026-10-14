# RFC 0075 — Realtime connection drain

- **Status:** 📝 Draft
- **Scope:** Closing a replica's realtime connections gently when it leaves, and refusing new
  ones while it is leaving. It covers:
  - a socket.io drain step that closes the selected connections at the transport level,
    spread over a window, as a drain phase from RFC 0074;
  - `close_spread`, exported for RFC 0076;
  - refusing connects while the replica is draining, with a declared code;
  - connection gauges and a refused-connect counter;
  - an `admission` hook on connect;
  - the same drain for `forze_fastapi`'s SSE and WebSocket live tails, through their shared
    hub.

  Pacing admissions in priority lanes, and rebalancing a running fleet, are out of scope.
- **Related:**
  - [`src/forze_socketio/connection.py`](../src/forze_socketio/connection.py): `on_connect`,
    `_local_connections`, `attach_realtime_connection`;
  - [`src/forze_socketio/gateway_lifecycle.py`](../src/forze_socketio/gateway_lifecycle.py);
  - [`src/forze_socketio/observability.py`](../src/forze_socketio/observability.py);
  - [`src/forze_fastapi/realtime/`](../src/forze_fastapi/realtime/): `ws.py`, `sse.py`,
    `hub.py`;
  - RFC 0074 (lifecycle state, drain phases, `serve_uvicorn`), RFC 0076 (rebalancing).
- **Origin:** hh-backend-v2's fleet proposals (F4, 2026-10-14). The app's `close_spread`,
  `drain` and `DRAIN_TAIL` (about 55 LOC) came from two incidents:
  - D-280: awaiting each close in turn let about 3,500 sockets outrun the grace period;
  - D-274: without the shutdown order, no drain ran at all.

---

## 1. Summary

```python
connection_drain_lifecycle_step(sio, *, window, tail, namespaces=("/",), select=None)
```

The step registers a drain phase that closes this replica's selected connections at the
Engine.IO level, over `window`. It starts each close on its own schedule rather than awaiting
the one before, then waits up to `tail` for the closes to finish. Socket.IO's JavaScript
clients see a transport close, and reconnect through the balancer to replicas that are still
ready.

While the replica is draining, `on_connect` refuses with the code `realtime_draining`. Gauges
count connections per namespace, and a counter counts refused connects by reason.
`attach_realtime_connection(admission=…)` lets an app refuse or delay a connect before
authentication.

The FastAPI SSE and WebSocket live tails share one hub, and draining the hub ends both.

## 2. Motivation

**Nothing closes realtime connections when a replica leaves.**
- The socket.io gateway step drains its signal-consume loop at a batch boundary
  (`gateway_lifecycle.py:90-145`). The identity-expiry, presence and backplane loops are
  drainables too (`connection_lifecycle.py:47`, `:70`, `:97`).
- The only `disconnect` call in `forze_socketio` is the expiry sweep (`connection.py:716`).
- The FastAPI lifespan enters the runtime scope and nothing else (`lifespan.py:42-53`). The SSE
  live tail ends only on client disconnect (`sse.py:313`).

Connections therefore end when the server process stops: under `uvicorn app:app`, every
WebSocket is closed with 1012 at the same moment, and otherwise at the orchestrator's kill.
Thousands of clients then reconnect in the same second to the replicas that are left.

**Closing them one at a time is not enough.** Awaiting each close in turn lets a few thousand
sockets outrun the grace period (hh-backend-v2's D-280).

**A connect that lands on a leaving replica is wasted.** `on_connect` refuses only on protocol
or authentication (`connection.py:517-541`), so a client that connects during the drain is
closed again moments later.

**Nothing counts connections.** The exported counters are about deliveries
(`observability.py:34-42`). `_local_connections` (`connection.py:680-683`) and
`RealtimeSseHub.subscribers` (`hub.py:145`) are computed but never exported.

## 3. Current state

All verified against the tree at the time of writing.

- **`on_connect`** (`connection.py:517-576`):
  1. negotiates the protocol;
  2. identifies;
  3. stores the session;
  4. joins the principal room;
  5. calls `presence.joined`;
  6. replays.

  It refuses with `SocketIOConnectionRefusedError` carrying a message string. A second
  argument would reach the client as the error's `data`.
- **`attach_realtime_connection(sio, *, namespace, resolve, presence, mailbox_factory,
  cursors_factory, runtime, share_identity)`** has no admission hook (`connection.py:598-608`).
  It accepts `runtime` only together with both replay factories (`:648-656`), so most apps
  give the connection handler no runtime.
- **`_local_connections(sio, namespace)`** lists this node's sids through
  `sio.manager.get_participants(namespace, None)`, which yields `(sid, eio_sid)` pairs.
- **A namespace-level close.** `sio.disconnect(sid, namespace)` sends a Socket.IO DISCONNECT
  packet (`python-socketio`'s `async_server.py:408-437`). The JavaScript client reports it as
  "io server disconnect" and does not reconnect on its own. The Python client stops
  reconnecting too (`async_client.py:401-412`, `:594`).
- **The FastAPI hub.** WebSocket live tails subscribe to the same `RealtimeSseHub` as SSE
  (`ws.py:476-480`). A WebSocket refusal is accepted first and then closed, because a close
  before accept becomes an opaque 403 (`ws.py:466-469`).
- **Metrics.** Counters for emitted, failed, skipped, rejected, mailboxed, bridged and
  poisoned signals, and two backplane gauges. Nothing for connections.
- **Resilience strategies.**
  - `RateLimitStrategy` rejects at once.
  - The bulkheads limit concurrency, with queues ordered FIFO or LIFO. Their `prioritized`
    option chooses which waiter is displaced and scales CoDel shedding per tier.

  None waits in priority lanes.

## 4. Goals / Non-goals

**Goals**

- A replica that is leaving closes its realtime connections spread over a window, finished
  inside the deploy's budget, before the server tears down. That holds under RFC 0074's
  `serve_uvicorn`.
- Socket.IO's JavaScript clients reconnect on their own after the close, to a replica that is
  still ready. Other clients, the Python one included, need their own reconnect (5.1).
- A connect that arrives while the replica is leaving is refused at once, with a code the
  client can act on.
- Connection counts are visible, and so are refused connects, by reason.
- Apps can refuse or delay connects before authentication, without forking forze's connect
  handler.

**Non-goals**

- **Choosing where clients reconnect.** That is the balancer's.
- **Rebalancing a running fleet** (RFC 0076). This RFC closes connections on exit only.
- **Priority-lane admission pacing.** The hook lets an app bring its own; forze does not ship
  one.
- **Graceful drains under `uvicorn app:app`.** Uvicorn closes every WebSocket at once before
  forze's phases run (RFC 0074 §5.5).

## 5. Design

### 5.1 Closing on a schedule

```python
async def close_spread(
    sio: AsyncServer,
    targets: Sequence[tuple[str, str]],   # (namespace, eio_sid)
    window: timedelta,
    *,
    tail: timedelta,
) -> int: ...                              # closes that finished in time
```

- **The transport level.** Each close is `sio.eio.disconnect(eio_sid)`, which ends the
  connection itself. A JavaScript client reports a transport close and reconnects with its
  usual backoff. A namespace-level `disconnect` would tell it not to.
- **Python clients reconnect in neither case.** The docs state the client contract.
- **Scheduling.** Close *i* of *n* starts at `i * window / n` after the call. Each starts as its
  own task, so a slow close never delays the next. After the last start, the call waits up to
  `tail` for the started closes, then cancels any still running and returns.
- **Bounds.** The whole call takes at most `window + tail`.
- **Failures.** A close that fails is logged and counted, never raised.
- **Deduplication.** One Engine.IO connection carries several namespaces, so targets are
  deduplicated by `eio_sid`.

### 5.2 The drain step

```python
def connection_drain_lifecycle_step(
    sio: AsyncServer,
    *,
    window: timedelta,
    tail: timedelta,
    namespaces: Sequence[str] = ("/",),
    select: Callable[[str, str], bool] | None = None,   # (namespace, sid) -> may close?
    order: int = 0,
    step_id: StrKey = "realtime_connection_drain",
) -> LifecycleStep: ...
```

- **The phase.** At startup the step registers a drain phase with RFC 0074's `drain_phases`,
  from its hook's `ctx`. It needs no handle on the runtime.
- **When it runs,** the phase lists this node's connections and groups them by Engine.IO
  transport.
  - A transport is closed only if every namespace it carries is in `namespaces` and accepted
    by `select` (all by default).
  - One Engine.IO connection can carry a movable namespace and a non-movable one, and closing
    the transport ends both. So a single refusal keeps the whole transport open.
- **Fitting the allowance.** The phase measures what it may still take after listing and
  selecting, `b`, from a deadline, not from the figure it was handed.
  - The tail becomes `min(tail, b / 2)`, and the window `min(window, b − that tail)`.
  - Fitting only ever shortens the configured window, never lengthens it, so a generous
    budget does not slow a drain down.
  - Below RFC 0074's floor the phase is skipped, and the connections close when the server
    stops.

  So a phase never overruns its allowance, and is never cancelled mid-close.
- **Connects keep arriving** until the balancer has noticed that readiness failed. The
  refusal in 5.3 covers them, so the phase does not loop.

### 5.3 Refusing while draining

`attach_realtime_connection` gains `draining: Callable[[], bool] | None`. It defaults to
`runtime.draining` when `runtime` is wired for replay, and an app without replay passes
`draining=lambda: runtime.draining` itself.

`on_connect` checks it twice:
1. Before negotiating the protocol, so a draining replica does no authentication work.
2. As its last step, after admission, authentication and replay. A connect that waited in
   admission or authentication while the drain listed its targets would otherwise finish
   after the snapshot and escape the drain.

When it is true, it refuses with
`SocketIOConnectionRefusedError("Server is draining", {"code": "realtime_draining"})`.

**A late refusal undoes the connect's side effects first.** A refused connect never reaches
`on_disconnect`. So before refusing at the last check, `on_connect` itself:
- calls `presence.left`;
- leaves the rooms it joined;
- drops the stored session and the replay progress.

Otherwise a refused client would stay counted as online in shared presence until its entry
expired.
- **What the client sees.** The data reaches the client as the connect error's `data`, so a
  client can tell this refusal from an authentication failure.
- **Retrying.** The docs advise retrying with jittered backoff. Behind sticky sessions an
  immediate retry would land on the same replica until the balancer notices.
- **Existing refusals are unchanged.** They stay message-only here, since adding codes to them
  changes what clients receive.

### 5.4 Admission

`attach_realtime_connection(…, admission=…)` takes an optional
`Callable[[SocketIOConnect], Awaitable[None]]`.
- It runs after the draining check and before protocol negotiation and authentication.
- Raising a `CoreException` refuses the connect with the reason `admission`. The same rule
  `on_connect` applies to other errors decides what the client sees: a client-safe error's
  summary, or, for a server-kind error, the generic internal detail, with the error logged.
- Awaiting delays the connect, so an app can pace admissions by its own rules: for example,
  hh-backend-v2's priority lanes.

### 5.5 Metrics

- **`forze.realtime.connections`:** an up-down counter, labelled by namespace. It moves on
  connect and disconnect.
- **`forze.realtime.connect_refused`:** a counter, labelled by namespace and reason. The
  reasons are `protocol`, `auth`, `draining`, `admission` and `internal`.
- **`forze.realtime.drain_closed`:** a counter of closes the drain started, labelled by
  outcome: `closed`, `failed`, `timed_out`.

### 5.6 FastAPI SSE and WebSocket live tails

WebSocket live tails subscribe to the same `RealtimeSseHub` as SSE streams, so one drain covers
both.
- **The drain.** A drain step gives the hub's subscribers an end signal through the same
  scheduling. An SSE response then ends, and the client reconnects with its last event id.
- **WebSocket tails.** A WebSocket handler whose subscription ends closes its socket with 1012
  (service restart), the code `WS_RECONNECT_CLOSE` already uses.
- **Refusals while draining.**
  - A new SSE request gets 503.
  - A new WebSocket upgrade is accepted and then closed with 1012, as the route's existing
    refusals are, because a close before accept becomes an opaque 403.

### 5.7 Alternatives considered

- **Close everything at once at shutdown.** It is what happens today by default, and sends
  every client to the remaining replicas in the same second. Rejected.
- **Await each close in turn.** It does not finish in time at a few thousand sockets (D-280).
  Rejected.
- **A namespace-level `disconnect`.** It tells Socket.IO clients the server meant it, and they
  do not reconnect. Rejected for an Engine.IO close.
- **A drainable instead of a drain phase.** Drainables stop after the gate closes, and under
  uvicorn after the server may already have closed the sockets (RFC 0074 §5.6). Rejected.
- **Codes on every refusal.** Useful, but it changes what existing clients receive. Left for
  its own change.

## 6. Tests

- **`close_spread`.**
  - 3,500 fake targets with a slow fake close finish within `window + tail`.
  - The starts are spread evenly.
  - A failing close is counted and not raised.
  - Targets sharing an `eio_sid` close once.
- **The drain step**, with RFC 0074's phases, placed in its own runtime's plan:
  - selected connections close and others stay;
  - a transport carrying a refused namespace stays open;
  - the window and the tail shrink to a small budget, and a large budget keeps the configured
    window;
  - below the floor the phase is skipped.
- **A late connect.** A connect held in admission while the drain lists its targets is refused
  at its last check, and the presence count, the rooms and the session store are as they were
  before it arrived.
- **Reconnects.** A real python-socketio server with an Engine.IO-level close. The client's
  disconnect reason is a transport close, not a server disconnect.
- **Refusal.** While draining, a connect is refused with `{"code": "realtime_draining"}` before
  authentication runs, with replay wired and without it.
- **Admission.** A hook that raises refuses with reason `admission`. A hook that sleeps delays
  the connect.
- **Metrics.** The connection count follows connects and disconnects, and refusals are counted
  by reason.
- **FastAPI.**
  - Draining the hub ends SSE streams and closes WebSocket tails with 1012, over the window.
  - While draining, new SSE requests get 503, and new upgrades are accepted then closed with
    1012.
- **Integration with `serve_uvicorn`.** SIGTERM closes connections over the window before
  uvicorn's own shutdown runs.

## 7. Docs

- **The socket.io page:**
  - draining a replica, and the window and the tail against the budget;
  - the client contract: JavaScript clients reconnect after a transport close, Python
    clients need their own reconnect;
  - the refusal code, and retrying with backoff;
  - admission and the metrics.
- **The FastAPI realtime page:** the hub drain for SSE and WebSocket tails.

## 8. Out of scope

- **Priority-lane pacing.** The hook makes it possible; a forze implementation waits for a
  second app that needs it.
- **Rebalancing** (RFC 0076).
- **Codes on existing refusals.**
- **Graceful drains under `uvicorn app:app`** (§4).

## 9. Risks

- **A window longer than the budget.** The phase shortens it, and RFC 0074's docs give the
  arithmetic.
- **python-socketio internals.** `get_participants` and `eio.disconnect` are how the step lists
  and closes connections. A release that changes them breaks the step, and the tests run
  against the library's own manager and server.
- **A reconnect storm moved, not removed.** Spreading the closes spreads the reconnects too. A
  balancer that sends them all to one replica still overloads it. The docs recommend a
  least-connections balancer.

## 10. Unresolved questions

- Whether the hub's end signal is a new subscription state or a sentinel event. Implementation
  decides.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Connections close in a drain phase (RFC 0074), not a drainable, so under `serve_uvicorn` they close while the replica still serves and before uvicorn's own shutdown. |
| 2 | `LOCKED` | `close_spread` closes at the Engine.IO level, starting each close on its own schedule over the window and never awaiting closes in turn. The whole call is bounded by `window + tail`. |
| 3 | `ASSUMED` | The phase measures its allowance after listing, shortens its tail to at most half of it and its window to at most the rest, never lengthening the configured window, and is skipped below RFC 0074's floor. |
| 4 | `ASSUMED` | `on_connect` refuses while `draining()` is true, checked before negotiation and again as its last step, with data `{"code": "realtime_draining"}`. A refusal at the last step first undoes presence, room joins and the stored session. `draining` defaults to `runtime.draining` when replay wires a runtime, and can be passed otherwise. Existing refusals are unchanged. |
| 5 | `ASSUMED` | `admission` runs after the draining check and before authentication, and refuses on a `CoreException`. Awaiting it delays the connect. |
| 6 | `ASSUMED` | Metrics: a connection count per namespace, refused connects by reason, and drain closes by outcome. |
| 7 | `ASSUMED` | FastAPI drains through the shared hub. Ended SSE streams and WebSocket tails close with 1012. While draining, new SSE requests get 503, and new upgrades are accepted then closed with 1012. |
| 8 | `ASSUMED` | The step reads its state from its hook's `ctx`, and takes no `runtime`. |
| 9 | `LOCKED` | Selection is per Engine.IO transport: a transport is closed only when every namespace it carries is eligible, because closing it ends them all. |

## 12. Phasing

1. **socket.io:** `close_spread`, the drain step, the draining refusal and the metrics, once
   RFC 0074's phases ship.
2. **The admission hook.**
3. **The FastAPI hub drain.**
