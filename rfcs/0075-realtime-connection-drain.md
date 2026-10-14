# RFC 0075 — Realtime connection drain

- **Status:** 📝 Draft
- **Scope:** Closing a replica's realtime connections gently when it leaves, and refusing new
  ones while it is leaving. It covers:
  - a socket.io drain step that closes the selected connections spread over a window, as a
    drain phase from RFC 0074;
  - `close_spread`, exported for RFC 0076;
  - refusing connects while the runtime is draining, with a declared code;
  - connection gauges and a refused-connect counter;
  - an `admission` hook on connect;
  - the same drain for `forze_fastapi`'s WebSocket and SSE routes.

  Pacing admissions in priority lanes, and rebalancing a running fleet, are out of scope.
- **Related:**
  - [`src/forze_socketio/connection.py`](../src/forze_socketio/connection.py): `on_connect`,
    `_local_connections`, `attach_realtime_connection`;
  - [`src/forze_socketio/gateway_lifecycle.py`](../src/forze_socketio/gateway_lifecycle.py);
  - [`src/forze_socketio/observability.py`](../src/forze_socketio/observability.py);
  - [`src/forze_fastapi/realtime/`](../src/forze_fastapi/realtime/): `ws.py`, `sse.py`,
    `hub.py`;
  - RFC 0074 (drain announcement and phases), RFC 0076 (rebalancing).
- **Origin:** hh-backend-v2's fleet proposals (F4, 2026-10-14). The app's `close_spread`,
  `drain` and `DRAIN_TAIL` (about 55 LOC) came from two incidents:
  - D-280: awaiting each close in turn let about 3,500 sockets outrun the grace period;
  - D-274: without the shutdown order, no drain ran at all.

---

## 1. Summary

```python
connection_drain_lifecycle_step(sio, *, runtime, window, tail, namespaces=("/",), select=None)
```

The step registers a drain phase that closes this replica's selected connections over
`window`. It starts each close on its own schedule rather than awaiting the one before, then
waits up to `tail` for the closes to finish. Clients reconnect through the balancer to
replicas that are still ready.

While `runtime.draining` is true, `on_connect` refuses with the code `realtime_draining`.
Gauges count connections per namespace, and a counter counts refused connects by reason.
`attach_realtime_connection(admission=…)` lets an app refuse or delay a connect before
authentication.

`forze_fastapi`'s WebSocket and SSE routes get the same drain.

## 2. Motivation

**Nothing closes realtime connections when a replica leaves.**
- The socket.io gateway step drains its signal-consume loop at a batch boundary
  (`gateway_lifecycle.py:90-145`). The identity-expiry, presence and backplane loops are
  drainables too (`connection_lifecycle.py:47`, `:70`, `:97`).
- The only `disconnect` call in `forze_socketio` is the expiry sweep (`connection.py:716`).
- The FastAPI lifespan enters the runtime scope and nothing else (`lifespan.py:42-53`). The SSE
  live tail ends only on client disconnect (`sse.py:313`). `WS_RECONNECT_CLOSE` (1012) is used
  only after an unconfirmed replay (`ws.py:110`, `:614-617`).

Connections therefore end when the server process dies: all at once, at the orchestrator's
kill. Thousands of clients then reconnect in the same second to the replicas that are left.

**Closing them one at a time is not enough.** Awaiting each close in turn lets a few thousand
sockets outrun the grace period (hh-backend-v2's D-280).

**A connect that lands on a leaving replica is wasted.** `on_connect` refuses only on protocol
or authentication (`connection.py:517-541`), and does not look at the runtime, though it holds
one (`:606`). A client that connects during the drain is closed again moments later.

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

  It refuses with `SocketIOConnectionRefusedError` carrying a message string: the error's
  summary, or the generic internal detail for server-kind errors. No code reaches the client.
- **`attach_realtime_connection(sio, *, namespace, resolve, presence, mailbox_factory,
  cursors_factory, runtime, share_identity)`** has no admission hook (`connection.py:598-608`).
- **`_local_connections(sio, namespace)`** lists this node's sids for a namespace, through
  `sio.manager.get_participants(namespace, None)`.
- **Metrics.** Counters for emitted, failed, skipped, rejected, mailboxed, bridged and
  poisoned signals, and two backplane gauges. Nothing for connections.
- **Resilience strategies.**
  - `RateLimitStrategy` rejects at once.
  - The bulkheads limit concurrency, with queues ordered FIFO or LIFO. Their `prioritized`
    option only decides which waiter is displaced.

  None waits in priority lanes.

## 4. Goals / Non-goals

**Goals**

- A replica that is leaving closes its realtime connections spread over a window, finished
  inside the deploy's grace period, before teardown.
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

## 5. Design

### 5.1 Closing on a schedule

```python
async def close_spread(
    sio: AsyncServer,
    sids: Sequence[tuple[str, str]],     # (namespace, sid)
    window: timedelta,
    *,
    tail: timedelta,
) -> int: ...                             # closes that finished in time
```

- **Scheduling.** Close *i* of *n* starts at `i * window / n` after the call. Each starts as its
  own task, so a slow close never delays the next. After the last start, the call waits up to
  `tail` for the started closes, then cancels any still running and returns.
- **Bounds.** The whole call takes at most `window + tail`.
- **Failures.** A close that fails is logged and counted, never raised.

### 5.2 The drain step

```python
def connection_drain_lifecycle_step(
    sio: AsyncServer,
    *,
    runtime: ExecutionRuntime,
    window: timedelta,
    tail: timedelta,
    namespaces: Sequence[str] = ("/",),
    select: Callable[[str, str], bool] | None = None,   # (namespace, sid) -> close it?
    order: int = 0,
    step_id: StrKey = "realtime_connection_drain",
) -> LifecycleStep: ...
```

- **The phase.** At startup the step registers a drain phase with RFC 0074's `drain_phases`.
  When it runs, the phase lists this node's connections in the given namespaces, keeps those
  `select` accepts (all by default), and calls `close_spread`.
- **The budget.** The window is shortened to fit the budget the phase is given, less `tail`.
  If no budget is left, it closes everything at once, so a late phase never overruns the
  deploy.
- **Connects keep arriving** until the balancer has noticed that readiness failed. The
  refusal in 5.3 covers them, so the phase does not loop.

### 5.3 Refusing while draining

`on_connect` checks `runtime.draining` before negotiating the protocol. When it is true, it
refuses with `SocketIOConnectionRefusedError("Server is draining", {"code":
"realtime_draining"})`.

The data argument reaches the client as the connect error's `data`, so a client can tell this
refusal from an authentication failure and reconnect at once. Existing refusals are
unchanged: they stay message-only here, since adding codes to them changes what clients
receive.

### 5.4 Admission

`attach_realtime_connection(…, admission=…)` takes an optional
`Callable[[SocketIOConnect], Awaitable[None]]`.
- It runs after the draining check and before protocol negotiation and authentication.
- Raising a `CoreException` refuses the connect, with its summary and the reason `admission`.
- Awaiting delays the connect, so an app can pace admissions by its own rules: for example,
  hh-backend-v2's priority lanes.

### 5.5 Metrics

- **`forze.realtime.connections`:** an up-down counter, labelled by namespace. It moves on
  connect and disconnect.
- **`forze.realtime.connect_refused`:** a counter, labelled by namespace and reason. The
  reasons are `protocol`, `auth`, `draining`, `admission` and `internal`.
- **`forze.realtime.drain_closed`:** a counter of closes the drain started, labelled by
  outcome: `closed`, `failed`, `timed_out`.

### 5.6 FastAPI WebSocket and SSE

The same shape, in `forze_fastapi`:
- **WebSockets.** The WebSocket routes register each accepted socket in a per-app registry. A
  drain step closes them with code 1012 (service restart) through the same scheduling.
- **SSE.** The SSE hub ends each subscriber's stream through the same scheduling. A client
  reconnects with its last event id, as it does after any disconnect.
- **Refusals.** New WebSocket upgrades are refused with 1012, and new SSE requests with 503,
  while `runtime.draining` is true.

### 5.7 Alternatives considered

- **Close everything at once at shutdown.** Simple, and it is what happens today by default.
  It sends every client to the remaining replicas in the same second. Rejected.
- **Await each close in turn.** It does not finish in time at a few thousand sockets (D-280).
  Rejected.
- **A drainable instead of a drain phase.** Drainables stop after the gate closes, and under
  uvicorn after the server may already have closed the sockets (RFC 0074 §5.5). Rejected.
- **Codes on every refusal.** Useful, but it changes what existing clients receive. Left for
  its own change.

## 6. Tests

- **`close_spread`.**
  - 3,500 fake sids with a slow fake close finish within `window + tail`.
  - The starts are spread evenly.
  - A failing close is counted and not raised.
- **The drain step**, with RFC 0074's phases:
  - selected connections close and others stay;
  - the window shrinks to the budget;
  - with no budget left, everything closes at once.
- **Refusal.** While `runtime.draining` is true, a connect is refused with
  `{"code": "realtime_draining"}` before authentication runs.
- **Admission.** A hook that raises refuses with reason `admission`. A hook that sleeps delays
  the connect.
- **Metrics.** The connection count follows connects and disconnects, and refusals are counted
  by reason.
- **FastAPI.**
  - Open WebSockets close with 1012 over the window, and new upgrades are refused while
    draining.
  - SSE streams end over the window, and new requests get 503.
- **Integration.** A real python-socketio server and client: SIGTERM through RFC 0074's helper
  closes connections over the window, and the client reconnects elsewhere.

## 7. Docs

- **The socket.io page:** draining a replica, the window and the tail against the grace
  period, the refusal code for clients, admission, and the metrics.
- **The FastAPI realtime page:** the WebSocket and SSE drain.

## 8. Out of scope

- **Priority-lane pacing.** The hook makes it possible; a forze implementation waits for a
  second app that needs it.
- **Rebalancing** (RFC 0076).
- **Codes on existing refusals.**

## 9. Risks

- **A window longer than the grace period.** The phase shortens it to its budget, and RFC
  0074's docs give the arithmetic.
- **Reading python-socketio's manager.** `get_participants` is how forze lists sids today. A
  manager that changes it breaks the listing, and the test against the library's own manager
  catches it.
- **A reconnect storm moved, not removed.** Spreading the closes over the window spreads the
  reconnects too. A balancer that sends them to one replica still overloads it. The docs
  recommend a least-connections balancer.

## 10. Unresolved questions

- Whether the WebSocket registry belongs to the routes or to a small connection-tracking
  helper shared with the SSE hub. Implementation decides.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Connections close in a drain phase (RFC 0074), not a drainable, so they close while the replica still serves and before any server-side teardown. |
| 2 | `LOCKED` | `close_spread` starts each close on its own schedule over the window and never awaits closes in turn. The whole call is bounded by `window + tail`. |
| 3 | `ASSUMED` | The phase shortens its window to the budget it is given, and closes everything at once when none is left. |
| 4 | `ASSUMED` | `on_connect` refuses while `runtime.draining`, before negotiation and authentication, with data `{"code": "realtime_draining"}`. Existing refusals are unchanged. |
| 5 | `ASSUMED` | `admission` runs after the draining check and before authentication, and refuses on a `CoreException`. Awaiting it delays the connect. |
| 6 | `ASSUMED` | Metrics: a connection count per namespace, refused connects by reason, and drain closes by outcome. |
| 7 | `ASSUMED` | FastAPI WebSockets close with 1012 and SSE streams end over the same schedule. While draining, new upgrades get 1012 and new SSE requests get 503. |

## 12. Phasing

1. **socket.io:** `close_spread`, the drain step, the draining refusal and the metrics, once
   RFC 0074's phases ship.
2. **The admission hook.**
3. **FastAPI WebSocket and SSE.**
