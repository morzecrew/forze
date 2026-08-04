"""The ``/_mock`` control plane: provoke states instead of waiting for them.

This is the difference between a served mock and a schema mock, and the reason a container
beats a browser worker: a frontend developer needs to *deterministically* reach the empty
state, the 409, the slow request and the expired token — not to observe whatever traffic
happens to occur.

Plain Starlette routes on purpose. ``forze_mock`` must not import a sibling integration
package (a transport's route generators here would invert the layering for every consumer of
the mock), so the control plane owns third-party routing and nothing of the app's.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError as PydanticValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from forze.application.contracts.interception import PortSelector
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.exceptions.mapping import map_pydantic

from .faults import ArmedFault, ArmedLatency

if TYPE_CHECKING:
    from .session import MockSession

# ----------------------- #

_INSPECTABLE_STORES = (
    "documents",
    "queues",
    "streams",
    "pubsub_logs",
    "storage",
    "cache_kv",
    "counters",
    "inbox",
    "outbox_rows",
    "identity",
    "analytics_ingest_log",
)
"""Stores worth a debugging peek. An allowlist, not ``getattr`` on anything named: the state
object holds locks and byte payloads that neither render nor belong in a response."""

# ....................... #


def _jsonable(value: Any) -> Any:
    """Render a mock store for inspection — lossy on purpose, never raising."""

    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]

    if isinstance(value, list | tuple | set | frozenset):
        return [_jsonable(item) for item in value]  # pyright: ignore[reportUnknownVariableType]

    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"

    if isinstance(value, str | int | float | bool) or value is None:
        return value

    return str(value)


# ....................... #


async def _body(request: Request) -> dict[str, Any]:
    """Parse a JSON object body, tolerating an empty one.

    Every way a body can be wrong answers 422. A malformed one reaching the client as a 500
    would say the *server* broke, which sends a frontend developer looking in the wrong place.
    """

    raw = await request.body()

    if not raw:
        return {}

    try:
        payload = await request.json()

    except JSONDecodeError as error:
        raise exc.validation(f"Control-plane body is not valid JSON: {error}") from error

    if not isinstance(payload, dict):
        raise exc.validation("Control-plane bodies must be JSON objects")

    return payload  # pyright: ignore[reportUnknownVariableType]


# ....................... #


def _selector(payload: dict[str, Any]) -> PortSelector:
    """The port-call selector a fault or latency body describes.

    ``route`` is the spec name, so ``{"route": "notes", "op": "update"}`` is the natural
    spelling of "the notes update operation" and matches both its query and command ports.
    """

    return PortSelector(
        surface=payload.get("surface"),
        route=payload.get("route"),
        op=payload.get("op"),
    )


# ....................... #


def _kind(payload: dict[str, Any]) -> ExceptionKind:
    """The declared exception kind, refused by name rather than defaulted."""

    raw = payload.get("kind")

    if not raw:
        raise exc.validation("A fault needs a 'kind'")

    try:
        return ExceptionKind(str(raw))

    except ValueError as error:
        allowed = ", ".join(sorted(kind.value for kind in ExceptionKind))

        raise exc.validation(
            f"Unknown exception kind {raw!r}; expected one of: {allowed}"
        ) from error


# ....................... #


def _seconds(raw: Any, *, field: str = "seconds") -> float:
    """A duration in seconds, refused by value rather than blowing up on conversion."""

    try:
        return float(raw)

    except (TypeError, ValueError) as error:
        raise exc.validation(f"{field.capitalize()} must be a number, got {raw!r}") from error


# ....................... #


def _times(raw: Any) -> int | None:
    """How many calls a fault fires on — ``None`` meaning "until disarmed".

    Refused here rather than at the call it would fire on: an unusable ``times`` stored now
    raises inside the *app's* port chain later, which reads as an app bug.
    """

    if raw is None:
        return None

    try:
        times = int(raw)

    except (TypeError, ValueError) as error:
        raise exc.validation(f"A fault's 'times' must be an integer, got {raw!r}") from error

    if times < 1:
        raise exc.validation(f"A fault's 'times' must be at least 1, got {times}")

    return times


# ....................... #


def _instant(raw: Any) -> datetime | None:
    """Parse an ISO-8601 instant, reading a naive one as UTC (never as local time)."""

    if raw is None:
        return None

    try:
        parsed = datetime.fromisoformat(str(raw))

    except ValueError as error:
        raise exc.validation(f"Not an ISO-8601 instant: {raw!r}") from error

    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


# ....................... #


def build_control_app(session: MockSession) -> Starlette:
    """The control-plane ASGI app for *session*.

    Takes a :class:`MockSession` and not a runtime plus a flag: only the mock server can
    mint one, so these routes cannot be mounted on a production app by configuration.
    """

    async def health(_request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "mock": True,
                "warning": "in-memory mock server — not a production backend",
                "seeded": session.seed is not None,
                "clock": session.clock.now().isoformat(),
                "armed_faults": len(session.board.faults),
                "armed_latencies": len(session.board.latencies),
            }
        )

    async def reset(_request: Request) -> JSONResponse:
        """Back to the pristine seed: state cleared, everything disarmed, seed re-applied."""

        session.state.clear()
        session.board.clear()
        seeded = await _apply_seed(session)

        return JSONResponse({"reset": True, "seeded": seeded})

    async def seed(request: Request) -> JSONResponse:
        """Re-apply the declared plan (optionally onto the current state)."""

        payload = await _body(request)

        if payload.get("reset", False):
            session.state.clear()

        seeded = await _apply_seed(session)

        return JSONResponse({"seeded": seeded})

    async def state(request: Request) -> JSONResponse:
        store = request.path_params["store"]

        if store not in _INSPECTABLE_STORES:
            raise exc.not_found(
                f"Unknown store {store!r}; inspectable: {', '.join(_INSPECTABLE_STORES)}"
            )

        # Strict, not `getattr(..., None)`: an allowlisted name that is not a field of
        # MockState means the allowlist has drifted from the state object, and answering
        # `null` would read as "that store is empty" — the one answer a debugging aid must
        # never invent.
        if not hasattr(session.state, store):
            raise exc.configuration(
                f"Store {store!r} is inspectable but is not a field of MockState — "
                "the allowlist has drifted from the state object"
            )

        return JSONResponse({store: _jsonable(getattr(session.state, store))})

    async def fault(request: Request) -> JSONResponse:
        payload = await _body(request)
        armed = ArmedFault(
            selector=_selector(payload),
            kind=_kind(payload),
            summary=str(payload.get("summary", "Injected by the mock control plane")),
            code=payload.get("code"),
            remaining=_times(payload.get("times")),
        )
        session.board.arm_fault(armed)

        return JSONResponse({"armed": "fault", "kind": armed.kind.value}, status_code=201)

    async def latency(request: Request) -> JSONResponse:
        payload = await _body(request)
        seconds = _seconds(payload.get("seconds", 0))

        if seconds < 0:
            raise exc.validation("Latency seconds must not be negative")

        session.board.arm_latency(ArmedLatency(selector=_selector(payload), seconds=seconds))

        return JSONResponse({"armed": "latency", "seconds": seconds}, status_code=201)

    async def disarm(_request: Request) -> JSONResponse:
        session.board.clear()

        return JSONResponse({"disarmed": True})

    async def time_control(request: Request) -> JSONResponse:
        """Freeze, advance or resume the server clock — TTLs, schedules, expiry screens."""

        payload = await _body(request)
        action = str(payload.get("action", "freeze"))

        if action == "freeze":
            now = session.clock.freeze(_instant(payload.get("instant") or None))

        elif action == "advance":
            now = session.clock.advance(timedelta(seconds=_seconds(payload.get("seconds", 0))))

        elif action == "resume":
            now = session.clock.resume()

        else:
            raise exc.validation(f"Unknown time action {action!r}; expected freeze/advance/resume")

        return JSONResponse({"now": now.isoformat(), "frozen": session.clock.frozen_at is not None})

    async def emit(request: Request) -> JSONResponse:
        """Fire one signal at one audience — the thing a notification badge needs."""

        from forze.application.contracts.realtime import RealtimeSignal

        if session.on_emit is None:
            raise exc.precondition(
                "This mock server cannot emit: the realtime egress plane lives above "
                "forze_mock, so the app supplies it via MockApp(on_emit=...)"
            )

        payload = await _body(request)
        signal = RealtimeSignal.model_validate(payload)

        await session.on_emit(session.runtime.get_context(), signal)

        return JSONResponse({"emitted": signal.event}, status_code=202)

    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/reset", reset, methods=["POST"]),
        Route("/seed", seed, methods=["POST"]),
        Route("/state/{store}", state, methods=["GET"]),
        Route("/fault", fault, methods=["POST"]),
        Route("/latency", latency, methods=["POST"]),
        Route("/disarm", disarm, methods=["POST"]),
        Route("/time", time_control, methods=["POST"]),
        Route("/emit", emit, methods=["POST"]),
    ]

    control = Starlette(routes=routes)
    control.add_exception_handler(CoreException, _core_exception_response)
    control.add_exception_handler(PydanticValidationError, _pydantic_validation_response)

    return control


# ....................... #


async def _apply_seed(session: MockSession) -> int:
    """Apply the session's plan, if it has one; return how many documents it created.

    Uses the scope the server's lifespan already holds open. Opening another would raise —
    the runtime keeps one execution context per process — and would be wrong anyway: the
    control plane acts on the *running* server, not on a private copy of it.
    """

    if session.seed is None:
        return 0

    from forze_mock.seeding import apply_seed

    result = await apply_seed(session.runtime.get_context(), session.seed)

    return result.total


# ....................... #


def _core_exception_response(_request: Request, error: Exception) -> JSONResponse:
    """Map a control-plane error to a status, independently of the app's own handlers.

    The control plane is mounted beside the app, not inside it, so it cannot borrow the
    app's exception handlers — and should not: these are the *tool's* errors, not the
    domain's.
    """

    if not isinstance(error, CoreException):  # pragma: no cover - registered for CoreException
        raise error

    status = {
        ExceptionKind.NOT_FOUND: 404,
        ExceptionKind.VALIDATION: 422,
        ExceptionKind.PRECONDITION: 400,
        ExceptionKind.CONFIGURATION: 500,
    }.get(error.kind, 400)

    body: dict[str, Any] = {"error": error.summary, "code": error.code}

    if error.details:
        body["details"] = error.details

    return JSONResponse(body, status_code=status)


# ....................... #


def _pydantic_validation_response(request: Request, error: Exception) -> JSONResponse:
    """Answer a malformed control-plane model with the 422 the body deserves.

    A route that hands a raw payload to ``model_validate`` raises pydantic's own error, which
    is not a :class:`CoreException` and would otherwise surface as a 500. Mapped through the
    framework's own mapper, so the field details are scrubbed the same way they are anywhere
    else rather than echoed back raw.
    """

    mapped = map_pydantic(error, site="forze_mock.server.control")

    if mapped is None:  # pragma: no cover - registered for PydanticValidationError
        raise error

    return _core_exception_response(request, mapped)
