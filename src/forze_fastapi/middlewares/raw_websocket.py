from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from starlette.types import Receive, Scope, Send

# ----------------------- #

WS_POLICY_VIOLATION: int = 1008
"""RFC 6455 close code sent when a raw websocket scope is refused."""


async def refuse_raw_websocket(scope: Scope, receive: Receive, send: Send) -> None:
    """Refuse a raw ``websocket`` scope at the ASGI level.

    Governed middlewares resolve identity, tenancy, and the invocation envelope
    for HTTP scopes only — a raw websocket route mounted on the same app would
    silently run without any of it. Instead of passing such scopes through
    unauthenticated, the middlewares fail closed and close the handshake with a
    policy violation (servers surface this as a 403-rejected upgrade). Apps that
    deliberately self-manage websocket routes opt out per middleware with
    ``allow_raw_websockets=True`` and own identity, tenancy, and error shaping
    on every websocket route themselves.
    """

    # Sent before ``websocket.accept``: ASGI servers reject the upgrade handshake.
    await send(
        {
            "type": "websocket.close",
            "code": WS_POLICY_VIOLATION,
            "reason": "raw websocket ingress is disabled (allow_raw_websockets=False)",
        }
    )
