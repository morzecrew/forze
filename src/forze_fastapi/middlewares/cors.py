"""CORS for a browser front end on another origin, answering the origins an allowlist names."""

from collections.abc import Collection, Iterable
from typing import Any, Final

from starlette.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp

from ..exceptions import ERROR_CODE_HEADER
from ..security.value_objects import OriginAllowlist

# ----------------------- #

EXPOSED_RESPONSE_HEADERS: Final[tuple[str, ...]] = (
    ERROR_CODE_HEADER,
    "X-Request-ID",
    "X-Correlation-ID",
    "X-Causation-ID",
    "Content-Disposition",
    "ETag",
)
"""Response headers forze sets that a cross-origin browser can read only once exposed: the
error code, the invocation ids (:class:`InvocationMetadataMiddleware`'s default names), and a
download's filename and validator. An app that renames the invocation headers, or sets its own,
passes its list as ``expose_headers``."""

# ....................... #


class AllowlistCORSMiddleware(CORSMiddleware):
    """Starlette's ``CORSMiddleware``, answering the origins an :class:`OriginAllowlist` names
    and exposing :data:`EXPOSED_RESPONSE_HEADERS` by default.

    *allowed_origins* takes the allowlist's entries: exact origins, or a port range or ``*`` on
    a loopback host (``http://localhost:5173-5199``) for a dev server. Every other keyword is
    ``CORSMiddleware``'s.

    :raises CoreException: ``configuration`` for an entry that is not an origin.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        allowed_origins: Iterable[str],
        expose_headers: Collection[str] = EXPOSED_RESPONSE_HEADERS,
        **kwargs: Any,
    ) -> None:
        self.allowlist = OriginAllowlist.parse(allowed_origins)
        super().__init__(app, expose_headers=expose_headers, **kwargs)

    # ....................... #

    def is_allowed_origin(self, origin: str) -> bool:
        return self.allowlist.allows(origin)
