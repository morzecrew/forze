"""CORS for a browser front end on another origin, answering the origins an allowlist names."""

from collections.abc import Collection, Iterable
from typing import Any, Final

from starlette.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp

from forze.base.exceptions import exc

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
    "Accept-Ranges",
    "Content-Range",
    "Retry-After",
)
"""Response headers forze sets that a cross-origin browser can read only once exposed: the
error code, the invocation ids (:class:`InvocationMetadataMiddleware`'s default names), a
download's filename, validator and range headers, and a throttled error's ``Retry-After``.
``Last-Modified`` and ``Content-Length`` are
safelisted, so a browser reads them anyway. An app that renames the invocation headers, or sets its own,
passes its list as ``expose_headers``."""

# ....................... #


class AllowlistCORSMiddleware(CORSMiddleware):
    """Starlette's ``CORSMiddleware``, answering the origins an :class:`OriginAllowlist` names
    and exposing :data:`EXPOSED_RESPONSE_HEADERS` by default.

    *allowed_origins* takes the allowlist's entries: exact origins, or a port range or ``*`` on
    a loopback host (``http://localhost:5173-5199``) for a dev server. Every other keyword is
    ``CORSMiddleware``'s, except its own ``allow_origins`` and ``allow_origin_regex``.

    :raises CoreException: ``configuration`` for an entry that is not an origin, or for
        ``allow_origins`` / ``allow_origin_regex``: Starlette answers ``*`` for every origin
        under ``allow_origins=["*"]`` before asking the allowlist, and would ignore the regex.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        allowed_origins: Iterable[str],
        expose_headers: Collection[str] = EXPOSED_RESPONSE_HEADERS,
        **kwargs: Any,
    ) -> None:
        if named := sorted({"allow_origins", "allow_origin_regex"} & kwargs.keys()):
            raise exc.configuration(
                f"AllowlistCORSMiddleware takes its origins as allowed_origins, not "
                f"{', '.join(named)}",
            )

        self.allowlist = OriginAllowlist.parse(allowed_origins)
        super().__init__(app, expose_headers=expose_headers, **kwargs)

    # ....................... #

    def is_allowed_origin(self, origin: str) -> bool:
        return self.allowlist.allows(origin)
