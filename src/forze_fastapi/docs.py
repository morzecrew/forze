from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import re
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from scalar_fastapi import (
    AgentScalarConfig,
    DocumentDownloadType,
    Theme,
    get_scalar_api_reference,  # pyright: ignore[reportUnknownVariableType]
)

# ----------------------- #

DISABLE_MCP_CUSTOM_CSS = """
.scalar-mcp-layer {
  display: none !important;
}
"""

CUSTOM_CSS = f"""
{DISABLE_MCP_CUSTOM_CSS}
"""

# Length-bounded per label (<=63) and TLD, matching DNS limits — accepts every valid
# hostname while keeping backtracking strictly linear (no nested-quantifier ReDoS).
_DNS_PATTERN = re.compile(r"^(?:[a-zA-Z0-9-]{1,63}\.)+[a-zA-Z]{2,63}$")

# ....................... #


def _is_valid_dns(address: str) -> bool:
    """Return ``True`` if *address* matches a valid DNS hostname pattern."""

    return bool(_DNS_PATTERN.match(address))


# ....................... #


def _forwarded_proto(request: Request) -> str | None:
    raw = request.headers.get("x-forwarded-proto")

    if raw is None:
        return None

    proto = raw.split(",", 1)[0].strip().lower()

    if proto in ("http", "https"):
        return proto

    return None


# ....................... #


def _scalar_servers_from_request(
    request: Request,
    *,
    trust_forwarded_host: bool,
) -> list[dict[str, str]]:
    """Build Scalar ``servers`` from forwarded headers when explicitly trusted."""

    if not trust_forwarded_host:
        return []

    host = request.headers.get("x-forwarded-host")

    if not host:
        return []

    host = host.split(",", 1)[0].strip()
    root_path = request.scope.get("root_path") or ""
    proto = _forwarded_proto(request)

    if proto is None:
        proto = "https" if _is_valid_dns(host) else "http"

    return [{"url": f"{proto}://{host}{root_path}"}]


# ....................... #

DownloadType = Literal["json", "yaml", "both", "none"]
ThemeType = Literal[
    "default",
    "purple",
    "solarized",
    "laserwave",
    "bluePlanet",
    "saturn",
    "kepler",
    "mars",
    "deepSpace",
    "none",
]


def scalar_docs(
    request: Request,
    title: str | None = None,
    favicon_url: str = "https://fastapi.tiangolo.com/img/icon-white.svg",
    version: str = "1.41.0",
    custom_css: str | None = None,
    telemetry: bool = False,
    show_devtools: Literal["always", "never", "localhost"] = "never",
    hide_download_button: bool = True,
    download_type: DownloadType = "both",
    theme: ThemeType = "purple",
    *,
    persist_auth: bool = False,
    trust_forwarded_host: bool = False,
) -> HTMLResponse:
    """Return a Scalar API reference HTML page for the current OpenAPI spec."""

    root_path = request.scope.get("root_path")
    servers = _scalar_servers_from_request(
        request,
        trust_forwarded_host=trust_forwarded_host,
    )

    favicon_host_split = favicon_url.split("://")

    if len(favicon_host_split) == 1:
        favicon_url = f"{root_path}/{favicon_url.lstrip('/')}"

    document_download_type = DocumentDownloadType(download_type)
    theme_ = Theme(theme)

    res = get_scalar_api_reference(
        title=title,
        openapi_url=f"{root_path}/openapi.json",
        hide_download_button=hide_download_button,
        hide_models=True,
        document_download_type=document_download_type,
        servers=servers,
        scalar_favicon_url=favicon_url,
        scalar_js_url=f"https://cdn.jsdelivr.net/npm/@scalar/api-reference@{version}",
        show_developer_tools=show_devtools,
        telemetry=telemetry,
        theme=theme_,
        hide_dark_mode_toggle=True,
        hidden_clients=True,
        hide_client_button=True,
        persist_auth=persist_auth,
        custom_css=custom_css or CUSTOM_CSS,
        agent=AgentScalarConfig(disabled=True),
    )

    return res


# ....................... #


def register_scalar_docs(
    app: FastAPI,
    *,
    path: str = "/docs",
    favicon_url: str = "https://fastapi.tiangolo.com/img/icon-white.svg",
    scalar_version: str = "1.57.0",
    custom_css: str | None = None,
    telemetry: bool = False,
    show_devtools: Literal["always", "never", "localhost"] = "never",
    hide_download_button: bool = True,
    download_type: DownloadType = "both",
    theme: ThemeType = "purple",
    persist_auth: bool = False,
    trust_forwarded_host: bool = False,
) -> None:
    """Register a Scalar docs route on *app* at *path*."""

    @app.get(path, include_in_schema=False)
    def docs_route(  # pyright: ignore[reportUnusedFunction]
        request: Request,
    ) -> HTMLResponse:
        return scalar_docs(
            request,
            title=app.title,
            favicon_url=favicon_url,
            version=scalar_version,
            custom_css=custom_css,
            telemetry=telemetry,
            show_devtools=show_devtools,
            hide_download_button=hide_download_button,
            download_type=download_type,
            theme=theme,
            persist_auth=persist_auth,
            trust_forwarded_host=trust_forwarded_host,
        )
