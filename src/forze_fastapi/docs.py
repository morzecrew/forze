from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import re
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from pydantic import SecretStr
from scalar_fastapi import (
    AgentScalarConfig,
    DocumentDownloadType,
    Theme,
    get_scalar_api_reference,  # pyright: ignore[reportUnknownVariableType]
)

# ----------------------- #

RESPONSE_WRAP_CUSTOM_CSS = """
/* Target response body — several Scalar versions / layouts */
[data-testid="response-body-raw"] .cm-editor,
[data-testid="response-body-raw"] .cm-scroller,
[data-testid="response-body-raw"] .cm-content,
.body-raw-scroller .cm-editor,
.body-raw-scroller .cm-scroller,
.body-raw-scroller .cm-content {
  min-width: 0 !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
}
[data-testid="response-body-raw"] .cm-scroller,
.body-raw-scroller .cm-scroller {
  overflow-x: hidden !important;
}
[data-testid="response-body-raw"] .cm-content,
[data-testid="response-body-raw"] .cm-line,
[data-testid="response-body-raw"] .cm-line > span,
.body-raw-scroller .cm-content,
.body-raw-scroller .cm-line,
.body-raw-scroller .cm-line > span {
  white-space: pre-wrap !important;
  overflow-wrap: anywhere !important;
  word-break: break-word !important;
}
"""

CUSTOM_CSS = f"""
{RESPONSE_WRAP_CUSTOM_CSS}
"""

# ....................... #


def _is_valid_dns(address: str) -> bool:
    """Return ``True`` if *address* matches a valid DNS hostname pattern."""

    dns_pattern = re.compile(r"^([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$")

    return bool(dns_pattern.match(address))


# ....................... #


def scalar_docs(
    request: Request,
    title: str | None = None,
    favicon_url: str = "https://fastapi.tiangolo.com/img/icon-white.svg",
    version: str = "1.41.0",
    custom_css: str | None = None,
    telemetry: bool = False,
    agent_enabled: bool = False,
    agent_key: str | SecretStr | None = None,
    show_devtools: Literal["always", "never", "localhost"] = "never",
    hide_download_button: bool = True,
    download_type: DocumentDownloadType = DocumentDownloadType.BOTH,
    theme: Theme = Theme.PURPLE,
) -> HTMLResponse:
    """Return a Scalar API reference HTML page for the current OpenAPI spec."""

    root_path = request.scope.get("root_path")
    host = request.headers.get("x-forwarded-host")

    if not host:
        servers = []

    else:
        proto = "http"

        if _is_valid_dns(host):
            proto = "https"

        servers = [{"url": f"{proto}://{host}{root_path}"}]

    favicon_host_split = favicon_url.split("://")

    if len(favicon_host_split) == 1:
        favicon_url = f"{root_path}/{favicon_url.lstrip('/')}"

    if isinstance(agent_key, SecretStr):
        agent_key = agent_key.get_secret_value()

    return get_scalar_api_reference(
        title=title,
        openapi_url=f"{root_path}/openapi.json",
        hide_download_button=hide_download_button,
        hide_models=True,
        document_download_type=download_type,
        servers=servers,
        scalar_favicon_url=favicon_url,
        scalar_js_url=f"https://cdn.jsdelivr.net/npm/@scalar/api-reference@{version}",
        show_developer_tools=show_devtools,
        telemetry=telemetry,
        theme=theme,
        hide_dark_mode_toggle=True,
        hidden_clients=True,
        hide_client_button=True,
        persist_auth=True,
        custom_css=custom_css or CUSTOM_CSS,
        agent=AgentScalarConfig(
            disabled=not agent_enabled,
            key=agent_key,
        ),
    )


# ....................... #


def register_scalar_docs(
    app: FastAPI,
    *,
    path: str = "/docs",
    favicon_url: str = "https://fastapi.tiangolo.com/img/icon-white.svg",
    scalar_version: str = "1.57.0",
    custom_css: str | None = None,
    telemetry: bool = False,
    agent_enabled: bool = False,
    agent_key: str | SecretStr | None = None,
    show_devtools: Literal["always", "never", "localhost"] = "never",
    hide_download_button: bool = True,
    download_type: DocumentDownloadType = DocumentDownloadType.BOTH,
    theme: Theme = Theme.PURPLE,
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
            agent_enabled=agent_enabled,
            agent_key=agent_key,
            show_devtools=show_devtools,
            hide_download_button=hide_download_button,
            download_type=download_type,
            theme=theme,
        )
