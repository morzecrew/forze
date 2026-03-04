from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import re
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from scalar_fastapi import (
    Theme,
    get_scalar_api_reference,  # pyright: ignore[reportUnknownVariableType]
)

# ----------------------- #


def _is_valid_dns(address: str) -> bool:
    """
    Check if the address is a valid DNS

    Args:
        address (str): The address to check

    Returns:
        res (bool): True if the address is a valid DNS, False otherwise
    """

    dns_pattern = re.compile(r"^([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$")

    return bool(dns_pattern.match(address))


# ....................... #


def scalar_docs(
    request: Request,
    title: Optional[str] = None,
    favicon_url: str = "https://fastapi.tiangolo.com/img/icon-white.svg",
    version: str = "1.41.0",
):
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

    return get_scalar_api_reference(
        title=title,
        openapi_url=f"{root_path}/openapi.json",
        hide_download_button=True,
        hide_models=True,
        servers=servers,
        scalar_favicon_url=favicon_url,
        scalar_js_url=f"https://cdn.jsdelivr.net/npm/@scalar/api-reference@{version}",
        show_developer_tools="never",
        telemetry=False,
        theme=Theme.PURPLE,
        hide_dark_mode_toggle=True,
        hidden_clients=True,
        hide_client_button=True,
        persist_auth=True,
    )


# ....................... #


def register_scalar_docs(
    app: FastAPI,
    *,
    path: str = "/docs",
    favicon_url: str = "https://fastapi.tiangolo.com/img/icon-white.svg",
    scalar_version: str = "1.41.0",
):
    @app.get(path, include_in_schema=False)
    def docs_route(  # pyright: ignore[reportUnusedFunction]
        request: Request,
    ) -> HTMLResponse:
        return scalar_docs(
            request,
            title=app.title,
            favicon_url=favicon_url,
            version=scalar_version,
        )
