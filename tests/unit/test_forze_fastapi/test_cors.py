"""Cross-origin browsers can read the headers forze sets, from the origins an allowlist names.

Without ``Access-Control-Expose-Headers`` a cross-origin browser sees none of forze's own
response headers (the error code, the request and correlation ids, a download's filename).
"""

from __future__ import annotations

from typing import Any

import attrs
import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from forze.base.exceptions import CoreException
from forze_fastapi.exceptions import ERROR_CODE_HEADER
from forze_fastapi.middlewares import (
    EXPOSED_RESPONSE_HEADERS,
    AllowlistCORSMiddleware,
    InvocationMetadataMiddleware,
)

# ----------------------- #


def _app(**cors: Any) -> FastAPI:
    app = FastAPI()

    @app.get("/ok")
    async def ok() -> dict[str, int]:
        return {"ok": 1}

    @app.get("/boom")
    async def boom() -> None:
        raise HTTPException(status_code=400, headers={ERROR_CODE_HEADER: "bad"})

    app.add_middleware(
        AllowlistCORSMiddleware,  # type: ignore[arg-type]
        allowed_origins=["https://app.example.com", "http://localhost:5173-5199"],
        allow_credentials=True,
        **cors,
    )

    return app


async def _get(app: FastAPI, path: str, origin: str) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://api") as client:
        return await client.get(path, headers={"Origin": origin})


async def _preflight(app: FastAPI, origin: str) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://api") as client:
        return await client.options(
            "/ok", headers={"Origin": origin, "Access-Control-Request-Method": "GET"}
        )


# ....................... #


def test_the_list_names_every_header_forze_sets() -> None:
    names = {name.lower() for name in EXPOSED_RESPONSE_HEADERS}
    invocation = attrs.fields(InvocationMetadataMiddleware)

    for header in (
        ERROR_CODE_HEADER,
        invocation.exec_header.default,
        invocation.corr_header.default,
        invocation.caus_header.default,
        "Content-Disposition",
        "ETag",
        "Accept-Ranges",
        "Content-Range",
    ):
        assert str(header).lower() in names


@pytest.mark.parametrize("origin", ["https://app.example.com", "http://localhost:5174"])
async def test_an_allowed_origin_reads_forzes_headers(origin: str) -> None:
    response = await _get(_app(), "/ok", origin)

    assert response.headers["access-control-allow-origin"] == origin
    assert response.headers["access-control-allow-credentials"] == "true"

    exposed = {h.strip().lower() for h in response.headers["access-control-expose-headers"].split(",")}

    assert exposed == {name.lower() for name in EXPOSED_RESPONSE_HEADERS}

    preflight = await _preflight(_app(), origin)

    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == origin


@pytest.mark.parametrize(
    "origin",
    ["https://evil.example.com", "http://localhost:5200", "https://app.example.com.evil.io"],
)
async def test_another_origin_is_not_answered(origin: str) -> None:
    response = await _get(_app(), "/ok", origin)

    assert "access-control-allow-origin" not in response.headers
    assert (await _preflight(_app(), origin)).status_code == 400


async def test_an_error_response_exposes_its_code() -> None:
    response = await _get(_app(), "/boom", "https://app.example.com")

    assert response.headers[ERROR_CODE_HEADER] == "bad"
    assert ERROR_CODE_HEADER.lower() in response.headers["access-control-expose-headers"].lower()


async def test_an_app_can_expose_its_own_headers_too() -> None:
    app = _app(expose_headers=(*EXPOSED_RESPONSE_HEADERS, "X-Mine"))
    response = await _get(app, "/ok", "https://app.example.com")

    assert "x-mine" in response.headers["access-control-expose-headers"].lower()


def test_an_origin_that_is_not_one_is_refused_at_startup() -> None:
    with pytest.raises(CoreException, match="allowed_origins"):
        AllowlistCORSMiddleware(FastAPI(), allowed_origins=["https://app.example.com/path"])


@pytest.mark.parametrize(
    "setting", [{"allow_origins": ["*"]}, {"allow_origin_regex": r"https://.*\.example\.com"}]
)
def test_another_way_to_name_origins_is_refused(setting: dict[str, Any]) -> None:
    """Starlette's own origin settings would bypass the allowlist (``*`` answers every origin)
    or be silently ignored; the allowlist is the one place origins are named."""

    with pytest.raises(CoreException, match="allowed_origins"):
        AllowlistCORSMiddleware(FastAPI(), allowed_origins=["https://app.example.com"], **setting)
