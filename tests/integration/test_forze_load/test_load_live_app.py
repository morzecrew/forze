"""``forze_load`` against a real forze FastAPI app served by uvicorn on a socket.

The app is the mock-server recipe: generated document routes and API-key authn, on
in-memory backends. It runs in a thread of this process, which is why the client's
saturation is judged by its event-loop thread's CPU and lag, not by the process's.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import attrs
import httpx
import pytest
import uvicorn
from fastapi import Request

from examples.recipes.mock_server.app import build_server
from forze.base.exceptions import CoreException
from forze_load import (
    WRITE_HOSTS_ENV,
    Call,
    ClosedRamp,
    LoadPlan,
    Principal,
    Scenario,
    api_key,
    run,
)

pytestmark = pytest.mark.integration

_KEY_ENV = "FORZE_LOAD_TEST_API_KEY"
_RECIPE_KEY = "dev-key"  # the recipe's own identity.local.json; not a secret

# ----------------------- #


@attrs.define
class _Served:
    port: int
    requests: list[str]

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Served]:
    monkeypatch.setenv(_KEY_ENV, _RECIPE_KEY)
    app = build_server()
    requests: list[str] = []

    @app.middleware("http")
    async def count(request: Request, call_next):  # type: ignore[no-untyped-def]
        requests.append(request.url.path)

        return await call_next(request)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on"))
    thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[sock])), daemon=True)
    thread.start()

    try:
        deadline = time.perf_counter() + 15

        while not server.started:
            assert time.perf_counter() < deadline, "uvicorn did not start"
            time.sleep(0.02)

        yield _Served(port=sock.getsockname()[1], requests=requests)

    finally:
        server.should_exit = True
        thread.join(timeout=15)
        sock.close()


async def _list(call: Call) -> httpx.Response:
    return await call.request("POST", "/products/list", json={})


async def _create(call: Call) -> httpx.Response:
    return await call.request(
        "POST", "/products", json={"name": f"load-{call.rng.randrange(10**6)}", "price": 1}
    )


async def _burn_then_list(call: Call) -> httpx.Response:
    end = time.perf_counter() + 0.03  # 30 ms of client CPU on the event loop per call

    while time.perf_counter() < end:
        pass

    return await _list(call)


def _plan(base_url: str, *scenarios: Scenario, **kw: object) -> LoadPlan:
    mix = {scenario.name: 1 for scenario in scenarios}

    return LoadPlan(
        base_url=base_url,
        principals=[Principal(name="viewer", mix=mix, auth=api_key(_KEY_ENV))],
        scenarios=list(scenarios),
        **kw,  # type: ignore[arg-type]
    )


# ....................... #


async def test_a_ramp_reads_the_app_over_a_socket(served: _Served, tmp_path: Path) -> None:
    out = tmp_path / "ramp.jsonl"

    result = await run(
        _plan(served.base_url, Scenario("list", _list, writes=False)),
        ClosedRamp(steps=[1, 2, 4], step_s=1.0, warm_s=0.2, think_s=0.01),
        out=out,
    )

    assert [row["workers"] for row in result.rows] == [1, 2, 4]

    for row in result.rows:
        assert row["outcomes"]["ok"] > 0
        assert row["outcomes"]["forbidden"] == 0
        assert row["outcomes"]["server_error"] == 0
        assert row["ms"]["p50"] is not None

    assert served.requests.count("/products/list") >= sum(r["calls"] for r in result.rows)
    kinds = [json.loads(line)["kind"] for line in out.read_text().splitlines()]
    assert kinds == ["meta", "step", "step", "step", "summary"]
    assert _RECIPE_KEY not in out.read_text()


async def test_writes_run_against_loopback_once_allowed(served: _Served) -> None:
    result = await run(
        _plan(served.base_url, Scenario("create", _create, writes=True), allow_writes=True),
        ClosedRamp(steps=[2], step_s=0.6, think_s=0.01),
    )

    assert result.rows[0]["outcomes"]["ok"] > 0
    assert result.rows[0]["outcomes"]["client_error"] == 0


async def test_the_guard_refuses_a_host_outside_the_allowlist(
    served: _Served, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(WRITE_HOSTS_ENV, "load-target.invalid.example")
    before = len(served.requests)

    with pytest.raises(CoreException) as error:
        await run(
            _plan(
                f"http://load-target.invalid:{served.port}",
                Scenario("create", _create, writes=True),
                allow_writes=True,
            ),
            ClosedRamp(steps=[1], step_s=0.5),
        )

    assert error.value.code == "load_write_host_refused"
    assert len(served.requests) == before  # refused before anything was sent


async def test_the_read_sweep_resolves_against_the_app_s_own_openapi(served: _Served) -> None:
    async with httpx.AsyncClient(base_url=served.base_url) as seeding:
        for n in range(3):
            created = await seeding.post(
                "/products",
                json={"name": f"seed-{n}", "price": 1},
                headers={"X-API-Key": _RECIPE_KEY},
            )
            assert created.status_code == 201

    result = await run(
        _plan(served.base_url, Scenario.reads("products.")),
        ClosedRamp(steps=[2], step_s=0.6, think_s=0.01),
    )

    row = result.rows[0]
    assert {"products.get", "products.list"} <= set(result.meta["plan"]["scenarios"])
    assert "products.create" not in result.meta["plan"]["scenarios"]  # a command, never generated
    assert row["scenarios"]["products.get"]["ok"] > 0
    assert row["outcomes"]["not_found"] == row["outcomes"]["client_error"] == 0
    assert len(result.meta["openapi_sha256"]) == 64


async def test_a_command_named_as_a_read_is_refused_by_its_kind(served: _Served) -> None:
    before = len(served.requests)

    with pytest.raises(CoreException) as error:
        await run(
            _plan(served.base_url, Scenario.op("products.create", body={"name": "x", "price": 1})),
            ClosedRamp(steps=[1], step_s=0.5),
        )

    assert error.value.code == "load_op_mismatch"
    assert served.requests[before:] == ["/openapi.json"]


async def test_a_cpu_bound_client_is_flagged_and_never_named_the_knee(served: _Served) -> None:
    light = await run(
        _plan(served.base_url, Scenario("list", _list, writes=False)),
        ClosedRamp(steps=[1], step_s=1.0, warm_s=0.2, think_s=0.05),
    )
    burning = await run(
        _plan(served.base_url, Scenario("list", _burn_then_list, writes=False)),
        ClosedRamp(steps=[1, 4], step_s=1.0, warm_s=0.2),
    )

    assert light.rows[0]["client"]["loop_cpu_judged"]  # served by another thread
    assert not light.rows[0]["client"]["saturated"]
    assert all(row["client"]["saturated"] for row in burning.rows)
    assert burning.summary["knee"] is None
    assert burning.summary["note"].startswith("client saturated")
