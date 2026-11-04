"""The run engine against a fake app: models, stop rules, sign-in, output and refusals."""

from __future__ import annotations

import asyncio
import json
import secrets
from collections import Counter
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_load import (
    Call,
    ClosedRamp,
    LoadPlan,
    OpenRamp,
    Principal,
    Scenario,
    api_key,
    read_run,
    run,
)

pytestmark = pytest.mark.unit

type Handler = Callable[[httpx.Request], Awaitable[httpx.Response]]

# ----------------------- #


async def _get(call: Call) -> httpx.Response:
    return await call.request("GET", "/items")


def _plan(*, principals: list[Principal] | None = None, **kw) -> LoadPlan:
    return LoadPlan(
        base_url="http://127.0.0.1:9",
        principals=principals or [Principal(name="viewer", mix={"get": 1})],
        scenarios=kw.pop("scenarios", [Scenario("get", _get, writes=False)]),
        **kw,
    )


def _transport(handler: Handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


async def _ok(_request: httpx.Request) -> httpx.Response:
    await asyncio.sleep(0.001)

    return httpx.Response(200)


# ....................... #


class TestClosedModel:
    async def test_each_step_writes_a_row_and_the_summary_closes_the_run(self) -> None:
        result = await run(
            _plan(),
            ClosedRamp(steps=[1, 2], step_s=0.3, warm_s=0.05),
            transport=_transport(_ok),
        )

        assert [row["workers"] for row in result.rows] == [1, 2]
        assert all(row["latency"] == "closed" for row in result.rows)
        assert all(row["outcomes"]["ok"] > 0 for row in result.rows)
        assert result.summary["kind"] == "summary"
        assert result.summary["stopped"] is None
        assert result.meta["model"] == "closed"

    async def test_server_errors_stop_the_ramp(self) -> None:
        async def failing(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.001)

            return httpx.Response(503)

        result = await run(
            _plan(),
            ClosedRamp(steps=[1, 2, 4], step_s=0.2),
            transport=_transport(failing),
        )

        assert len(result.rows) == 1
        assert result.summary["stopped"] == {"workers": 1, "reason": "errors"}

    async def test_deadlines_alone_stop_the_ramp(self) -> None:
        async def timing_out(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.001)

            return httpx.Response(504)

        result = await run(
            _plan(),
            ClosedRamp(steps=[1, 2], step_s=0.2),
            transport=_transport(timing_out),
        )

        assert result.rows[0]["outcomes"]["deadline"] > 0
        assert result.rows[0]["outcomes"]["server_error"] == 0
        assert result.summary["stopped"] == {"workers": 1, "reason": "errors"}

    async def test_a_forbidden_mix_is_misconfigured_and_never_a_knee(self) -> None:
        async def forbidden(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(403)

        result = await run(
            _plan(),
            ClosedRamp(steps=[1, 2], step_s=0.2),
            transport=_transport(forbidden),
        )

        assert result.rows[0]["misconfigured"]
        assert result.summary["stopped"]["reason"] == "misconfigured"
        assert result.summary["knee"] is None

    async def test_transport_failures_are_counted_not_raised(self) -> None:
        async def broken(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        result = await run(
            _plan(),
            ClosedRamp(steps=[1], step_s=0.1, stop_error_rate=1.0),
            transport=_transport(broken),
        )

        assert result.rows[0]["outcomes"]["transport"] > 0

    async def test_a_scenario_bug_stops_the_run_with_its_error(self) -> None:
        async def buggy(_call: Call) -> httpx.Response:
            raise KeyError("missing field")

        with pytest.raises(KeyError):
            await run(
                _plan(scenarios=[Scenario("get", buggy, writes=False)]),
                ClosedRamp(steps=[1], step_s=1.0),
                transport=_transport(_ok),
            )

    async def test_workers_split_by_principal_weight(self) -> None:
        seen: Counter[str] = Counter()

        async def record(request: httpx.Request) -> httpx.Response:
            seen[request.headers.get("X-Tenant-Id", "none")] += 1
            await asyncio.sleep(0.001)

            return httpx.Response(200)

        principals = [
            Principal(name="viewer", mix={"get": 1}, weight=3, tenant="t-viewer"),
            Principal(name="admin", mix={"get": 1}, weight=1),
        ]
        await run(
            _plan(principals=principals),
            ClosedRamp(steps=[4], step_s=0.3),
            transport=_transport(record),
        )

        assert seen["t-viewer"] > seen["none"] > 0


class TestOpenModel:
    async def test_a_stall_shows_in_every_request_scheduled_during_it(self) -> None:
        """Coordinated omission: the open model times from the intended send."""

        stall = asyncio.Event()

        async def stalling(_request: httpx.Request) -> httpx.Response:
            if not stall.is_set():
                stall.set()
                await asyncio.sleep(0.3)

            return httpx.Response(200)

        async def serial(call: Call) -> httpx.Response:
            async with lock:
                return await call.request("GET", "/items")

        lock = asyncio.Lock()  # one connection: a stall holds up everything behind it
        plan = _plan(scenarios=[Scenario("get", serial, writes=False)])
        open_result = await run(
            plan, OpenRamp(rates=[50], step_s=0.6), transport=_transport(stalling)
        )
        stall.clear()
        closed_result = await run(
            plan, ClosedRamp(steps=[1], step_s=0.6), transport=_transport(stalling)
        )

        open_ms = open_result.rows[0]["ms"]
        closed_ms = closed_result.rows[0]["ms"]

        assert open_result.rows[0]["latency"] == "open"
        # Many open-model requests waited behind the stall; the closed worker saw it once.
        assert open_ms["p90"] > 100
        assert closed_ms["p90"] < 100
        assert closed_ms["max"] >= 250

    async def test_sends_past_the_in_flight_cap_count_as_transport(self) -> None:
        async def slow(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(1.0)

            return httpx.Response(200)

        result = await run(
            _plan(timeout_s=0.05),
            OpenRamp(rates=[100], step_s=0.3, stop_error_rate=1.0),
            transport=_transport(slow),
        )

        assert result.rows[0]["outcomes"]["transport"] > 0

    async def test_poisson_arrivals_run(self) -> None:
        result = await run(
            _plan(),
            OpenRamp(rates=[40, 80], step_s=0.3, arrivals="poisson"),
            transport=_transport(_ok),
        )

        assert [row["rate"] for row in result.rows] == [40, 80]
        assert result.rows[0]["outcomes"]["ok"] > 0
        assert result.rows[0]["client"]["behind_schedule"] >= 0


class TestBeforeLoad:
    async def test_writes_are_refused_before_any_request(self) -> None:
        requests: list[httpx.Request] = []

        async def count(request: httpx.Request) -> httpx.Response:
            requests.append(request)

            return httpx.Response(200)

        with pytest.raises(CoreException) as error:
            await run(
                _plan(scenarios=[Scenario("get", _get, writes=True)]),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=_transport(count),
            )

        assert error.value.code == "load_writes_refused"
        assert requests == []

    async def test_a_failed_sign_in_stops_the_run_before_load(self, monkeypatch) -> None:
        monkeypatch.delenv("LOAD_KEY", raising=False)
        requests: list[httpx.Request] = []

        async def count(request: httpx.Request) -> httpx.Response:
            requests.append(request)

            return httpx.Response(200)

        with pytest.raises(CoreException) as error:
            await run(
                _plan(principals=[Principal(name="v", mix={"get": 1}, auth=api_key("LOAD_KEY"))]),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=_transport(count),
            )

        assert error.value.code == "load_credential_missing"
        assert requests == []

    @pytest.mark.parametrize(
        "make",
        [
            lambda: ClosedRamp(steps=[], step_s=1),
            lambda: ClosedRamp(steps=[0], step_s=1),
            lambda: ClosedRamp(steps=[1], step_s=1, warm_s=1),
            lambda: OpenRamp(rates=[-1.0], step_s=1),
        ],
    )
    def test_bad_profiles_are_refused(self, make: Callable[[], object]) -> None:
        with pytest.raises(CoreException):
            make()


class TestOutput:
    async def test_the_file_holds_counts_never_credentials(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        key = secrets.token_hex(16)
        monkeypatch.setenv("LOAD_KEY", key)

        async def echo(request: httpx.Request) -> httpx.Response:
            # A server that echoes the credential back must not get it into the file.
            return httpx.Response(200, json={"key": request.headers.get("X-API-Key")})

        async def callable_auth(_client: httpx.AsyncClient) -> dict[str, str]:
            return {"X-Trace": "load"}

        out = tmp_path / "run.jsonl"
        principals = [
            Principal(name="viewer", mix={"get": 1}, auth=api_key("LOAD_KEY")),
            Principal(name="robot", mix={"get": 1}, auth=callable_auth),
        ]
        await run(
            _plan(principals=principals),
            ClosedRamp(steps=[1, 2], step_s=0.2),
            out=out,
            transport=_transport(echo),
        )

        text = out.read_text()
        assert key not in text
        records = [json.loads(line) for line in text.splitlines()]
        assert [r["kind"] for r in records] == ["meta", "step", "step", "summary"]

        written = read_run(out)
        assert written.meta["plan"]["principals"] == {"viewer": 1, "robot": 1}
        assert written.summary is not None
        assert len(written.rows) == 2
