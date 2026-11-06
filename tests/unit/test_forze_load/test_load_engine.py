"""The run engine against a fake app: models, stop rules, sign-in, output and refusals."""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx
import pytest

from forze.base.exceptions import CoreException, exc
from forze_load import (
    Call,
    ClosedRamp,
    LoadPlan,
    OpenRamp,
    Principal,
    Scenario,
    api_key,
    inflight_cap,
    password_login,
    read_run,
    run,
)
from forze_load.catalog import resolve
from forze_load.engine import _Probe, _Run

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
        # A call that got no answer has no latency: percentiles cover answered calls.
        assert result.rows[0]["sampled"] == 0
        assert result.rows[0]["ms"]["p50"] is None

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

        row = result.rows[0]
        assert row["outcomes"]["transport"] > 0
        assert row["calls"] == sum(row["outcomes"].values())  # a refusal is a call

    async def test_the_cap_refuses_nothing_from_a_healthy_server(self) -> None:
        async def quick(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.1)

            return httpx.Response(200)

        result = await run(
            _plan(timeout_s=1.0),
            OpenRamp(rates=[20], step_s=1.0, arrivals="poisson"),
            transport=_transport(quick),
        )

        assert result.rows[0]["outcomes"]["transport"] == 0

    def test_the_cap_is_a_poisson_quantile(self) -> None:
        assert inflight_cap(1.0, 1.0) == 9
        assert inflight_cap(100.0, 1.0) == 151
        assert inflight_cap(1000.0, 1.0) == 1154
        assert inflight_cap(0.01, 1.0) >= 1

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

    def test_open_rates_are_floats_whatever_was_given(self) -> None:
        rates = OpenRamp(rates=[50, 100.0], step_s=1).rates

        assert rates == (50.0, 100.0)
        assert all(type(rate) is float for rate in rates)

    @pytest.mark.parametrize(
        "make",
        [
            lambda: ClosedRamp(steps=[], step_s=1),
            lambda: ClosedRamp(steps=[0], step_s=1),
            lambda: ClosedRamp(steps=[1], step_s=1, warm_s=1),
            lambda: OpenRamp(rates=[-1.0], step_s=1),
            lambda: OpenRamp(rates=[float("nan")], step_s=1),
            lambda: OpenRamp(rates=[float("inf")], step_s=1),
            lambda: ClosedRamp(steps=[1], step_s=float("inf")),
            lambda: ClosedRamp(steps=[1], step_s=1, warm_s=float("nan")),
            lambda: ClosedRamp(steps=[1], step_s=1, think_s=float("nan")),
            lambda: ClosedRamp(steps=[1], step_s=1, think_s=-1),
            lambda: ClosedRamp(steps=[1], step_s=1, stop_error_rate=float("nan")),
            lambda: ClosedRamp(steps=[1], step_s=1, stop_error_rate=1.5),
            lambda: ClosedRamp(steps=[1.5], step_s=1),
            lambda: ClosedRamp(steps=[True], step_s=1),
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
        assert written.meta["plan"]["mixes"] == {"viewer": {"get": 1}, "robot": {"get": 1}}
        assert written.meta["target"] == "http://127.0.0.1:9"
        assert written.summary is not None
        assert len(written.rows) == 2


class TestRunState:
    async def test_a_slow_answer_counts_in_the_step_that_sent_it(self) -> None:
        async def slow(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.25)

            return httpx.Response(200)

        result = await run(
            _plan(), ClosedRamp(steps=[1, 1], step_s=0.3), transport=_transport(slow)
        )

        first = result.rows[0]
        # Sent at 0 s and at 0.25 s, the second answered after the step closed.
        assert first["calls"] == 2
        assert first["unfinished"] == 0

    async def test_a_call_waiting_on_its_credential_counts_in_its_step(self) -> None:
        waits = {"n": 0}

        async def slow_after_sign_in(_client: httpx.AsyncClient) -> dict[str, str]:
            waits["n"] += 1

            if waits["n"] > 1:  # sign-in is quick; the run's first call waits past the window
                await asyncio.sleep(0.3)

            return {}

        async def instant(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200)

        result = await run(
            _plan(principals=[Principal(name="v", mix={"get": 1}, auth=slow_after_sign_in)]),
            ClosedRamp(steps=[1], step_s=0.2),
            transport=_transport(instant),
        )

        row = result.rows[0]
        assert row["calls"] == 1
        assert row["auth"]["max_ms"] >= 250

    async def test_rates_use_the_measured_window(self) -> None:
        result = await run(
            _plan(), ClosedRamp(steps=[1], step_s=0.3, warm_s=0.1), transport=_transport(_ok)
        )

        row = result.rows[0]
        assert row["duration_s"] >= 0.2
        assert row["goodput_rps"] == pytest.approx(
            row["outcomes"]["ok"] / row["duration_s"], rel=0.01
        )

    async def test_auth_time_is_kept_out_of_latency_and_counted(self) -> None:
        clients: dict[str, httpx.AsyncClient] = {}

        async def slow_auth(client: httpx.AsyncClient) -> dict[str, str]:
            clients["auth"] = client
            await asyncio.sleep(0.05)

            return {"X-Trace": "load"}

        async def get(call: Call) -> httpx.Response:
            clients["request"] = call.client

            return await call.request("GET", "/items")

        async def instant(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200)

        result = await run(
            _plan(
                principals=[Principal(name="v", mix={"get": 1}, auth=slow_auth)],
                scenarios=[Scenario("get", get, writes=False)],
            ),
            ClosedRamp(steps=[1], step_s=0.4),
            transport=_transport(instant),
        )

        row = result.rows[0]
        assert row["ms"]["p99"] < 40
        assert row["auth"]["max_ms"] >= 40
        assert row["auth"]["total_s"] > 0.1
        assert clients["auth"] is not clients["request"]  # auth never waits on the pool

    async def test_a_url_with_a_host_stops_the_run_and_sends_no_credential(
        self, monkeypatch
    ) -> None:
        key = secrets.token_hex(16)
        monkeypatch.setenv("LOAD_KEY", key)
        seen: list[httpx.Request] = []

        async def record(request: httpx.Request) -> httpx.Response:
            seen.append(request)

            return httpx.Response(200)

        async def follow_next(call: Call) -> httpx.Response:
            return await call.request("GET", "https://evil.example/page/2")

        with pytest.raises(CoreException) as error:
            await run(
                _plan(
                    principals=[Principal(name="v", mix={"next": 1}, auth=api_key("LOAD_KEY"))],
                    scenarios=[Scenario("next", follow_next, writes=False)],
                ),
                ClosedRamp(steps=[1], step_s=0.5),
                transport=_transport(record),
            )

        assert error.value.code == "load_plan_invalid"
        assert seen == []

    def test_a_plan_runs_again_in_a_new_event_loop(self, monkeypatch) -> None:
        password = secrets.token_hex(8)
        monkeypatch.setenv("LOAD_PW", password)
        logins: list[int] = []

        async def app(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/login":
                logins.append(1)

                return httpx.Response(
                    200, json={"access_token": secrets.token_hex(8), "access_expires_in": 900}
                )

            return httpx.Response(200)

        plan = _plan(
            principals=[
                Principal(
                    name="v", mix={"get": 1}, auth=password_login("v", password_env="LOAD_PW")
                )
            ]
        )

        for _ in range(2):  # each asyncio.run is a new loop; the plan holds no loop-bound lock
            asyncio.run(run(plan, ClosedRamp(steps=[1], step_s=0.1), transport=_transport(app)))

        assert len(logins) == 2  # no token carried from the first run

    async def test_an_in_process_app_leaves_loop_cpu_unjudged(self) -> None:
        result = await run(_plan(), ClosedRamp(steps=[1], step_s=0.2), transport=_transport(_ok))

        assert result.rows[0]["client"]["loop_cpu_judged"] is False

    def test_unjudged_loop_cpu_does_not_mark_saturation(self, monkeypatch) -> None:
        clock = {"thread": 0.0, "wall": 0.0}
        monkeypatch.setattr(time, "thread_time", lambda: clock["thread"])
        monkeypatch.setattr(time, "perf_counter", lambda: clock["wall"])
        probe = _Probe()
        probe.begin()
        clock["thread"] = clock["wall"] = 1.0  # a whole core for a second, with no scheduler luck

        assert probe.report(0, 0, cpu_judged=True)["saturated"]
        assert not probe.report(0, 0, cpu_judged=False)["saturated"]

    async def test_arrivals_draw_from_their_own_stream(self) -> None:
        async with httpx.AsyncClient() as client:
            plan = _plan()
            catalog = await resolve(plan, client)
            first = _Run(
                plan=plan, catalog=catalog, client=client, auth_client=client, cpu_judged=True
            )
            second = _Run(
                plan=plan, catalog=catalog, client=client, auth_client=client, cpu_judged=True
            )

        for _ in range(10):
            first.rng.random()  # a scenario drawing from Call.rng

        assert first.arrival_rng.expovariate(5) == second.arrival_rng.expovariate(5)


class _SettlingSession:
    """A credential with work a cancelled caller left running: the run awaits it."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.failure = failure
        self.settled = 0

    async def headers(self, _client: httpx.AsyncClient) -> dict[str, str]:
        return {}

    async def settle(self) -> None:
        self.settled += 1

        if self.failure is not None:
            raise self.failure


class TestCredentialSettle:
    async def test_the_run_waits_for_credential_work_and_raises_its_failure(self) -> None:
        refused = exc.configuration("refresh failed", code="load_refresh_failed")
        session = _SettlingSession(refused)
        plan = _plan(principals=[Principal(name="viewer", mix={"get": 1}, auth=session)])

        with pytest.raises(CoreException) as failed:
            await run(plan, ClosedRamp(steps=[1], step_s=0.1), transport=_transport(_ok))

        assert failed.value is refused
        assert session.settled == 1

    async def test_a_run_that_failed_first_keeps_its_own_failure(self) -> None:
        async def broken(_call: Call) -> httpx.Response:
            raise RuntimeError("scenario bug")

        session = _SettlingSession(exc.configuration("refresh failed"))
        plan = _plan(
            principals=[Principal(name="viewer", mix={"get": 1}, auth=session)],
            scenarios=[Scenario("get", broken, writes=False)],
        )

        with pytest.raises(RuntimeError, match="scenario bug"):
            await run(plan, ClosedRamp(steps=[1], step_s=0.1), transport=_transport(_ok))

        assert session.settled == 1

    async def test_every_session_settles_before_the_first_failure_is_raised(self) -> None:
        first_failure = exc.configuration("refresh failed", code="load_refresh_failed")
        sessions = [
            _SettlingSession(first_failure),
            _SettlingSession(exc.configuration("second")),
            _SettlingSession(),
        ]
        principals = [
            Principal(name=f"p{i}", mix={"get": 1}, auth=session)
            for i, session in enumerate(sessions)
        ]

        with pytest.raises(CoreException) as failed:
            await run(
                _plan(principals=principals),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=_transport(_ok),
            )

        assert failed.value is first_failure
        assert [session.settled for session in sessions] == [1, 1, 1]


def test_the_profile_alias_names_every_profile() -> None:
    from forze_load import Realtime, Soak, engine

    assert engine.Profile.__value__ == ClosedRamp | OpenRamp | Soak | Realtime
