"""Plan validation and the write guard: refusals happen before anything is sent."""

from __future__ import annotations

import random

import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_load import (
    WRITE_HOSTS_ENV,
    Call,
    LoadPlan,
    Principal,
    Scenario,
    validate_plan,
    write_host_allowed,
)
from forze_load.plan import safe_target

pytestmark = pytest.mark.unit

# ----------------------- #


async def _send(call: Call) -> httpx.Response:
    return await call.request("GET", "/x")


def _plan(*, base_url: str = "http://127.0.0.1:8000", writes: bool = False, **kw) -> LoadPlan:
    return LoadPlan(
        base_url=base_url,
        principals=[Principal(name="viewer", mix={"read": 1})],
        scenarios=[Scenario("read", _send, writes=writes)],
        **kw,
    )


def _code(error: pytest.ExceptionInfo[CoreException]) -> str | None:
    return error.value.code


# ....................... #


class TestWriteGuard:
    def test_reads_run_anywhere(self) -> None:
        validate_plan(_plan(base_url="https://staging.example.com"))

    def test_a_writing_scenario_is_refused_while_writes_are_off(self) -> None:
        with pytest.raises(CoreException) as error:
            validate_plan(_plan(writes=True))

        assert _code(error) == "load_writes_refused"

    def test_writes_on_loopback_are_allowed_once_enabled(self) -> None:
        validate_plan(_plan(writes=True, allow_writes=True))
        validate_plan(_plan(base_url="http://localhost:9", writes=True, allow_writes=True))
        validate_plan(_plan(base_url="http://[::1]:9", writes=True, allow_writes=True))

    def test_writes_to_another_host_are_refused_even_when_enabled(self, monkeypatch) -> None:
        monkeypatch.delenv(WRITE_HOSTS_ENV, raising=False)

        with pytest.raises(CoreException) as error:
            validate_plan(
                _plan(base_url="https://staging.example.com", writes=True, allow_writes=True)
            )

        assert _code(error) == "load_write_host_refused"

    def test_the_plan_allowlist_admits_its_host(self) -> None:
        validate_plan(
            _plan(
                base_url="https://Stage.Example.com",
                writes=True,
                allow_writes=True,
                write_hosts=["stage.example.com"],
            )
        )

    def test_scenario_must_state_its_effect(self) -> None:
        with pytest.raises(TypeError):
            Scenario("read", _send)  # type: ignore[call-arg]


class TestWriteHostAllowed:
    @pytest.mark.parametrize(
        "host",
        ["localhost.example.com", "127.0.0.1.nip.io", "evil-localhost", "localhost."],
    )
    def test_lookalikes_are_refused(self, host: str, monkeypatch) -> None:
        monkeypatch.delenv(WRITE_HOSTS_ENV, raising=False)

        assert not write_host_allowed(host)

    @pytest.mark.parametrize("host", ["localhost", "LOCALHOST", "127.0.0.1", "127.8.9.10", "::1"])
    def test_loopback_is_allowed(self, host: str) -> None:
        assert write_host_allowed(host)

    def test_the_environment_list_matches_exactly(self, monkeypatch) -> None:
        monkeypatch.setenv(WRITE_HOSTS_ENV, " stage.internal , load.internal")

        assert write_host_allowed("stage.internal")
        assert write_host_allowed("LOAD.internal")
        assert not write_host_allowed("stage.internal.example.com")
        assert not write_host_allowed("internal")


class TestPlanShape:
    @pytest.mark.parametrize(
        "base_url",
        ["ftp://127.0.0.1", "127.0.0.1:8000", "http://", "http://user:pw@127.0.0.1"],
    )
    def test_bad_urls_are_refused(self, base_url: str) -> None:
        with pytest.raises(CoreException) as error:
            validate_plan(_plan(base_url=base_url))

        assert _code(error) == "load_plan_invalid"

    def test_mix_must_name_known_scenarios(self) -> None:
        plan = LoadPlan(
            base_url="http://127.0.0.1",
            principals=[Principal(name="viewer", mix={"missing": 1})],
            scenarios=[Scenario("read", _send, writes=False)],
        )

        with pytest.raises(CoreException, match="unknown scenarios"):
            validate_plan(plan)

    @pytest.mark.parametrize(
        "principals",
        [
            [],
            [Principal(name="a", mix={"read": 1}), Principal(name="a", mix={"read": 1})],
            [Principal(name="a", mix={"read": 1}, weight=0)],
            [Principal(name="a", mix={})],
            [Principal(name="a", mix={"read": 0})],
        ],
    )
    def test_bad_principals_are_refused(self, principals: list[Principal]) -> None:
        plan = LoadPlan(
            base_url="http://127.0.0.1",
            principals=principals,
            scenarios=[Scenario("read", _send, writes=False)],
        )

        with pytest.raises(CoreException) as error:
            validate_plan(plan)

        assert _code(error) == "load_plan_invalid"

    def test_duplicate_scenarios_and_bad_timeouts_are_refused(self) -> None:
        with pytest.raises(CoreException):
            validate_plan(
                LoadPlan(
                    base_url="http://127.0.0.1",
                    principals=[Principal(name="a", mix={"read": 1})],
                    scenarios=[
                        Scenario("read", _send, writes=False),
                        Scenario("read", _send, writes=False),
                    ],
                )
            )

        with pytest.raises(CoreException):
            validate_plan(_plan(timeout_s=0))

        for bad in (float("nan"), float("inf")):
            with pytest.raises(CoreException):
                validate_plan(_plan(timeout_s=bad))

    def test_the_output_target_keeps_scheme_host_and_port_only(self) -> None:
        assert safe_target("http://h:1/tenants/123/api?token=x#y") == "http://h:1"
        assert safe_target("https://h") == "https://h"


class TestCallRequest:
    """A scenario's URL stays on the plan's target, and so does the credential."""

    @pytest.mark.parametrize(
        "url",
        ["https://evil.example/steal", "//evil.example/steal", "http://127.0.0.1:9/x"],
    )
    async def test_a_url_with_a_scheme_or_host_is_refused_before_sending(self, url: str) -> None:
        sent: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sent.append(request)

            return httpx.Response(200)

        transport = httpx.MockTransport(handler)

        async with httpx.AsyncClient(base_url="http://127.0.0.1:9", transport=transport) as client:
            call = Call(
                client=client,
                headers={"X-API-Key": "k"},
                principal="viewer",
                rng=random.Random(0),
            )

            with pytest.raises(CoreException) as error:
                await call.request("GET", url)

            assert _code(error) == "load_plan_invalid"
            assert sent == []

            await call.request("GET", "/items?page=2")

        assert [str(r.url) for r in sent] == ["http://127.0.0.1:9/items?page=2"]
