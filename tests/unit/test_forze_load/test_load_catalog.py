"""Operation references: resolution from OpenAPI, the kind check, samples and the read sweep."""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_load import (
    Call,
    ClosedRamp,
    LoadPlan,
    OpenRamp,
    Principal,
    RunResult,
    Scenario,
    password_login,
    run,
    sample,
)
from forze_load.catalog import _body_misfit
from forze_load.plan import PlanScenario

pytestmark = pytest.mark.unit

# ----------------------- #


def _op(
    key: str,
    kind: str | None = "query",
    *,
    params: Sequence[tuple[str, str, bool]] = (),
    body: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    operation: dict[str, Any] = {"operationId": key}

    if kind is not None:
        operation["x-forze-kind"] = kind

    if params:
        operation["parameters"] = [
            {"name": name, "in": where, "required": required} for name, where, required in params
        ]

    if body is not None:
        operation["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": body}},
        }

    return operation


_ID = [("id", "path", True)]
_DOC: dict[str, Any] = {
    "openapi": "3.1.0",
    "paths": {
        "/products/{id}": {
            "get": _op("products.get", params=_ID),
            "delete": _op("products.kill", "command", params=_ID),
        },
        "/products/list": {"post": _op("products.list", body={"$ref": "#/components/schemas/L"})},
        "/products/agg_list": {
            "post": _op("products.agg_list", body={"properties": {"a": {}}, "required": ["a"]})
        },
        "/products": {"post": _op("products.create", "command", body={"properties": {"n": {}}})},
        "/products/search": {"get": _op("products.search", params=[("q", "query", True)])},
        "/orders/{id}": {"get": _op("orders.get", params=_ID)},
        "/rpc/notes.get": {"get": _op("notes.get", params=[("id", "query", True)])},
        "/rpc/notes.list": {"post": _op("notes.list", body={"properties": {"size": {}}})},
        "/health": {"get": {"operationId": "health_get"}},
    },
    "components": {"schemas": {"L": {"properties": {"page": {}, "size": {}}}}},
}


class _App:
    """Serves :data:`_DOC`, lists each caller's own ids, and answers 404 for ids gone."""

    def __init__(self, ids: Mapping[str, Sequence[str]], doc: Mapping[str, Any] = _DOC) -> None:
        self.doc = doc
        self.ids = ids
        self.gone: set[str] = set()
        self.requests: list[httpx.Request] = []

    def paths(self) -> list[str]:
        return [request.url.path for request in self.requests]

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        visible = self.ids.get(request.headers.get("x-caller", ""), [])

        if path == "/openapi.json":
            return httpx.Response(200, json=self.doc)

        if path.endswith("list"):
            size = json.loads(request.content or b"{}").get("size", 10)
            return httpx.Response(200, json={"hits": [{"id": i} for i in visible][:size]})

        wanted = path.rpartition("/")[2] if path.startswith("/products/") else None
        wanted = request.url.params.get("id", wanted)

        if wanted is not None and (wanted in self.gone or wanted not in visible):
            return httpx.Response(404)

        return httpx.Response(200)


def _as(caller: str) -> Any:
    async def headers(_client: httpx.AsyncClient) -> dict[str, str]:
        return {"X-Caller": caller}

    return headers


def _plan(
    *scenarios: PlanScenario,
    mix: Mapping[str, int] | None = None,
    principals: Sequence[Principal] | None = None,
    **kw: Any,
) -> LoadPlan:
    return LoadPlan(
        base_url=kw.pop("base_url", "http://127.0.0.1:9"),
        principals=principals
        or [Principal(name="ada", mix=mix or {s.name: 1 for s in scenarios}, auth=_as("ada"))],
        scenarios=list(scenarios),
        **kw,
    )


async def _run(app: _App, plan: LoadPlan, steps: Sequence[int] = (1,), **kw: Any) -> RunResult:
    return await run(
        plan,
        ClosedRamp(steps=list(steps), step_s=0.3),
        transport=httpx.MockTransport(app.handler),
        **kw,
    )


async def _refused(app: _App, plan: LoadPlan) -> str | None:
    with pytest.raises(CoreException) as error:
        await _run(app, plan)

    return error.value.code


_GET = Scenario.op("products.get", path={"id": sample("products.list")})


# ....................... #


class TestResolution:
    async def test_an_operation_resolves_to_its_route(self) -> None:
        app = _App({"ada": ["p1", "p2"]})

        result = await _run(app, _plan(_GET))

        assert result.rows[0]["outcomes"]["ok"] > 0
        gets = {p for p in app.paths() if p.startswith("/products/p")}
        assert gets <= {"/products/p1", "/products/p2"}
        assert result.meta["plan"]["scenarios"] == {
            "products.get": {"writes": False, "operation": "products.get"}
        }

    async def test_a_command_is_refused_while_writes_are_off(self) -> None:
        app = _App({})
        plan = _plan(Scenario.op("products.kill", path={"id": "p1"}))

        assert await _refused(app, plan) == "load_op_mismatch"
        assert app.paths() == ["/openapi.json"]  # no sign-in, no load

    @pytest.mark.parametrize("kind", [None, "read", ""])
    async def test_an_absent_or_unknown_kind_counts_as_command(self, kind: str | None) -> None:
        doc = {"paths": {"/x": {"post": _op("x.list", kind, body={})}}}

        assert await _refused(_App({}, doc), _plan(Scenario.op("x.list", body={}))) == (
            "load_op_mismatch"
        )

    async def test_writes_on_a_query_is_a_mismatch_too(self) -> None:
        plan = _plan(Scenario.op("products.list", writes=True, body={}), allow_writes=True)

        assert await _refused(_App({}), plan) == "load_op_mismatch"

    async def test_a_declared_command_runs_once_writes_are_allowed(self) -> None:
        app = _App({"ada": ["p1"]})
        plan = _plan(
            Scenario.op("products.create", writes=True, body={"n": "x"}), allow_writes=True
        )

        result = await _run(app, plan)

        assert result.rows[0]["outcomes"]["ok"] > 0
        assert "/products" in app.paths()

    async def test_a_declared_command_is_still_refused_without_allow_writes(self) -> None:
        plan = _plan(Scenario.op("products.create", writes=True, body={"n": "x"}))

        assert await _refused(_App({}), plan) == "load_writes_refused"

    async def test_a_missing_operation_stops_the_run_before_sign_in(self, monkeypatch) -> None:
        monkeypatch.setenv("LOAD_PW", secrets.token_hex(8))
        app = _App({})
        plan = _plan(
            Scenario.op("products.gone"),
            principals=[
                Principal(
                    name="ada",
                    mix={"products.gone": 1},
                    auth=password_login("ada", password_env="LOAD_PW"),
                )
            ],
        )

        assert await _refused(app, plan) == "load_op_missing"
        assert "/login" not in app.paths()

    async def test_an_operation_listed_twice_is_ambiguous(self) -> None:
        doc = {"paths": {"/a": {"get": _op("dup")}, "/b": {"get": _op("dup")}}}

        assert await _refused(_App({}, doc), _plan(Scenario.op("dup"))) == "load_op_ambiguous"

    @pytest.mark.parametrize(
        "scenario",
        [
            Scenario.op("products.get"),  # {id} left unfilled
            Scenario.op("products.get", path={"id": "p1", "slug": "x"}),
            Scenario.op("products.get", path={"id": "p1"}, params={"typo": 1}),
            Scenario.op("products.search"),  # required ?q= missing
            Scenario.op("products.list"),  # required body missing
            Scenario.op("products.get", path={"id": "p1"}, body={}),  # GET takes no body
        ],
    )
    async def test_inputs_that_do_not_fit_the_route_are_refused(self, scenario) -> None:
        assert await _refused(_App({}), _plan(scenario)) == "load_plan_invalid"

    async def test_an_unavailable_document_is_refused(self) -> None:
        async def no_docs(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        with pytest.raises(CoreException) as error:
            await run(
                _plan(_GET),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=httpx.MockTransport(no_docs),
            )

        assert error.value.code == "load_openapi_unavailable"

    async def test_a_document_from_a_file_or_a_mapping_resolves_alike(self, tmp_path) -> None:
        path = tmp_path / "openapi.json"
        path.write_text(json.dumps(_DOC))
        served, from_file, given = (
            _App({"ada": ["p1"]}),
            _App({"ada": ["p1"]}),
            _App({"ada": ["p1"]}),
        )

        first = await _run(served, _plan(_GET))
        second = await _run(from_file, _plan(_GET, openapi=path))
        third = await _run(given, _plan(_GET, openapi=_DOC))

        assert "/openapi.json" not in from_file.paths() + given.paths()
        assert first.meta["openapi_sha256"] == second.meta["openapi_sha256"]
        assert first.meta["openapi_sha256"] == third.meta["openapi_sha256"]
        assert len(first.meta["openapi_sha256"]) == 64

    async def test_a_plan_without_references_reads_no_document(self) -> None:
        app = _App({})

        async def get(call: Call) -> httpx.Response:
            return await call.request("GET", "/health")

        result = await _run(app, _plan(Scenario("health", get, writes=False)))

        assert "/openapi.json" not in app.paths()
        assert result.meta["openapi_sha256"] is None

    def test_the_openapi_path_must_stay_on_the_target(self) -> None:
        from forze_load import validate_plan

        with pytest.raises(CoreException) as error:
            validate_plan(_plan(_GET, openapi="https://evil.example/openapi.json"))

        assert error.value.code == "load_plan_invalid"


class TestSamples:
    async def test_each_principal_calls_only_the_ids_it_can_see(self) -> None:
        app = _App({"ada": ["a1", "a2"], "bob": ["b1"]})
        plan = _plan(
            _GET,
            principals=[
                Principal(name="ada", mix={"products.get": 1}, auth=_as("ada")),
                Principal(name="bob", mix={"products.get": 1}, auth=_as("bob")),
            ],
        )

        result = await _run(app, plan, steps=[2])

        assert result.rows[0]["outcomes"]["not_found"] == 0
        lists = [r.headers["x-caller"] for r in app.requests if r.url.path == "/products/list"]
        assert sorted(lists) == ["ada", "bob"]  # once each, before load

    async def test_an_empty_sample_stops_the_run_before_load(self) -> None:
        app = _App({"ada": []})

        with pytest.raises(CoreException) as error:
            await _run(app, _plan(_GET))

        assert error.value.code == "load_sample_empty"
        assert "'ada'" in error.value.summary
        assert "products.list" in error.value.summary
        assert not any(p.startswith("/products/") and p != "/products/list" for p in app.paths())

    async def test_a_failing_sample_stops_the_run(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/openapi.json":
                return httpx.Response(200, json=_DOC)

            return httpx.Response(403)

        with pytest.raises(CoreException) as error:
            await run(
                _plan(_GET),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=httpx.MockTransport(handler),
            )

        assert error.value.code == "load_sample_failed"

    async def test_a_sample_must_read_a_query(self) -> None:
        plan = _plan(Scenario.op("products.get", path={"id": sample("products.create")}))

        assert await _refused(_App({}), plan) == "load_op_mismatch"

    @pytest.mark.parametrize("size", [0, 10_001, True])
    def test_a_sample_size_outside_the_route_cap_is_refused(self, size: int) -> None:
        with pytest.raises(CoreException) as error:
            sample("products.list", size=size)

        assert error.value.code == "load_plan_invalid"

    async def test_the_size_is_sent_to_the_list_route(self) -> None:
        app = _App({"ada": [f"p{i}" for i in range(20)]})

        await _run(
            app, _plan(Scenario.op("products.get", path={"id": sample("products.list", size=3)}))
        )

        listing = next(r for r in app.requests if r.url.path == "/products/list")
        assert json.loads(listing.content) == {"size": 3}
        assert {p for p in app.paths() if p.startswith("/products/p")} <= {
            "/products/p0",
            "/products/p1",
            "/products/p2",
        }

    async def test_a_sample_emptied_by_404s_retires_its_scenario_only(self) -> None:
        app = _App({"ada": ["p1", "p2"]})
        app.gone = {"p1", "p2"}  # deleted after sampling
        listing = Scenario.op("products.list", name="list", body={})

        result = await _run(app, _plan(_GET, listing), steps=[1, 1])

        first, second = result.rows
        assert first["outcomes"]["not_found"] <= 2
        assert first["sample_exhausted"] == [{"principal": "ada", "scenario": "products.get"}]
        assert "sample_exhausted" not in second
        assert second["outcomes"]["ok"] > 0  # the rest of the mix carries on
        assert set(second["scenarios"]) == {"list"}
        assert result.summary["stopped"] is None

    async def test_a_principal_with_nothing_left_stops_the_run(self, tmp_path: Path) -> None:
        app = _App({"ada": ["p1"]})
        app.gone = {"p1"}
        out = tmp_path / "run.jsonl"

        result = await _run(app, _plan(_GET), steps=[1, 2], out=out)

        assert len(result.rows) == 1
        assert result.summary["knee"] is None  # the cut-short step is never a knee
        assert result.summary["note"] == "no step measured the server"
        assert result.summary["stopped"] == {
            "workers": 1,
            "reason": "sample_exhausted",
            "principal": "ada",
        }
        assert read_back(out)[-1]["stopped"]["reason"] == "sample_exhausted"

    async def test_sampled_ids_never_reach_the_output(self, tmp_path: Path) -> None:
        ids = [f"id-{secrets.token_hex(6)}" for _ in range(3)]
        app = _App({"ada": ids})
        out = tmp_path / "run.jsonl"

        result = await _run(app, _plan(_GET), out=out)

        text = out.read_text()
        assert result.rows[0]["scenarios"]["products.get"]["ok"] > 0  # the ids were called
        assert not any(i in text for i in ids)

    async def test_a_call_whose_sample_emptied_while_it_waited_sends_nothing(self) -> None:
        # Worker A sends and its answer is slow; worker B picks the same scenario and waits on
        # its credential. A's 404 empties the sample; B then finds nothing to call.
        app = _App({"ada": ["p1"]})
        app.gone = {"p1"}
        handle = app.handler
        credentials = 0

        async def slow_answers(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/products/p1":
                await asyncio.sleep(0.02)

            return await handle(request)

        async def every_other_waits(_client: httpx.AsyncClient) -> dict[str, str]:
            nonlocal credentials
            credentials += 1

            if credentials % 2 == 0:  # sign-in 1, sample 2, worker A 3, worker B 4
                await asyncio.sleep(0.1)

            return {"X-Caller": "ada"}

        plan = _plan(
            _GET,
            principals=[Principal(name="ada", mix={"products.get": 1}, auth=every_other_waits)],
        )

        result = await run(
            plan,
            ClosedRamp(steps=[2], step_s=1.0),
            transport=httpx.MockTransport(slow_answers),
        )

        assert app.paths().count("/products/p1") == 1
        assert result.rows[0]["calls"] == result.rows[0]["outcomes"]["not_found"] == 1
        assert result.summary["stopped"]["reason"] == "sample_exhausted"

    async def test_a_stop_mid_window_marks_the_step_cut_short(self) -> None:
        app = _App({"ada": ["p1"]})
        app.gone = {"p1"}

        result = await _run(app, _plan(_GET))

        assert result.rows[0]["cut_short"] is True
        assert result.summary["note"] == "no step measured the server"

    async def test_a_stop_after_the_window_keeps_the_whole_step(self) -> None:
        # The 404 lands while the step drains, after its window closed: the step is whole.
        app = _App({"ada": ["p1"]})
        app.gone = {"p1"}
        handle = app.handler

        async def late(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/products/p1":
                await asyncio.sleep(0.4)

            return await handle(request)

        result = await run(
            _plan(_GET), ClosedRamp(steps=[1, 2], step_s=0.2), transport=httpx.MockTransport(late)
        )

        assert len(result.rows) == 1
        assert "cut_short" not in result.rows[0]  # whole, so `compare` gates it
        # Its retirement still stops the knee search: the next step would run another mix.
        assert result.summary["note"] == "mix changed at {'workers': 1}"
        assert result.summary["stopped"]["reason"] == "sample_exhausted"


class TestDocumentShapes:
    @pytest.mark.parametrize(
        "operation",
        [
            {"operationId": "x", "x-forze-kind": "query", "parameters": None},
            {"operationId": "x", "x-forze-kind": "query", "parameters": ["id"]},
            {"operationId": "x", "x-forze-kind": "query", "requestBody": {"content": []}},
            {"operationId": "x", "x-forze-kind": "query", "requestBody": "body"},
        ],
    )
    async def test_a_malformed_operation_is_refused_not_a_crash(self, operation) -> None:
        doc = {"paths": {"/x": {"get": operation}}}

        assert await _refused(_App({}, doc), _plan(Scenario.op("x"))) == (
            "load_openapi_unavailable"
        )

    async def test_path_level_parameters_and_a_path_item_ref_resolve(self) -> None:
        doc = {
            "paths": {
                "/x/{id}": {"$ref": "#/components/pathItems/X"},
            },
            "components": {
                "pathItems": {
                    "X": {
                        "parameters": [{"name": "id", "in": "path", "required": True}],
                        "get": _op("x.get"),
                    }
                }
            },
        }
        app = _App({"ada": ["p1"]}, doc)

        result = await _run(app, _plan(Scenario.op("x.get", path={"id": "p1"})))

        assert result.rows[0]["calls"] > 0
        assert "/x/p1" in app.paths()

    async def test_a_server_path_missing_from_the_base_url_is_refused(self) -> None:
        doc = {**_DOC, "servers": [{"url": "/api/v1"}]}

        assert await _refused(_App({}, doc), _plan(_GET)) == "load_plan_invalid"

    async def test_a_server_path_in_the_base_url_resolves(self) -> None:
        doc = {**_DOC, "servers": [{"url": "/api/v1"}, {"url": "https://other.example"}]}

        seen: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)

            if request.url.path == "/api/v1/openapi.json":
                return httpx.Response(200, json=doc)

            return httpx.Response(200, json={"hits": [{"id": "p1"}]})

        result = await run(
            _plan(_GET, base_url="http://127.0.0.1:9/api/v1"),
            ClosedRamp(steps=[1], step_s=0.2),
            transport=httpx.MockTransport(handler),
        )

        assert result.rows[0]["outcomes"]["ok"] > 0
        assert "/api/v1/products/p1" in seen

    @pytest.mark.parametrize("level", ["operation", "path item"])
    async def test_a_reference_to_an_operation_on_its_own_server_is_refused(self, level) -> None:
        doc = json.loads(json.dumps(_DOC))
        item = doc["paths"]["/products/{id}"]
        (item["get"] if level == "operation" else item)["servers"] = [{"url": "/v2"}]

        assert await _refused(_App({}, doc), _plan(_GET)) == "load_plan_invalid"

    async def test_an_operation_naming_the_documents_server_resolves(self) -> None:
        doc = json.loads(json.dumps(_DOC))
        doc["paths"]["/products/{id}"]["get"]["servers"] = [{"url": "/"}]

        result = await _run(_App({"ada": ["p1"]}, doc), _plan(_GET))

        assert result.rows[0]["outcomes"]["ok"] > 0

    async def test_a_sweep_skips_an_operation_on_its_own_server(self) -> None:
        doc = json.loads(json.dumps(_DOC))
        doc["paths"]["/rpc/notes.get"]["servers"] = [{"url": "https://elsewhere.example/v2"}]

        result = await _run(_App({"ada": ["p1"]}, doc), _plan(Scenario.reads("notes.")))

        assert set(result.meta["plan"]["scenarios"]) == {"notes.list"}
        assert result.meta["plan"]["skipped"] == {
            "notes.get": "declares its own server 'https://elsewhere.example/v2'"
        }

    async def test_an_operation_listed_under_two_kinds_is_skipped_not_dropped(self) -> None:
        doc = {
            "paths": {
                "/a": {"get": _op("a.read")},
                "/b": {"get": _op("a.dup")},
                "/c": {"post": _op("a.dup", "command")},
            }
        }

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {"a.dup": "appears 2 times in the document"}

    async def test_a_composed_body_is_skipped_not_guessed(self) -> None:
        doc = {
            "paths": {
                "/a": {"get": _op("a.read")},
                "/b": {"post": _op("a.union", body={"oneOf": [{"required": ["k"]}]})},
            }
        }

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {
            "a.union": "its body schema uses keywords the kit does not evaluate: oneOf"
        }


class TestReadSweep:
    async def test_query_routes_are_generated_and_the_rest_listed_as_skipped(self) -> None:
        app = _App({"ada": ["p1"]})

        result = await _run(app, _plan(Scenario.reads("products.")))

        scenarios = result.meta["plan"]["scenarios"]
        assert set(scenarios) == {"products.get", "products.list"}
        assert all(s["group"] == "reads" and not s["writes"] for s in scenarios.values())
        assert result.meta["plan"]["skipped"] == {
            "products.agg_list": "its body requires ['a']",
            "products.search": "needs ['q']",
        }
        listing = next(r for r in app.requests if r.url.path == "/products/list")
        assert json.loads(listing.content) == {"size": 500}  # the sample, before load
        assert "/products/p1" in app.paths()
        assert not any(r.method == "DELETE" for r in app.requests)  # commands never

    async def test_a_mix_weight_on_the_group_is_split_between_its_routes(self) -> None:
        app = _App({"ada": ["p1"]})

        result = await _run(app, _plan(Scenario.reads("products.")))

        calls = Counter({n: s["calls"] for n, s in result.rows[0]["scenarios"].items()})
        assert set(calls) == {"products.get", "products.list"}
        assert min(calls.values()) > 0

    async def test_an_id_route_with_no_list_beside_it_is_skipped(self) -> None:
        result = await _run(_App({"ada": ["p1"]}), _plan(Scenario.reads("")))

        assert (
            result.meta["plan"]["skipped"]["orders.get"]
            == "no 'orders.list' query to sample ids from"
        )

    async def test_an_rpc_id_goes_in_the_query(self) -> None:
        app = _App({"ada": ["n1"]})

        await _run(app, _plan(Scenario.reads("notes.")))

        gets = [r for r in app.requests if r.url.path == "/rpc/notes.get"]
        assert gets and all(r.url.params["id"] == "n1" for r in gets)

    async def test_a_sweep_that_generates_nothing_stops_the_run(self) -> None:
        assert await _refused(_App({}), _plan(Scenario.reads("orders."))) == "load_op_missing"

    async def test_an_operation_named_by_another_scenario_is_left_to_it(self) -> None:
        app = _App({"ada": ["p1"]})
        plan = _plan(Scenario.op("products.list", body={"size": 2}), Scenario.reads("products."))

        result = await _run(app, plan)

        assert result.meta["plan"]["skipped"]["products.list"] == "named by another scenario"
        assert "group" not in result.meta["plan"]["scenarios"]["products.list"]


def read_back(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


# ....................... #


class TestRouteInputs:
    """What a route needs, read as the OpenAPI specification defines it."""

    async def test_a_sweep_skips_a_route_needing_a_header_or_a_cookie(self) -> None:
        doc = {
            "paths": {
                "/a": {"get": _op("a.read")},
                "/h": {"get": _op("a.region", params=[("X-Region", "header", True)])},
                "/c": {"get": _op("a.session", params=[("session", "cookie", True)])},
                "/o": {"get": _op("a.traced", params=[("X-Trace", "header", False)])},
            }
        }

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert set(result.meta["plan"]["scenarios"]) == {"a.read", "a.traced"}
        assert result.meta["plan"]["skipped"] == {
            "a.region": "needs ['header X-Region']",
            "a.session": "needs ['cookie session']",
        }

    async def test_a_sample_of_a_route_needing_a_header_is_refused(self) -> None:
        doc = {
            "paths": {
                "/x/{id}": {"get": _op("x.get", params=_ID)},
                "/x/list": {"get": _op("x.list", params=[("X-Region", "header", True)])},
            }
        }
        plan = _plan(Scenario.op("x.get", path={"id": sample("x.list")}))

        assert await _refused(_App({}, doc), plan) == "load_plan_invalid"

    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "array"},
            {"type": "string"},
            {"type": "object", "minProperties": 1},
            {"type": "object", "enum": [{"mode": "all"}]},
            {"const": {"mode": "all"}},
            {"type": "object", "enum": "not a list"},
        ],
    )
    async def test_a_body_that_cannot_be_an_empty_object_is_skipped(self, schema) -> None:
        doc = {"paths": {"/a": {"get": _op("a.read")}, "/b": {"post": _op("a.batch", body=schema)}}}

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {"a.batch": "its body cannot be an empty object"}

    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "object", "enum": [{"mode": "all"}, {}]},
            {"const": {}},
            {"type": ["object", "null"]},
        ],
    )
    async def test_a_body_that_admits_an_empty_object_is_generated(self, schema) -> None:
        doc = {"paths": {"/b": {"post": _op("a.batch", body=schema)}}}

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert set(result.meta["plan"]["scenarios"]) == {"a.batch"}

    @pytest.mark.parametrize(
        ("schema", "named"),
        [
            ({"type": "object", "not": {"maxProperties": 0}}, "not"),
            ({"if": {"required": ["k"]}, "then": {"minProperties": 1}}, "if, then"),
            ({"properties": {}, "patternProperties": {"^x": {}}}, "patternProperties"),
            ({"dependentRequired": {"a": ["b"]}}, "dependentRequired"),
        ],
    )
    async def test_a_keyword_the_kit_does_not_evaluate_skips_the_body(self, schema, named) -> None:
        doc = {"paths": {"/a": {"get": _op("a.read")}, "/b": {"post": _op("a.batch", body=schema)}}}

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {
            "a.batch": f"its body schema uses keywords the kit does not evaluate: {named}"
        }

    async def test_an_allowlisted_schema_still_generates(self) -> None:
        schema = {
            "type": "object",
            "title": "Listing",
            "description": "A page of rows.",
            "properties": {"page": {"type": "integer", "default": 1}},
            "additionalProperties": False,
            "maxProperties": 3,
            "nullable": False,
            "examples": [{}],
            "deprecated": False,
            "$comment": "kept",
        }
        doc = {"paths": {"/b": {"post": _op("a.batch", body=schema)}}}

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert set(result.meta["plan"]["scenarios"]) == {"a.batch"}

    async def test_an_operation_parameter_overrides_the_path_s_own(self) -> None:
        doc = {
            "paths": {
                "/x": {
                    "parameters": [{"name": "q", "in": "query", "required": True}],
                    "get": _op("x.read", params=[("q", "query", False)]),
                }
            }
        }

        explicit = await _run(_App({}, doc), _plan(Scenario.op("x.read")))
        swept = await _run(_App({}, doc), _plan(Scenario.reads("x.")))

        assert explicit.rows[0]["calls"] > 0
        assert set(swept.meta["plan"]["scenarios"]) == {"x.read"}

    @pytest.mark.parametrize(
        "ref",
        [
            "#/components/schemas/Missing",
            "other.json#/components/schemas/B",
            "#/components/schemas/Loop",
        ],
    )
    async def test_a_body_schema_that_cannot_be_resolved_is_skipped(self, ref: str) -> None:
        doc = {
            "paths": {
                "/a": {"get": _op("a.read")},
                "/b": {"post": _op("a.body", body={"$ref": ref})},
            },
            "components": {
                "schemas": {
                    "Loop": {"$ref": "#/components/schemas/Loop2"},
                    "Loop2": {"$ref": "#/components/schemas/Loop"},
                }
            },
        }

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {"a.body": "its body schema cannot be resolved"}

    @pytest.mark.parametrize(
        "paths",
        [
            {"/x": {"post": {**_op("x.read"), "requestBody": {"$ref": "#/components/x/Gone"}}}},
            {"/x": {"$ref": "#/components/pathItems/Gone"}},
        ],
    )
    async def test_an_unresolvable_body_or_path_item_is_refused(self, paths) -> None:
        plan = _plan(Scenario.op("x.read", body={}))

        assert await _refused(_App({}, {"paths": paths}), plan) == "load_openapi_unavailable"

    async def test_an_escaped_json_pointer_resolves(self) -> None:
        doc = {
            "paths": {
                "/a": {"get": _op("a.read")},
                "/b": {"post": _op("a.body", body={"$ref": "#/components/schemas/a~1b~0c"})},
            },
            "components": {"schemas": {"a/b~c": {"properties": {"k": {}}, "required": ["k"]}}},
        }

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {"a.body": "its body requires ['k']"}

    async def test_a_body_for_a_route_that_takes_no_json_is_refused(self) -> None:
        form = {"multipart/form-data": {"schema": {}}}
        upload = {**_op("x.upload", "command"), "requestBody": {"required": True, "content": form}}
        plan = _plan(Scenario.op("x.upload", writes=True, body={"f": "x"}), allow_writes=True)

        assert await _refused(_App({}, {"paths": {"/u": {"post": upload}}}), plan) == (
            "load_plan_invalid"
        )

    async def test_a_json_flavoured_media_type_takes_a_json_body(self) -> None:
        merge = {"application/merge-patch+json": {"schema": {"properties": {"n": {}}}}}
        patch = {**_op("x.patch", "command"), "requestBody": {"required": True, "content": merge}}
        plan = _plan(Scenario.op("x.patch", writes=True, body={"n": 1}), allow_writes=True)
        app = _App({}, {"paths": {"/p": {"patch": patch}}})

        result = await _run(app, plan)

        assert result.rows[0]["calls"] > 0
        sent = [r for r in app.requests if r.url.path == "/p"]
        assert sent and {r.headers["content-type"] for r in sent} == {
            "application/merge-patch+json"
        }
        assert json.loads(sent[0].content) == {"n": 1}

    async def test_a_route_that_also_takes_application_json_gets_it(self) -> None:
        content = {
            "application/merge-patch+json": {"schema": {"properties": {"n": {}}}},
            "application/json": {"schema": {"properties": {"n": {}}}},
        }
        patch = {**_op("x.patch", "command"), "requestBody": {"required": True, "content": content}}
        plan = _plan(Scenario.op("x.patch", writes=True, body={"n": 1}), allow_writes=True)
        app = _App({}, {"paths": {"/p": {"patch": patch}}})

        await _run(app, plan)

        sent = [r for r in app.requests if r.url.path == "/p"]
        assert sent and {r.headers["content-type"] for r in sent} == {"application/json"}

    async def test_a_sample_sends_the_list_route_s_json_media_type(self) -> None:
        body = {"application/vnd.shop+json": {"schema": {"properties": {"size": {}}}}}
        listing = {**_op("x.list"), "requestBody": {"required": True, "content": body}}
        doc = {
            "paths": {"/x/{id}": {"get": _op("x.get", params=_ID)}, "/x/list": {"post": listing}}
        }
        app = _App({"ada": ["p1"]}, doc)

        await _run(app, _plan(Scenario.op("x.get", path={"id": sample("x.list", size=5)})))

        request = next(r for r in app.requests if r.url.path == "/x/list")
        assert request.headers["content-type"] == "application/vnd.shop+json"
        assert json.loads(request.content) == {"size": 5}

    async def test_an_explicit_body_missing_a_required_field_is_refused(self) -> None:
        assert await _refused(_App({}), _plan(Scenario.op("products.agg_list", body={}))) == (
            "load_plan_invalid"
        )

        result = await _run(_App({}), _plan(Scenario.op("products.agg_list", body={"a": 1})))
        assert result.rows[0]["calls"] > 0

    async def test_an_operation_called_under_another_name_is_left_to_it(self) -> None:
        plan = _plan(
            Scenario.op("products.list", name="list", body={}), Scenario.reads("products.")
        )

        result = await _run(_App({"ada": ["p1"]}), plan)

        assert result.meta["plan"]["skipped"]["products.list"] == "named by another scenario"
        assert "products.list" not in result.meta["plan"]["scenarios"]


class TestSampleBounds:
    @pytest.mark.parametrize(
        "schema",
        [
            {"properties": {"size": {}}, "const": {}},
            {"properties": {"size": {}}, "enum": [{}]},
            {"properties": {"size": {}}, "maxProperties": 0},
            {"properties": {"size": {"type": "integer", "maximum": 100}}},
            {"properties": {"size": {"type": "string"}}},
            {"properties": {"size": {"anyOf": [{"type": "integer"}]}}},
            {"properties": {"size": {"$ref": "#/components/schemas/Size"}}},
        ],
    )
    async def test_a_sample_body_its_schema_refuses_stops_the_run(self, schema) -> None:
        doc = {
            "paths": {
                "/x/{id}": {"get": _op("x.get", params=_ID)},
                "/x/list": {"post": _op("x.list", body=schema)},
            }
        }
        plan = _plan(Scenario.op("x.get", path={"id": sample("x.list", size=500)}))

        with pytest.raises(CoreException) as error:
            await _run(_App({"ada": ["p1"]}, doc), plan)

        assert error.value.code == "load_plan_invalid"
        assert error.value.summary.startswith("sample('x.list') cannot call its operation: its ")

    @pytest.mark.parametrize(
        "schema",
        [
            {"properties": {"size": {}}, "enum": [{"size": 5}]},
            {"properties": {"size": {"type": "integer", "minimum": 1, "maximum": 5}}},
            {"properties": {"size": {"exclusiveMinimum": 0}}, "additionalProperties": False},
            {"properties": {"size": {}}, "required": ["size"], "maxProperties": 1},
        ],
    )
    async def test_a_sample_body_its_schema_admits_is_sent(self, schema) -> None:
        doc = {
            "paths": {
                "/x/{id}": {"get": _op("x.get", params=_ID)},
                "/x/list": {"post": _op("x.list", body=schema)},
            }
        }
        app = _App({"ada": ["p1"]}, doc)

        await _run(app, _plan(Scenario.op("x.get", path={"id": sample("x.list", size=5)})))

        listing = next(r for r in app.requests if r.url.path == "/x/list")
        assert json.loads(listing.content) == {"size": 5}

    async def test_a_sample_keeps_no_more_than_its_size(self) -> None:
        doc = {**_DOC, "components": {"schemas": {"L": {"properties": {"page": {}}}}}}
        app = _App({"ada": [f"p{i}" for i in range(10)]}, doc)

        await _run(
            app, _plan(Scenario.op("products.get", path={"id": sample("products.list", size=3)}))
        )

        listing = next(r for r in app.requests if r.url.path == "/products/list")
        assert json.loads(listing.content) == {}  # the route takes no size; it answers ten
        assert {p for p in app.paths() if p.startswith("/products/p")} <= {
            "/products/p0",
            "/products/p1",
            "/products/p2",
        }

    async def test_the_size_goes_in_the_query_when_the_body_has_none(self) -> None:
        listing_op = _op(
            "x.list", params=[("size", "query", False)], body={"properties": {"f": {}}}
        )
        doc = {
            "paths": {"/x/{id}": {"get": _op("x.get", params=_ID)}, "/x/list": {"post": listing_op}}
        }
        app = _App({"ada": ["p1"]}, doc)

        await _run(app, _plan(Scenario.op("x.get", path={"id": sample("x.list", size=7)})))

        listing = next(r for r in app.requests if r.url.path == "/x/list")
        assert listing.url.params["size"] == "7"
        assert json.loads(listing.content) == {}

    async def test_a_404_on_two_sampled_values_drops_neither(self) -> None:
        item = _op("items.get", params=[("shop", "path", True), ("id", "path", True)])
        doc = {
            "paths": {
                "/shops/list": {"post": _op("shops.list", body={})},
                "/products/list": {"post": _op("products.list", body={})},
                "/shops/{shop}/items/{id}": {"get": item},
            }
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/openapi.json":
                return httpx.Response(200, json=doc)

            if request.url.path.endswith("list"):
                return httpx.Response(200, json={"hits": [{"id": "x1"}]})

            return httpx.Response(404)  # the shop or the item: the answer does not say

        both = {"shop": sample("shops.list"), "id": sample("products.list")}
        result = await run(
            _plan(Scenario.op("items.get", path=both)),
            ClosedRamp(steps=[1, 1], step_s=0.2),
            transport=httpx.MockTransport(handler),
        )

        assert len(result.rows) == 2
        assert result.rows[0]["outcomes"]["not_found"] > 0
        assert result.summary["stopped"] is None
        assert not any("sample_exhausted" in row for row in result.rows)

    async def test_each_sample_reads_the_credential_afresh(self) -> None:
        app = _App({"ada": ["p1"]})
        handle = app.handler
        credentials = 0
        at_listing: list[int] = []

        async def counted(_client: httpx.AsyncClient) -> dict[str, str]:
            nonlocal credentials
            credentials += 1

            return {"X-Caller": "ada"}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("list"):
                at_listing.append(credentials)

            return await handle(request)

        plan = _plan(
            Scenario.op("products.get", path={"id": sample("products.list")}),
            Scenario.op("notes.get", params={"id": sample("notes.list")}),
            principals=[
                Principal(name="ada", mix={"products.get": 1, "notes.get": 1}, auth=counted)
            ],
        )

        await run(plan, ClosedRamp(steps=[1], step_s=0.1), transport=httpx.MockTransport(handler))

        assert at_listing == [2, 3]  # after sign-in, once more per sample

    async def test_open_arrivals_stop_once_the_run_halts(self) -> None:
        app = _App({"ada": ["p1"]})
        app.gone = {"p1"}
        handle = app.handler
        halted: list[float] = []
        slow_sends: list[float] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/slow":
                slow_sends.append(time.perf_counter())
                await asyncio.sleep(0.3)  # keeps the halted step draining

                return httpx.Response(200)

            if request.url.path == "/products/p1":
                await asyncio.sleep(0.05)  # bob's slow calls are in flight by then

            response = await handle(request)

            if request.url.path == "/products/p1":
                halted.append(time.perf_counter())

            return response

        async def slow(call: Call) -> httpx.Response:
            return await call.request("GET", "/slow")

        plan = _plan(
            _GET,
            Scenario("slow", slow, writes=False),
            principals=[
                Principal(name="ada", mix={"products.get": 1}, auth=_as("ada")),
                Principal(name="bob", mix={"slow": 1}, auth=_as("bob")),
            ],
        )

        result = await run(
            plan, OpenRamp(rates=[100], step_s=1.0), transport=httpx.MockTransport(handler)
        )

        assert result.summary["stopped"]["reason"] == "sample_exhausted"
        assert not [t for t in slow_sends if t > halted[0] + 0.05]


class TestRefusals:
    """Every way the document or a sample can fail stops the run before load, as a refusal."""

    @pytest.mark.parametrize(
        "answer",
        [
            httpx.ConnectError("refused"),
            httpx.Response(200, text="<html>docs</html>"),
            httpx.Response(200, json={"openapi": "3.1.0"}),
        ],
        ids=["transport", "not-json", "no-paths"],
    )
    async def test_a_document_that_cannot_be_read_is_refused(self, answer) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if isinstance(answer, Exception):
                raise answer

            return answer

        with pytest.raises(CoreException) as error:
            await run(
                _plan(_GET),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=httpx.MockTransport(handler),
            )

        assert error.value.code == "load_openapi_unavailable"

    @pytest.mark.parametrize("content", [None, "{not json"], ids=["missing", "not-json"])
    async def test_a_document_file_that_cannot_be_read_is_refused(self, tmp_path, content) -> None:
        path = tmp_path / "openapi.json"

        if content is not None:
            path.write_text(content)

        assert await _refused(_App({}), _plan(_GET, openapi=path)) == "load_openapi_unavailable"

    @pytest.mark.parametrize(
        "paths",
        [
            {"/x": {"parameters": {"q": 1}, "get": _op("x.read")}},
            {"/x": {"get": {**_op("x.read"), "parameters": [{"$ref": "#/components/p/Gone"}]}}},
        ],
        ids=["path-parameters-not-a-list", "parameter-ref-gone"],
    )
    async def test_malformed_parameters_are_refused(self, paths) -> None:
        assert await _refused(_App({}, {"paths": paths}), _plan(Scenario.op("x.read"))) == (
            "load_openapi_unavailable"
        )

    async def test_an_operation_without_an_operation_id_is_not_indexed(self) -> None:
        doc = {"paths": {"/a": {"get": _op("a.read"), "post": {"x-forze-kind": "query"}}}}

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert set(result.meta["plan"]["scenarios"]) == {"a.read"}

    async def test_a_sweep_skips_a_required_body_that_is_not_json(self) -> None:
        form = {"application/x-www-form-urlencoded": {"schema": {}}}
        doc = {
            "paths": {
                "/a": {"get": _op("a.read")},
                "/f": {
                    "post": {**_op("a.form"), "requestBody": {"required": True, "content": form}}
                },
            }
        }

        result = await _run(_App({}, doc), _plan(Scenario.reads("a.")))

        assert result.meta["plan"]["skipped"] == {"a.form": "takes a body that is not JSON"}

    @pytest.mark.parametrize(
        "answer",
        [
            httpx.ConnectError("refused"),
            httpx.Response(200, text="ids: p1"),
            httpx.Response(200, json={"items": [{"id": "p1"}]}),
        ],
        ids=["transport", "not-json", "no-hits"],
    )
    async def test_a_sample_that_cannot_be_read_is_refused(self, answer) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/openapi.json":
                return httpx.Response(200, json=_DOC)

            if isinstance(answer, Exception):
                raise answer

            return answer

        with pytest.raises(CoreException) as error:
            await run(
                _plan(_GET),
                ClosedRamp(steps=[1], step_s=0.1),
                transport=httpx.MockTransport(handler),
            )

        assert error.value.code == "load_sample_failed"
        assert "p1" not in error.value.summary


class TestRunGuards:
    async def test_two_principals_running_out_stop_the_run_once(self) -> None:
        app = _App({"ada": ["a1"], "bob": ["b1"]})
        app.gone = {"a1", "b1"}
        handle = app.handler

        async def slow_gets(request: httpx.Request) -> httpx.Response:
            if request.url.path in ("/products/a1", "/products/b1"):
                await asyncio.sleep(0.02)  # both calls in flight when the first answers

            return await handle(request)

        plan = _plan(
            _GET,
            principals=[
                Principal(name="ada", mix={"products.get": 1}, auth=_as("ada")),
                Principal(name="bob", mix={"products.get": 1}, auth=_as("bob")),
            ],
        )

        result = await run(
            plan, ClosedRamp(steps=[2, 4], step_s=0.3), transport=httpx.MockTransport(slow_gets)
        )

        assert len(result.rows) == 1
        assert result.summary["stopped"]["principal"] in {"ada", "bob"}  # the first one only
        assert len(result.rows[0]["sample_exhausted"]) == 2

    async def test_the_first_of_two_scenario_errors_is_the_one_raised(self) -> None:
        raised: list[str] = []

        async def broken(call: Call) -> httpx.Response:
            await asyncio.sleep(0)  # both workers are inside a call before either raises
            raised.append(call.principal)

            raise RuntimeError(f"broken #{len(raised)}")

        plan = LoadPlan(
            base_url="http://127.0.0.1:9",
            principals=[Principal(name="ada", mix={"broken": 1})],
            scenarios=[Scenario("broken", broken, writes=False)],
        )

        with pytest.raises(RuntimeError, match="broken #1"):
            await run(plan, ClosedRamp(steps=[2], step_s=0.5), transport=httpx.MockTransport(_ok))

        assert len(raised) == 2

    async def test_a_call_for_a_principal_with_nothing_left_sends_nothing(self) -> None:
        from forze_load.catalog import resolve
        from forze_load.engine import _Run
        from forze_load.stats import StepRecorder

        app = _App({"ada": ["p1"]})
        plan = _plan(_GET)

        async with httpx.AsyncClient(
            base_url=plan.base_url, transport=httpx.MockTransport(app.handler)
        ) as client:
            state = _Run(
                plan=plan,
                catalog=await resolve(plan, client),
                client=client,
                auth_client=client,
                cpu_judged=True,
            )
            state.mixes["ada"] = ([], [])  # an arrival scheduled before its principal ran out
            step = StepRecorder(warm_until=0.0)

            await state.call(plan.principals[0], time.perf_counter(), step)

        assert app.paths() == ["/openapi.json"]
        assert step.summary(1.0)["calls"] == 0
        assert step.inflight == 0


async def _ok(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200)


class TestBodyMisfit:
    """The body check, alone: whatever it does not model it refuses."""

    @pytest.mark.parametrize(
        ("schema", "body", "reason"),
        [
            (
                {"properties": []},
                {},
                "its body schema has 'properties' or 'required' of the wrong shape",
            ),
            (
                {"required": "size"},
                {},
                "its body schema has 'properties' or 'required' of the wrong shape",
            ),
            ({"additionalProperties": False}, {"size": 5}, "its body cannot carry ['size']"),
            (
                {"additionalProperties": {"type": "integer"}},
                {"size": 5},
                "its body cannot carry ['size']",
            ),
            ({"minProperties": "1"}, {}, "its body cannot be an empty object"),
            (
                {"properties": {"size": True}},
                {"size": 5},
                "its body's 'size' schema is not an object",
            ),
            (
                {"properties": {"size": {"minimum": 10, "exclusiveMinimum": True}}},
                {"size": 10},
                "its body's 'size' cannot be 10",
            ),
            (
                {"properties": {"size": {"maximum": 5, "exclusiveMaximum": True}}},
                {"size": 5},
                "its body's 'size' cannot be 5",
            ),
            (
                {"properties": {"size": {"maximum": "100"}}},
                {"size": 5},
                "its body's 'size' schema has a bound that is not a number",
            ),
        ],
    )
    def test_it_refuses(self, schema, body, reason) -> None:
        assert _body_misfit(schema, body) == reason

    @pytest.mark.parametrize(
        ("schema", "body"),
        [
            ({"properties": {"size": {"minimum": 5, "exclusiveMinimum": False}}}, {"size": 5}),
            ({"properties": {"size": {"maximum": 5, "exclusiveMaximum": False}}}, {"size": 5}),
            (
                {"properties": {"size": {"type": ["integer", "null"], "nullable": True}}},
                {"size": 5},
            ),
            ({"additionalProperties": True}, {}),
        ],
    )
    def test_it_admits(self, schema, body) -> None:
        assert _body_misfit(schema, body) is None
