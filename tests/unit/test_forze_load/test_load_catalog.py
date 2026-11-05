"""Operation references: resolution from OpenAPI, the kind check, samples and the read sweep."""

from __future__ import annotations

import json
import secrets
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
    Principal,
    RunResult,
    Scenario,
    password_login,
    run,
    sample,
)
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

        await _run(app, _plan(_GET), out=out)

        text = out.read_text()
        assert not any(i in text for i in ids)


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
            "a.union": "its body schema is composed (allOf, anyOf or oneOf)"
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
