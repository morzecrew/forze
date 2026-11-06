"""Plans the ``forze load`` command tests point at by import string.

The scenarios answer without the network, so ``run`` exercises the whole command offline.
"""

from __future__ import annotations

import attrs
import httpx

from forze_load import Call, LoadPlan, Principal, Scenario, password_login

# ----------------------- #


async def _offline(_call: Call) -> httpx.Response:
    return httpx.Response(200)


def _plan(*, writes: bool) -> LoadPlan:
    return LoadPlan(
        base_url="http://127.0.0.1:9",
        principals=[Principal(name="viewer", mix={"read": 1})],
        scenarios=[Scenario("read", _offline, writes=writes)],
    )


async def _unavailable(_call: Call) -> httpx.Response:
    return httpx.Response(503)


plan = _plan(writes=False)
writing_plan = _plan(writes=True)
# A soak on client numbers only: nothing listens on port 9 to serve `/metrics`.
soak_plan = attrs.evolve(plan, metrics=None)
failing_plan = LoadPlan(
    base_url="http://127.0.0.1:9",
    principals=[Principal(name="viewer", mix={"read": 1})],
    scenarios=[Scenario("read", _unavailable, writes=False)],
)
# Signs in against a port nothing listens on: a transport error at sign-in.
login_plan = LoadPlan(
    base_url="http://127.0.0.1:9",
    principals=[
        Principal(
            name="viewer",
            mix={"read": 1},
            auth=password_login("viewer", password_env="FORZE_LOAD_TEST_PASSWORD"),
        )
    ],
    scenarios=[Scenario("read", _offline, writes=False)],
)


# Resolved from a document given inline, so no request is needed to read it. Nothing listens
# on port 9, so the generated read fails in transport and the ramp stops after one step.
_DOC = {
    "paths": {
        "/items/list": {
            "post": {
                "operationId": "items.list",
                "x-forze-kind": "query",
                "requestBody": {"required": True, "content": {"application/json": {"schema": {}}}},
            }
        },
        "/items/{id}": {"get": {"operationId": "items.get", "x-forze-kind": "command"}},
        "/items/search": {
            "get": {
                "operationId": "items.search",
                "x-forze-kind": "query",
                "parameters": [{"name": "q", "in": "query", "required": True}],
            }
        },
    }
}
sweep_plan = LoadPlan(
    base_url="http://127.0.0.1:9",
    principals=[Principal(name="viewer", mix={"reads": 1})],
    scenarios=[Scenario.reads("items.")],
    openapi=_DOC,
)
mismatch_plan = LoadPlan(
    base_url="http://127.0.0.1:9",
    principals=[Principal(name="viewer", mix={"items.get": 1})],
    scenarios=[Scenario.op("items.get")],
    openapi=_DOC,
)


def make_plan() -> LoadPlan:
    return plan


def refusing_factory() -> LoadPlan:
    password_login("ada", password_env="LOAD_PW", refresh_path="//evil.example/refresh")

    return plan


not_a_plan = 42
