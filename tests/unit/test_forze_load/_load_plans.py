"""Plans the ``forze load`` command tests point at by import string.

The scenarios answer without the network, so ``run`` exercises the whole command offline.
"""

from __future__ import annotations

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


def make_plan() -> LoadPlan:
    return plan


def refusing_factory() -> LoadPlan:
    password_login("ada", password_env="LOAD_PW", refresh_path="//evil.example/refresh")

    return plan


not_a_plan = 42
