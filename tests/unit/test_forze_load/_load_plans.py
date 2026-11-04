"""Plans the ``forze load`` command tests point at by import string.

The scenarios answer without the network, so ``run`` exercises the whole command offline.
"""

from __future__ import annotations

import httpx

from forze_load import Call, LoadPlan, Principal, Scenario

# ----------------------- #


async def _offline(_call: Call) -> httpx.Response:
    return httpx.Response(200)


def _plan(*, writes: bool) -> LoadPlan:
    return LoadPlan(
        base_url="http://127.0.0.1:9",
        principals=[Principal(name="viewer", mix={"read": 1})],
        scenarios=[Scenario("read", _offline, writes=writes)],
    )


plan = _plan(writes=False)
writing_plan = _plan(writes=True)


def make_plan() -> LoadPlan:
    return plan


not_a_plan = 42
