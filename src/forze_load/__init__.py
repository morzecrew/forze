"""Load testing for apps built on forze (RFC 0084).

Drives a running app over HTTP as several principals, step by step, under a closed or an
open load model, and writes one JSON line per step::

    from forze_load import ClosedRamp, LoadPlan, Principal, Scenario, api_key, run

    async def list_products(call):
        return await call.request("POST", "/products/list", json={"size": 20})

    plan = LoadPlan(
        base_url="http://localhost:8000",
        principals=[Principal(name="viewer", auth=api_key("VIEWER_KEY"),
                              mix={"list": 1})],
        scenarios=[Scenario("list", list_products, writes=False)],
    )
    result = await run(plan, ClosedRamp(steps=[1, 2, 4, 8], step_s=30, warm_s=5),
                       out="ramp.jsonl")

Writes are off by default: a scenario declared ``writes=True`` is refused unless the plan
allows writes and its host is loopback or allowlisted. Credentials come from environment
variables only. Needs the ``load`` extra.
"""

from .auth import (
    ApiKeyAuth,
    Auth,
    BearerAuth,
    HeadersFn,
    PasswordLogin,
    api_key,
    bearer,
    password_login,
)
from .engine import ClosedRamp, OpenRamp, Profile, RunResult, run
from .plan import (
    WRITE_HOSTS_ENV,
    Call,
    LoadPlan,
    Principal,
    Scenario,
    SendFn,
    validate_plan,
    write_host_allowed,
)
from .report import Comparison, RunFile, compare_runs, parse_fail_on, read_run, summarize

# ----------------------- #

__all__ = [
    "ApiKeyAuth",
    "Auth",
    "BearerAuth",
    "Call",
    "ClosedRamp",
    "Comparison",
    "HeadersFn",
    "LoadPlan",
    "OpenRamp",
    "PasswordLogin",
    "Principal",
    "Profile",
    "RunFile",
    "RunResult",
    "Scenario",
    "SendFn",
    "WRITE_HOSTS_ENV",
    "api_key",
    "bearer",
    "compare_runs",
    "parse_fail_on",
    "password_login",
    "read_run",
    "run",
    "summarize",
    "validate_plan",
    "write_host_allowed",
]
