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

A scenario can also name an operation by key, ``Scenario.op("products.get", path={"id":
sample("products.list")})``, or sweep every read route, ``Scenario.reads("products.")``:
the run resolves both from the app's OpenAPI document and stops before load when an
operation is missing or its ``x-forze-kind`` contradicts the declared ``writes``.

A ``Soak`` holds one load level for hours, writes a row per window, and flags upward drift
in client latency and in the server's ``forze_*`` and process series scraped from
``/metrics``. ``Realtime`` holds SSE or WebSocket subscribers instead, writes at a set rate,
times each write's frame at every subscriber it addresses, and drops every stream at once to
measure the resume from ``Last-Event-ID``.

Writes are off by default: a scenario declared ``writes=True`` is refused unless the plan
allows writes and its host is loopback or allowlisted. Credentials come from environment
variables only. Needs the ``load`` extra.
"""

from ._wire import Frame
from .auth import (
    ApiKeyAuth,
    Auth,
    AuthSession,
    BearerAuth,
    HeadersFn,
    PasswordLogin,
    api_key,
    bearer,
    password_login,
)
from .engine import ClosedRamp, OpenRamp, Profile, RunResult, Soak, inflight_cap, run
from .plan import (
    WRITE_HOSTS_ENV,
    Call,
    LoadPlan,
    OpScenario,
    PlanScenario,
    Principal,
    ReadSweep,
    Sample,
    Scenario,
    SendFn,
    sample,
    validate_plan,
    write_host_allowed,
)
from .realtime import Realtime
from .report import Comparison, RunFile, compare_runs, parse_fail_on, read_run, summarize

# ----------------------- #

__all__ = [
    "ApiKeyAuth",
    "Auth",
    "AuthSession",
    "BearerAuth",
    "Call",
    "ClosedRamp",
    "Comparison",
    "Frame",
    "HeadersFn",
    "LoadPlan",
    "OpScenario",
    "OpenRamp",
    "PasswordLogin",
    "PlanScenario",
    "Principal",
    "Profile",
    "ReadSweep",
    "Realtime",
    "RunFile",
    "RunResult",
    "Sample",
    "Scenario",
    "SendFn",
    "Soak",
    "WRITE_HOSTS_ENV",
    "api_key",
    "bearer",
    "compare_runs",
    "inflight_cap",
    "parse_fail_on",
    "password_login",
    "read_run",
    "run",
    "sample",
    "summarize",
    "validate_plan",
    "write_host_allowed",
]
