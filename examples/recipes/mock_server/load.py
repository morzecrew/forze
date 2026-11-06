"""Recipe: load-test the mock server with `forze_load`, calling its operations by key.

The plan names no path. Each scenario names an operation key, the route's `operationId`,
and the run reads the method and path from the app's `/openapi.json` before anyone signs
in. It also checks every route's `x-forze-kind` against the scenario's `writes`, so a read
that turns out to write is refused before any load. `products.get` calls ids sampled from
`products.list` as each principal, after sign-in.

Serve the app (``just run``), then from the repository root:

    MOCK_SERVER_API_KEY=dev-key forze load run examples.recipes.mock_server.load:plan \\
        --steps 1,2,4,8 --step-s 10 --warm-s 2 --out ramp.jsonl

The create scenario writes, so the plan sets `allow_writes=True`. The run still refuses it
unless the target is loopback or an allowlisted host. Point a write plan at a disposable
stack only: the run never deletes what it created.

Exercised by ``tests/unit/test_examples/test_mock_server_load.py``.
"""

from __future__ import annotations

from forze_load import LoadPlan, Principal, Scenario, api_key, sample

# ----------------------- #

KEY_ENV = "MOCK_SERVER_API_KEY"
"""The variable holding the API key. Credentials come from the environment only."""


# --8<-- [start:plan]
def build_plan(base_url: str = "http://localhost:8000") -> LoadPlan:
    """Two principals: browsers that read, and an editor that also creates."""

    # The recipe's key file holds one key, so both principals send it. A real plan gives
    # each principal its own credential, so each one sees only what its role may read.
    return LoadPlan(
        base_url=base_url,
        principals=[
            Principal(
                name="browser", weight=3, mix={"products.get": 3, "list": 1}, auth=api_key(KEY_ENV)
            ),
            Principal(name="editor", weight=1, mix={"list": 1, "create": 1}, auth=api_key(KEY_ENV)),
        ],
        scenarios=[
            Scenario.op("products.get", path={"id": sample("products.list", size=100)}),
            Scenario.op("products.list", name="list", body={"size": 20}),
            Scenario.op(
                "products.create",
                name="create",
                writes=True,
                body={"name": "load", "price": 1},
            ),
        ],
        allow_writes=True,
    )


plan = build_plan()
# --8<-- [end:plan]
