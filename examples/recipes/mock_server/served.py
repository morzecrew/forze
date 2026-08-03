"""The `MockApp` declaration — what ``forze mock serve`` resolves.

The whole point of keeping this in its own module: it declares *nothing about the app*. The
factory, the identity wiring and the specs all come from ``app.py``, unchanged. This file
only says which real modules to keep (none — fully mocked), what to seed, and that the
control plane is on.

    FORZE_MOCK_SERVER=1 forze mock serve examples.recipes.mock_server.served:mock_app

Then provoke the states a frontend needs, instead of waiting for them::

    curl -X POST localhost:8000/_mock/fault  -d '{"route":"products","op":"update","kind":"conflict"}'
    curl -X POST localhost:8000/_mock/latency -d '{"route":"products","seconds":2}'
    curl -X POST localhost:8000/_mock/reset
"""

from __future__ import annotations

from forze_identity.authz import policy_principal_spec
from forze_identity.builtin.local import from_json_path, local_identity_deps
from forze_mock.seeding import SeedPlan, spec_seed
from forze_mock.server import MockApp

from examples.recipes.mock_server.app import (
    AUTHN,
    DEV_PRINCIPAL,
    KEY_FILE,
    build_app,
    product_spec,
)

# ----------------------- #

_CATALOG = (("Espresso", 250), ("Cortado", 320), ("Filter", 280))


# --8<-- [start:plan]
seed_plan = SeedPlan(
    specs=(
        # The dev key's principal, at the id the key file names — an identity row has to
        # exist under *that* id or the app's own eligibility gate refuses the credential.
        spec_seed(
            policy_principal_spec,
            fixtures=({"id": str(DEV_PRINCIPAL), "kind": "user"},),
        ),
        # Fixtures for the rows a demo shows; `count=` would add generated volume behind them.
        spec_seed(
            product_spec,
            fixtures=tuple({"name": name, "price": price} for name, price in _CATALOG),
        ),
    ),
)
# --8<-- [end:plan]


# --8<-- [start:declaration]
mock_app = MockApp(
    build_app=build_app,
    # Real modules would go in `modules=`; the mock composes under them as a fallback, so
    # "real Postgres for documents, mock for the rest" is this list and nothing else.
    deps=(local_identity_deps(from_json_path(KEY_FILE), authn_route=AUTHN.name),),
    seed=seed_plan,
)
# --8<-- [end:declaration]
