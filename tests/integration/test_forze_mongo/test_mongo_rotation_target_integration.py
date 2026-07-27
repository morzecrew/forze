"""MongoDB rotation target against a live server — the shared conformance battery.

The battery holds the claims; this file supplies what it cannot know: how to provision a
user, what a credential value looks like, how to prove one authenticates, and how to stall
an ``updateUser`` past a minimal ``maxTimeMS``.

That last one is the whole reason MongoDB is admissible as a target at all. RFC 0035 admits
a backend only if it can *server-side* kill a late apply — a client giving up does not stop
a write already at the server — so the stall here provokes the real thing and the battery
asserts the write never lands.

# covers: RotationTargetPort.compose
# covers: RotationTargetPort.apply
# covers: RotationTargetPort.verify
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator, Mapping
from datetime import timedelta
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from pymongo import AsyncMongoClient

from forze.application.contracts.secrets import SecretsAdminDepKey, SecretsDepKey
from forze.base.primitives import JsonDict
from forze_kits.integrations.secrets import SecretRotator
from forze_mongo.adapters.rotation_target import MongoRotationTarget
from forze_mongo.kernel.client.client import MongoClient
from forze_mongo.kernel.uri import with_mongo_credentials
from tests.support.rotation_targets import (
    REF,
    ROTATION_TARGET_BATTERY,
    Check,
    RotationTargetHarness,
    check_the_verify_gate_halts_before_promote,
    rotation_context,
)

# ----------------------- #


def _principal_of(uri: str) -> str:
    from forze_mongo.kernel.uri import mongo_uri_username

    return mongo_uri_username(uri)


def _non_credential_facts(uri: str) -> Mapping[str, str]:
    parts = urlsplit(uri)
    authority = parts.netloc.rpartition("@")[2]

    return {"scheme": parts.scheme, "authority": authority, "path": parts.path}


async def _authenticates(uri: str) -> bool:
    connection: AsyncMongoClient[JsonDict] = AsyncMongoClient(
        uri, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000
    )

    try:
        await connection.admin.command("ping")
        return True

    except Exception:
        return False

    finally:
        await connection.close()


# ....................... #


async def _build(mongo_rotation_container, *, idle_can_login: bool) -> RotationTargetHarness:
    root_uri = mongo_rotation_container.get_connection_url()
    suffix = uuid.uuid4().hex[:8]
    user_a, user_b = f"app_a_{suffix}", f"app_b_{suffix}"

    root: AsyncMongoClient[JsonDict] = AsyncMongoClient(root_uri)
    await root.admin.command(
        {"createUser": user_a, "pwd": "seed-a", "roles": ["readWriteAnyDatabase"]}
    )
    base_uri = root_uri

    if idle_can_login:
        await root.admin.command(
            {"createUser": user_b, "pwd": "seed-b", "roles": ["readWriteAnyDatabase"]}
        )

    else:
        # The idle user exists — so `updateUser` succeeds and the apply step passes — but it
        # may only authenticate from an address that is not ours, so no password can log it
        # in. MongoDB's analogue of a NOLOGIN role, and the exact shape the verify gate
        # exists to catch: a backend that accepted the write and still cannot be reached.
        #
        # Restrictions survive a password-only update. Mechanisms notably do NOT — an
        # `updateUser` carrying `pwd` recomputes the credential set back to the server
        # default, so a SCRAM-mechanism mismatch cannot be used to build this case.
        await root.admin.command(
            {
                "createUser": user_b,
                "pwd": "seed-b",
                "roles": ["readWriteAnyDatabase"],
                "authenticationRestrictions": [{"clientSource": ["10.255.255.1"]}],
            }
        )

    initial = with_mongo_credentials(base_uri, username=user_a, password="seed-a")

    admin_client = MongoClient()
    await admin_client.initialize(root_uri, db_name="admin")

    ctx, registry = rotation_context()
    await ctx.deps.provide(SecretsAdminDepKey).put(REF, initial)

    secrets = ctx.deps.provide(SecretsDepKey)
    target = MongoRotationTarget(secrets=secrets, client=admin_client, user_pair=(user_a, user_b))
    rotator = SecretRotator(target=target, publish_spec=None)
    rotator.register(registry)

    @contextlib.asynccontextmanager
    async def provoke_late_apply() -> AsyncIterator[MongoRotationTarget]:
        bounded = MongoRotationTarget(
            secrets=secrets,
            client=admin_client,
            user_pair=(user_a, user_b),
            apply_max_time=timedelta(milliseconds=250),
        )
        # Block the next updateUser for far longer than that bound; the server must kill it.
        await root.admin.command(
            {
                "configureFailPoint": "failCommand",
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["updateUser"],
                    "blockConnection": True,
                    "blockTimeMS": 3000,
                },
            }
        )

        try:
            yield bounded

        finally:
            await root.admin.command({"configureFailPoint": "failCommand", "mode": "off"})

    def build_understating_target() -> MongoRotationTarget:
        # Below the client's configured server-selection plus connect timeout, which the
        # target reads off the client rather than assuming.
        return MongoRotationTarget(
            secrets=secrets,
            client=admin_client,
            user_pair=(user_a, user_b),
            dispatch_allowance=timedelta(milliseconds=1),
        )

    return RotationTargetHarness(
        ctx=ctx,
        rotator=rotator,
        target=target,
        principals=(user_a, user_b),
        initial_secret=initial,
        authenticates=_authenticates,
        principal_of=_principal_of,
        non_credential_facts=_non_credential_facts,
        provoke_late_apply=provoke_late_apply,
        build_understating_target=build_understating_target,
    )


@pytest_asyncio.fixture
async def harness(mongo_rotation_container) -> RotationTargetHarness:
    return await _build(mongo_rotation_container, idle_can_login=True)


# ....................... #


@pytest.mark.parametrize("check", ROTATION_TARGET_BATTERY, ids=lambda check: check.__name__)
async def test_rotation_target_battery(check: Check, harness: RotationTargetHarness) -> None:
    await check(harness)


async def test_verify_gate_halts_before_promote(mongo_rotation_container) -> None:
    gated = await _build(mongo_rotation_container, idle_can_login=False)

    await check_the_verify_gate_halts_before_promote(gated)
