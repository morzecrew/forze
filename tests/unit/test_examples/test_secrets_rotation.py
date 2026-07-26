"""Credential-rotation recipe — verify-before-promote rotation + poll-observed reload (mock)."""

from __future__ import annotations

from examples.recipes.secrets_rotation.app import (
    DSN_REF,
    DemoDatabase,
    DemoRotationTarget,
    build_context,
    observe_change,
    rotate,
)
from forze_kits.integrations.durable import DurableFunctionRegistry
from forze_kits.integrations.secrets import SecretRotator, SecretsPollWatcher


async def test_rotation_promotes_verified_credential_and_watcher_observes_it() -> None:
    database = DemoDatabase()
    registry = DurableFunctionRegistry()
    ctx, secrets = build_context(registry)

    await secrets.put(DSN_REF, "app_a:seed")

    rotator = SecretRotator(
        target=DemoRotationTarget(database, secrets), publish_spec=None
    )
    rotator.register(registry)

    watcher = SecretsPollWatcher(secrets=secrets, refs=(DSN_REF,))
    await watcher.tick()  # prime

    version = await rotate(ctx, rotator)
    assert version

    change = await observe_change(watcher)
    assert change is not None
    assert change.ref == DSN_REF
    assert change.version.token == version

    # The promoted credential authenticates against the (demo) backend, and the
    # old seed no longer does — the rotation actually happened.
    user, _, password = (await secrets.resolve_str(DSN_REF)).partition(":")
    assert database.authenticate(user, password)
    assert not database.authenticate("app_a", "seed")
