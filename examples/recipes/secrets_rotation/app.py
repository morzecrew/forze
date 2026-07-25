"""Recipe: credential rotation — durable four-step rotation with hot reload.

A rotator mints a fresh credential, stages it at ``<path>.pending``, applies and
**verifies** it at the backend, and only then promotes — so an unverified password
can never reach the fleet. A poll watcher notices the promoted version and the
consumer re-resolves — the same contract the hot-reload binder drives for routed
pools. Mock-runnable — no Vault or Postgres needed; the "database" is an in-memory
credential table.

Run it:  uv run python -m examples.recipes.secrets_rotation.app
Exercised by tests/unit/test_examples/test_secrets_rotation.py.
"""

from __future__ import annotations

import asyncio

import structlog

from forze.application.contracts.secrets import (
    PendingCredential,
    SecretChanged,
    SecretRef,
    SecretsPort,
)
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze_kits.integrations.durable import DurableFunctionRegistry, durable_kits_deps
from forze_kits.integrations.secrets import SecretRotator, SecretsPollWatcher
from forze_mock import MockDepsModule, MockSecretsPort, MockState

_LOGGER_NAME = "secrets_rotation"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration and any framework logs cleanly (and filter trace/debug),
    # **only when run as a script** — leaving global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# --8<-- [start:target]
DSN_REF = SecretRef("db/app-dsn")


class DemoDatabase:
    """Stands in for Postgres: a table of credentials that can authenticate."""

    def __init__(self) -> None:
        self.passwords: dict[str, str] = {"app_a": "seed"}

    def authenticate(self, user: str, password: str) -> bool:
        return self.passwords.get(user) == password


class DemoRotationTarget:
    """The backend-specific steps: compose the pending DSN, set it, prove it works.

    A production deployment wires ``forze_postgres.PostgresRotationTarget`` here —
    same three methods, with a real ``ALTER ROLE`` and a real connection for verify.
    """

    def __init__(self, database: DemoDatabase, secrets: SecretsPort) -> None:
        self.database = database
        self.secrets = secrets

    async def compose(self, tenant_id: object, *, current: str, minted: str) -> str:
        user, _, _ = current.partition(":")  # "user:password" toy DSN
        return f"{user}:{minted}"

    async def apply(self, tenant_id: object, pending: PendingCredential) -> None:
        user, _, password = (await self.secrets.resolve_str(pending.ref)).partition(":")
        self.database.passwords[user] = password  # ALTER ROLE ... PASSWORD ...

    async def verify(self, tenant_id: object, pending: PendingCredential) -> None:
        user, _, password = (await self.secrets.resolve_str(pending.ref)).partition(":")

        if not self.database.authenticate(user, password):  # a REAL connection attempt
            raise RuntimeError("pending credential failed verification")


# --8<-- [end:target]


# --8<-- [start:rotate]
async def rotate(ctx: ExecutionContext, rotator: SecretRotator) -> str:
    # One durable run: create (mint + stage at db/app-dsn.pending) → set → test →
    # finish (promote + notify). A crash resumes from the last completed step; the
    # journal only ever carries {ref, version} — never the credential text.
    record = await rotator.rotate_now(ctx, DSN_REF)

    return str((record.output_json or {}).get("version_token", ""))


# --8<-- [end:rotate]


# --8<-- [start:watch]
async def observe_change(watcher: SecretsPollWatcher) -> SecretChanged | None:
    # In production the watcher runs as a lifecycle step (watcher.lifecycle_step())
    # and the hot-reload binder turns changes into routed-pool evictions; here we
    # drive one tick by hand and return what it observed.
    changes: list[SecretChanged] = []

    async def _drain() -> None:
        async for change in watcher.subscribe():
            changes.append(change)
            return

    task = asyncio.create_task(_drain())
    await asyncio.sleep(0)

    try:
        await watcher.tick()

        for _ in range(5):
            await asyncio.sleep(0)

    finally:
        task.cancel()

    return changes[0] if changes else None


# --8<-- [end:watch]


def build_context(registry: DurableFunctionRegistry) -> tuple[ExecutionContext, MockSecretsPort]:
    # One shared MockState: the store the handlers resolve is the store we seed.
    state = MockState()
    durable_deps, _, _ = durable_kits_deps(registry=registry)
    ctx = ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule(state=state))
        .with_deps(durable_deps)
        .freeze()
        .resolve()
    )

    return ctx, MockSecretsPort(state=state)


async def main() -> None:
    database = DemoDatabase()
    registry = DurableFunctionRegistry()
    ctx, secrets = build_context(registry)

    # Seed the current credential and wire the rotator.
    await secrets.put(DSN_REF, "app_a:seed")

    rotator = SecretRotator(
        target=DemoRotationTarget(database, secrets),
        publish_spec=None,  # notification fan-out needs a broker; the poll floor covers us here
    )
    rotator.register(registry)

    watcher = SecretsPollWatcher(secrets=secrets, refs=(DSN_REF,))
    await watcher.tick()  # first tick primes silently — no eviction storm at boot

    version = await rotate(ctx, rotator)
    log.info("rotation promoted", version=version)

    change = await observe_change(watcher)
    fresh = await secrets.resolve_str(DSN_REF)
    user, _, password = fresh.partition(":")
    log.info(
        "hot reload",
        observed_version=change.version.token if change else None,
        user=user,
        authenticates=database.authenticate(user, password),
    )


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
