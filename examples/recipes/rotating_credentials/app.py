"""Recipe: counterparty-rotated credentials — surviving a rotation you don't control.

Some providers rotate the credential *at* you: every refresh burns the token you presented
and hands back a replacement. Two things then matter far more than usual. The replacement
must be **durable before anyone uses it** — the provider committed the burn before you
committed anything, so a crash in between locks the grant out. And the exchange must be
**serialized**, because presenting an already-rotated token is reuse, and reuse detection
revokes the whole grant family. ``RotatingCredentialStorePort`` owns both.

Mock-runnable — no provider and no database needed; the "provider" is an in-memory OAuth
server that burns tokens and punishes reuse exactly like the real ones.

Run it:  uv run python -m examples.recipes.rotating_credentials.app
Exercised by tests/unit/test_examples/test_rotating_credentials.py.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import timedelta

import structlog

from forze.application.contracts.secrets import (
    INVALID_GRANT_CODE,
    ExchangedCredential,
    RotatingCredentialsDepKey,
    RotatingCredentialStorePort,
    SecretRef,
)
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.exceptions import exc
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.base.primitives import utcnow
from forze_kits.integrations.durable import DurableFunctionRegistry, durable_kits_deps
from forze_kits.integrations.durable.runner import DurableFunctionRunner
from forze_kits.integrations.secrets import CredentialSweeper
from forze_mock import MockDepsModule, MockState

_LOGGER_NAME = "rotating_credentials"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration and any framework logs cleanly (and filter trace/debug),
    # **only when run as a script** — leaving global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


GRANT_REF = SecretRef("oauth/crm")


# --8<-- [start:provider]
class DemoOAuthProvider:
    """Stands in for the third party: rotates refresh tokens, and punishes reuse.

    A production provider behaves this way whether or not you are ready for it — which is
    the whole reason this plane exists.
    """

    def __init__(self) -> None:
        self.live_access: str | None = None
        self.live_refresh: str | None = None
        self.spent: set[str] = set()
        self.grant_revoked = False
        self.exchanges = 0
        self._generation = 0

    def authorize(self) -> tuple[str, str]:
        """What a human completing the consent flow hands you, once."""

        self._generation += 1
        self.live_access = f"access-{self._generation}"
        self.live_refresh = f"refresh-{self._generation}"
        self.grant_revoked = False

        return self.live_access, self.live_refresh

    def accepts(self, access_token: str) -> bool:
        return not self.grant_revoked and access_token == self.live_access

    def expire_access_token(self) -> None:
        """Time passing, from the provider's point of view."""

        self.live_access = None

    def revoke(self) -> None:
        self.grant_revoked = True

    def exchange(self, refresh_token: str) -> tuple[str, str]:
        self.exchanges += 1

        if self.grant_revoked:
            raise PermissionError("invalid_grant: the grant was revoked")

        if refresh_token in self.spent:
            # Reuse detection: the whole family dies, not just this call.
            self.grant_revoked = True

            raise PermissionError("invalid_grant: refresh token reuse detected")

        if refresh_token != self.live_refresh:
            raise PermissionError("invalid_grant: unknown refresh token")

        self.spent.add(refresh_token)

        return self.authorize()


class DemoTokenExchanger:
    """The application's half: one bounded call to the provider's token endpoint.

    A production exchanger issues an HTTP request (typically over ``forze_http``). The only
    thing it must get right beyond that is the classification below — a transient failure
    reported as an invalid grant destroys a working credential.
    """

    def __init__(self, provider: DemoOAuthProvider) -> None:
        self.provider = provider

    async def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Mapping[str, str],
    ) -> ExchangedCredential:
        try:
            access, refresh = self.provider.exchange(refresh_token)

        except PermissionError as e:
            # Permanent: the provider has rejected the grant itself, so the store records a
            # burn notice and callers route to re-authorization instead of retrying.
            raise exc.precondition(str(e), code=INVALID_GRANT_CODE) from e

        return ExchangedCredential(
            access_token=access,
            refresh_token=refresh,
            expires_at=utcnow() + timedelta(hours=1),
            metadata=dict(metadata),
        )


# --8<-- [end:provider]


# --8<-- [start:call]
async def call_api(store: RotatingCredentialStorePort, provider: DemoOAuthProvider) -> str:
    """Use the credential, refreshing it when the provider stops accepting it.

    The whole pattern: read, and if the token is spent, hand the version you read back to
    ``refresh``. That version is what lets the store tell "nobody has rotated yet" from
    "somebody already did" — the loser of a race gets the winner's credential instead of
    burning a token twice.
    """

    credential = await store.get(GRANT_REF)

    if provider.accepts(credential.access_token):
        return "ok"

    fresh = await store.refresh(GRANT_REF, observed=credential.version)

    return "ok" if provider.accepts(fresh.access_token) else "rejected"


# --8<-- [end:call]


# --8<-- [start:authorize]
async def authorize(store: RotatingCredentialStorePort, provider: DemoOAuthProvider) -> None:
    """Store a freshly consented grant — the entry point, and the only way back from a burn."""

    access, refresh = provider.authorize()

    await store.put(
        GRANT_REF,
        ExchangedCredential(
            access_token=access,
            refresh_token=refresh,
            expires_at=utcnow() + timedelta(hours=1),
            # Carried forward on every rotation, so an account-specific endpoint stays
            # addressable without a second lookup.
            metadata={"host": "crm.example"},
        ),
    )


# --8<-- [end:authorize]


# --8<-- [start:sweep-wiring]
def build_context(
    provider: DemoOAuthProvider,
    *,
    refresh_if_idle_for: timedelta = timedelta(days=30),
) -> tuple[ExecutionContext, MockState, DurableFunctionRunner, CredentialSweeper]:
    # The exchanger is the one thing the framework cannot default: it is a call to someone
    # else's provider, so passing it is what registers the store at all.
    state = MockState()

    # The sweeper is two durable functions on the app's registry: a per-tenant sweep that
    # scans for idle grants, and a per-grant refresh it enqueues — so one dead provider
    # costs one failing run, never a stalled pass. The idle window is per-provider
    # configuration: set it well inside their documented inactivity limit.
    registry = DurableFunctionRegistry()
    sweeper = CredentialSweeper(refresh_if_idle_for=refresh_if_idle_for)
    sweeper.register(registry)
    durable_deps, runner, _scheduler = durable_kits_deps(registry=registry)

    ctx = ExecutionContext(
        deps=DepsRegistry.from_deps(
            MockDepsModule(state=state, rotating_credentials=DemoTokenExchanger(provider))(),
            durable_deps,
        )
        .freeze()
        .resolve()
    )

    return ctx, state, runner, sweeper


# --8<-- [end:sweep-wiring]


async def main() -> None:
    provider = DemoOAuthProvider()
    # A tiny idle window so the demo's sweep finds the grant due within milliseconds;
    # production uses days (see sweep_act).
    ctx, _, runner, sweeper = build_context(provider, refresh_if_idle_for=timedelta(milliseconds=10))
    # resolve_simple, not provide: the store registers as a per-scope factory (it carries
    # the scope's tenant provider), on the mock exactly as on Postgres.
    store = ctx.deps.resolve_simple(ctx, RotatingCredentialsDepKey)

    await authorize(store, provider)
    log.info("authorized", exchanges=provider.exchanges)

    # A live token needs no rotation at all.
    log.info("call with a live token", result=await call_api(store, provider))

    # The provider stops accepting the access token: one worker notices and rotates.
    provider.expire_access_token()
    log.info(
        "call after expiry",
        result=await call_api(store, provider),
        exchanges=provider.exchanges,
    )

    # Five workers notice at the same moment. Exactly one exchange may happen — a second
    # would present a burned token and revoke the family.
    provider.expire_access_token()
    before = provider.exchanges
    results = await asyncio.gather(*(call_api(store, provider) for _ in range(5)))
    log.info(
        "five concurrent workers",
        results=sorted(set(results)),
        exchanges_used=provider.exchanges - before,
        grant_alive=not provider.grant_revoked,
    )

    # The grant dies upstream (an admin revokes access). The store records a terminal burn
    # notice, so callers stop hammering the provider and escalate to re-authorization.
    provider.revoke()
    provider.expire_access_token()

    try:
        await call_api(store, provider)

    except Exception as e:
        log.info("grant burnt", error=str(e))

    # Re-authorization is the documented way back.
    await authorize(store, provider)
    log.info("re-authorized", result=await call_api(store, provider))

    await sweep_act(ctx, runner, sweeper, provider)


# --8<-- [start:sweep]
async def sweep_act(
    ctx: ExecutionContext,
    runner: DurableFunctionRunner,
    sweeper: CredentialSweeper,
    provider: DemoOAuthProvider,
) -> None:
    """The half on-demand refresh cannot do: keep a grant alive that nobody is using.

    Providers expire refresh tokens from *non-use* — weeks to months, reset by every
    exchange — so a tenant that goes quiet loses its grant permanently unless something
    exchanges on its behalf before the deadline. Nothing in this act calls the API.
    """

    # Production wires the cadence once; each firing becomes a durable sweep run.
    schedule = await sweeper.ensure_cron(ctx, cron="0 4 * * *")
    log.info("sweep scheduled", schedule_id=schedule.schedule_id)

    # A quarter with no traffic goes by (compressed: the demo window is milliseconds
    # where production uses days, purely so this example runs in the blink of an eye).
    await asyncio.sleep(0.05)

    exchanges_before = provider.exchanges
    record = await sweeper.sweep_now(ctx)  # what the cron fires, run inline

    # The sweep enqueued one durable refresh run per due grant; a worker drains them.
    while await runner.recover(ctx, limit=10):
        pass

    outcome = record.output_json or {}
    log.info(
        "idle grant kept alive without a single API call",
        due=outcome.get("due"),
        exchanges_spent=provider.exchanges - exchanges_before,
        # Not named *authorization*: the log masker redacts matching keys, and this is a
        # list of ref paths — operator routing data, never a secret.
        re_consent_needed=outcome.get("needs_reauthorization"),
    )


# --8<-- [end:sweep]


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
