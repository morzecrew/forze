"""A double-entry ledger and the cross-aggregate law that keeps it honest.

The entity-level ``@invariant`` can say "an account's balance is an integer" — but not "the balances
in a ledger sum to zero", because that ranges over *every* account in the ledger, not one. That
conservation law is a :class:`~forze.application.contracts.invariants.SystemInvariant`: a predicate
over an **aggregate** of a **read-set** — here ``$sum(balance)`` over the accounts of one ledger,
which must equal zero. This is exactly the class of correctness rule no per-entity check can express.

The law is enforced **detectively**: after a balance-changing operation commits, ``enforce`` re-reads
the ledger's sum and raises if it drifted. *Detective* is the honest word — the check runs
post-commit, so it **reports** a breach, it does not prevent it (the offending write is already
durable). Preventive enforcement, and verifying the law under simulated concurrency with a DST
oracle, are later phases of the same declaration.

Double-entry bookkeeping keeps the sum at zero: a ledger opens with balancing entries (an asset of
``+100`` against a liability of ``-100``) and a :func:`transfer` moves value between accounts without
changing the total. The bug the law catches is a **single-sided** write — :func:`mint`, a credit with
no balancing debit — which leaves the ledger summing to something other than zero.

Run it (from the repo root)::

    python -m examples.recipes.ledger_invariant.app
"""

from uuid import UUID

import structlog

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.invariants import ReadSet, Sum, SystemInvariant
from forze.application.execution import ExecutionContext
from forze.application.execution.deps import DepsRegistry
from forze.base.exceptions import CoreException
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates import aggregate_repository
from forze_kits.invariants import InvariantResult, enforce, enforce_preventive, evaluate
from forze_mock import MockDepsModule

_LOGGER_NAME = "ledger_invariant"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration and any framework logs cleanly (and filter trace/debug),
    # **only when run as a script** — leaving global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])

# ----------------------- #
# Domain — a ledger account, persisted through the document port.


class Account(Document):
    ledger_id: str
    balance: int = 0

    def debit(self, amount: int) -> "AccountUpdate":
        """Decide a debit — the balance decreases by *amount* (double-entry, so it may go negative;
        the ledger-wide conservation law, not a per-account guard, keeps the books balanced)."""

        return AccountUpdate(balance=self.balance - amount)

    def credit(self, amount: int) -> "AccountUpdate":
        """Decide a credit — the balance increases by *amount*."""

        return AccountUpdate(balance=self.balance + amount)


class AccountCreate(CreateDocumentCmd):
    ledger_id: str
    balance: int = 0


class AccountUpdate(BaseDTO):
    balance: int | None = None


class AccountRead(ReadDocument):
    ledger_id: str
    balance: int = 0


ACCOUNT_SPEC = DocumentSpec(
    name="ledger_accounts",
    read=AccountRead,
    write=DocumentWriteTypes(
        domain=Account, create_cmd=AccountCreate, update_cmd=AccountUpdate
    ),
)

# The cross-aggregate law: a ledger's balances sum to zero (double-entry conservation). A predicate
# over an aggregate of a scoped read-set — what `@invariant` cannot express.
LEDGER_BALANCED = SystemInvariant(
    name="ledger_balanced",
    read_set=ReadSet(spec=ACCOUNT_SPEC, scope_keys=("ledger_id",)),
    aggregate=Sum("balance"),
    holds=lambda total: total == 0,
)

_ROUTE = "mock"


# ....................... #
# Context + operations — ordinary forze code over ports; only `enforce` knows about the law.


def build_context() -> ExecutionContext:
    """An in-process context backed by the in-memory mock adapters."""

    return ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
    )


async def open_account(ctx: ExecutionContext, ledger_id: str, balance: int) -> UUID:
    """Open an account. Setup only — a single open need not balance; a ledger opens with balancing
    entries (e.g. ``+100`` and ``-100``), and the law is checked once a complete operation lands.
    """

    account = await ctx.document.command(ACCOUNT_SPEC).create(
        AccountCreate(ledger_id=ledger_id, balance=balance)
    )
    return account.id


async def transfer(
    ctx: ExecutionContext, ledger_id: str, src: UUID, dst: UUID, amount: int
) -> None:
    """Move *amount* from *src* to *dst* in one transaction — double-entry, so the sum is preserved.

    ``enforce`` defers the conservation check to post-commit; a correct transfer leaves the ledger
    summing to zero, so it passes silently.
    """

    async with ctx.tx_ctx.scope(_ROUTE):
        accounts = aggregate_repository(ctx, ACCOUNT_SPEC)
        source = await accounts.load(src)
        dest = await accounts.load(dst)
        await accounts.apply(source, source.debit(amount))
        await accounts.apply(dest, dest.credit(amount))
        await enforce(LEDGER_BALANCED, ctx, {"ledger_id": ledger_id})


async def mint(
    ctx: ExecutionContext, ledger_id: str, account: UUID, amount: int
) -> None:
    """BUG: credit an account with **no balancing debit** — a single-sided write.

    The transaction commits the credit, then the deferred conservation check finds the ledger no
    longer sums to zero and raises. Detective, not preventive: the bad credit is already durable when
    the breach is reported — which is exactly the honest limit of a post-commit check.
    """

    async with ctx.tx_ctx.scope(_ROUTE):
        accounts = aggregate_repository(ctx, ACCOUNT_SPEC)
        current = await accounts.load(account)
        await accounts.apply(current, current.credit(amount))  # BUG: no balancing debit
        await enforce(LEDGER_BALANCED, ctx, {"ledger_id": ledger_id})


async def mint_guarded(
    ctx: ExecutionContext,
    ledger_id: str,
    account: UUID,
    amount: int,
) -> None:
    """The same single-sided credit as :func:`mint`, enforced **preventively**.

    The check runs *inside* a ``SERIALIZABLE`` transaction (the law's ``required_isolation``) and
    raises before commit, so the bad write is **rolled back** — never durable. ``enforce_preventive``
    fails closed unless the transaction meets that floor and the backend's conformance-verified
    capabilities report it; that isolation is exactly what serializes away a concurrent write-skew
    instead of letting two innocent-looking writes jointly break the law.
    """

    async with ctx.tx_ctx.scope(_ROUTE, isolation=LEDGER_BALANCED.required_isolation):
        current = await ctx.document.query(ACCOUNT_SPEC).get(account)

        await ctx.document.command(ACCOUNT_SPEC).update(
            account,
            current.rev,
            AccountUpdate(balance=current.balance + amount),
        )
        await enforce_preventive(LEDGER_BALANCED, ctx, {"ledger_id": ledger_id})


async def ledger_balance(ctx: ExecutionContext, ledger_id: str) -> InvariantResult:
    """Evaluate the conservation law without enforcing it — a read, for inspection."""

    return await evaluate(LEDGER_BALANCED, ctx, {"ledger_id": ledger_id})


# ....................... #


async def main() -> None:
    # Detective: a correct transfer preserves the law; a single-sided mint is reported *after* commit
    # — so the bad write is already durable when the breach surfaces.
    ctx = build_context()
    asset = await open_account(ctx, "L1", 100)  # an asset of +100 …

    liability = await open_account(
        ctx,
        "L1",
        -100,
    )  # … against a liability of -100 → balanced

    await transfer(ctx, "L1", asset, liability, 30)  # preserves the sum

    balanced = await ledger_balance(ctx, "L1")
    log.info("after transfer", observed=balanced.observed, held=balanced.held)

    try:
        await mint(ctx, "L1", asset, 50)  # single-sided → caught post-commit

    except CoreException as error:
        log.info("detective: mint breach reported post-commit", error=str(error))

    durable = await ledger_balance(ctx, "L1")  # held=False, observed=50
    log.info(
        "…but the bad write is already durable",
        observed=durable.observed,
        held=durable.held,
    )

    # Preventive: the same bad write, checked *inside* a SERIALIZABLE transaction, is rolled back —
    # never durable. (And under real concurrency that isolation serializes away a write-skew.)
    ctx2 = build_context()
    funded = await open_account(ctx2, "L2", 100)

    await open_account(ctx2, "L2", -100)  # balanced

    try:
        await mint_guarded(
            ctx2, "L2", funded, 50
        )  # single-sided → caught before commit

    except CoreException as error:
        log.info("preventive: mint_guarded rejected before commit", error=str(error))

    rolled = await ledger_balance(ctx2, "L2")  # held=True, observed=0
    log.info(
        "…and rolled back — the ledger stays balanced",
        observed=rolled.observed,
        held=rolled.held,
    )


# ....................... #

if __name__ == "__main__":
    import asyncio

    _setup_logging("info")
    asyncio.run(main())
