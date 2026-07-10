"""`bind_invariants` threads SystemInvariant enforcement into a write op's plan (mock).

A double-entry conservation law (a ledger's balances sum to zero) is bound to the account CREATE op.
Preventive enforcement rolls back a write that would break the law; detective enforcement lets the
write commit but surfaces the breach post-commit. That the preventive violation raises a *domain*
error (not a configuration one) also proves the op's transaction was opened at the law's isolation
floor — otherwise `enforce_preventive` would fail closed on the floor check first.
"""

from __future__ import annotations

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import build_document_registry
from forze_kits.aggregates.document.dto import ListRequestDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.invariants import InvariantEnforcement, bind_invariants
from forze_mock import MockDepsModule

# ----------------------- #

_MOCK_TX = "mock"


class Account(Document):
    ledger_id: str
    balance: int = 0


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

# A ledger's balances must sum to zero (double-entry conservation).
LEDGER_BALANCED = SystemInvariant(
    name="ledger_balanced",
    read_set=ReadSet(spec=ACCOUNT_SPEC, scope_keys=("ledger_id",)),
    aggregate=SumOf("balance"),
    holds=lambda total: total == 0,
)

_CREATE = ACCOUNT_SPEC.default_namespace.key(DocumentKernelOp.CREATE)


def _enforcement(mode: str) -> InvariantEnforcement:
    return InvariantEnforcement(
        law=LEDGER_BALANCED,
        params=lambda args, result: {"ledger_id": result.ledger_id},
        mode=mode,
    )


def _registry(*enforcements: InvariantEnforcement):
    reg = build_document_registry(ACCOUNT_SPEC)
    return bind_invariants(reg, _CREATE, *enforcements, tx_route=_MOCK_TX).freeze()


async def _create(reg, ctx, ledger_id: str, balance: int):
    return await run_operation(
        reg, _CREATE, AccountCreate(ledger_id=ledger_id, balance=balance), ctx
    )


# ....................... #


class TestBindInvariantsShape:
    def test_no_enforcements_is_a_noop(self) -> None:
        reg = build_document_registry(ACCOUNT_SPEC)
        assert bind_invariants(reg, _CREATE) is reg

    def test_binding_freezes_cleanly(self) -> None:
        _registry(_enforcement("preventive"))  # builds + freezes without error


# ....................... #


class TestPreventiveEnforcement:
    async def test_balanced_write_commits(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _registry(_enforcement("preventive"))

        async with runtime.scope():
            ctx = runtime.get_context()
            account = await _create(reg, ctx, "L", 0)  # sum stays 0 — the law holds
            assert account.balance == 0

    async def test_unbalancing_write_is_rolled_back(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _registry(_enforcement("preventive"))

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException) as ei:
                await _create(reg, ctx, "L", 5)  # sum would be 5 — the law breaks

            # A *domain* violation (not a configuration/isolation-floor error) — so the op's tx was
            # opened at the law's required isolation, and the bad write never became durable.
            assert ei.value.kind is ExceptionKind.DOMAIN

            listed = await run_operation(
                reg, ACCOUNT_SPEC.default_namespace.key(DocumentKernelOp.LIST), ListRequestDTO(), ctx
            )
            assert listed.count == 0  # the rolled-back account is not present


# ....................... #


class TestDetectiveEnforcement:
    async def test_breaching_write_commits_but_surfaces(self) -> None:
        runtime = build_runtime(MockDepsModule())
        reg = _registry(_enforcement("detective"))

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException) as ei:
                await _create(reg, ctx, "L", 5)
            assert ei.value.kind is ExceptionKind.DOMAIN

            # Detective, not preventive: the write DID commit — the breach is surfaced, not prevented.
            listed = await run_operation(
                reg, ACCOUNT_SPEC.default_namespace.key(DocumentKernelOp.LIST), ListRequestDTO(), ctx
            )
            assert listed.count == 1
