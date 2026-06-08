"""``@invariant``: an always-true state rule enforced on create AND update.

The headline case is the footgun it closes: a raw Pydantic ``@model_validator`` is silently
skipped by Forze's merge-patch update (which uses ``model_copy``), so it can't guard updates.
``@invariant`` runs on both paths from one declaration.
"""

from __future__ import annotations

import pytest
from pydantic import model_validator

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import Document, invariant

# ----------------------- #


class Account(Document):
    balance: int = 0

    @invariant
    def _non_negative_balance(self) -> None:
        if self.balance < 0:
            raise exc.domain("balance must be non-negative")


class TestInvariant:
    def test_holds_on_create(self) -> None:
        assert Account(balance=10).balance == 10

        with pytest.raises(CoreException) as ei:
            Account(balance=-1)
        assert ei.value.kind is ExceptionKind.DOMAIN

    def test_holds_across_update(self) -> None:
        account = Account(balance=10)

        ok, _ = account.update({"balance": 5})
        assert ok.balance == 5

        # The footgun fix: a merge-patch update that would violate the invariant is rejected.
        with pytest.raises(CoreException) as ei:
            account.update({"balance": -3})
        assert ei.value.kind is ExceptionKind.DOMAIN

    def test_model_validator_alone_misses_the_update(self) -> None:
        # Contrast: a raw @model_validator is NOT run on update (model_copy bypass), so the
        # violating update slips through — exactly the gap @invariant closes.
        class LooseAccount(Document):
            balance: int = 0

            @model_validator(mode="after")
            def _check(self) -> LooseAccount:
                if self.balance < 0:
                    raise ValueError("negative")
                return self

        account = LooseAccount(balance=10)
        bad, _ = account.update({"balance": -5})

        assert bad.balance == -5  # invalid state persisted — no error raised

    def test_subclass_override_is_honoured(self) -> None:
        class Strict(Account):
            @invariant
            def _non_negative_balance(self) -> None:  # type: ignore[override]
                if self.balance < 100:
                    raise exc.domain("strict accounts need >= 100")

        with pytest.raises(CoreException):
            Strict(balance=50)
        assert Strict(balance=150).balance == 150

    def test_signature_is_guarded(self) -> None:
        with pytest.raises(CoreException) as ei:

            class Bad(Document):
                @invariant
                def _broken(self, extra: int) -> None: ...

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_no_invariants_is_a_noop(self) -> None:
        class Plain(Document):
            name: str = "x"

        plain = Plain()
        updated, _ = plain.update({"name": "y"})
        assert updated.name == "y"
