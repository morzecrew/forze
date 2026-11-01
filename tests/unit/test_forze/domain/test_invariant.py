"""``@invariant``: an always-true state rule enforced on create AND update.

Forze's merge-patch update fully re-validates the merged state, so both raw Pydantic
``@model_validator`` hooks and ``@invariant`` methods guard updates as well as creates;
``@invariant`` additionally raises domain-kind errors and composes across the hierarchy.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError, model_validator

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import Document, invariant
from forze.domain.models.document import is_invariant_check

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

    def test_model_validator_also_runs_on_update(self) -> None:
        # Document.update fully re-validates the merged state, so a raw
        # @model_validator now guards updates too (no model_copy bypass).
        class LooseAccount(Document):
            balance: int = 0

            @model_validator(mode="after")
            def _check(self) -> LooseAccount:
                if self.balance < 0:
                    raise ValueError("negative")
                return self

        account = LooseAccount(balance=10)

        with pytest.raises(ValidationError, match="negative"):
            account.update({"balance": -5})

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

    def test_only_a_class_with_invariants_validates_them_on_create(self) -> None:
        """A document without invariants carries no create-time validator, so validating one
        costs no call into it; one that declares or inherits an invariant still enforces it."""

        class Plain(Document):
            name: str = "x"

        class Savings(Account):
            rate: int = 0

        def checks(cls: type[Document]) -> int:
            return sum(
                is_invariant_check(d.func) for d in cls.__pydantic_decorators__.model_validators.values()
            )

        assert [checks(c) for c in (Document, Plain, Account, Savings)] == [0, 0, 1, 2]

        with pytest.raises(CoreException):
            Savings(balance=-1)

    def test_the_check_runs_after_every_validator_of_the_class_built(self) -> None:
        """A subclass's own after-validator runs before the check, as on the class declaring
        the invariant: one that breaks it is caught, one that restores it is not refused."""

        class Breaks(Account):
            @model_validator(mode="after")
            def _drain(self) -> Breaks:
                self.balance = -1
                return self

        class Restores(Account):
            @model_validator(mode="after")
            def _top_up(self) -> Restores:
                self.balance = max(self.balance, 0)
                return self

        with pytest.raises(CoreException):
            Breaks(balance=10)

        assert Restores(balance=-5).balance == 0

    def test_the_create_check_names_are_reserved(self) -> None:
        """A class's own attribute of that name would be replaced, and an invariant of that
        name would call the check that calls it."""

        with pytest.raises(CoreException) as ei:

            class Clash(Document):
                n: int = 0

                @invariant
                def _forze_invariants_on_create(self) -> None: ...

        assert ei.value.kind is ExceptionKind.CONFIGURATION
