"""A numbered aggregate's spec declares ``number_id`` unique by itself, and the stores keep it."""

from __future__ import annotations

from typing import Any, ClassVar, Final
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.document import (
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.guarantees import (
    GUARANTEE_UNSUPPORTED,
    SerializedBy,
    StorageGuarantees,
    UniqueTogether,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_kits.domain.number_id import NumberIdCreateCmdMixin, NumberIdMixin
from forze_mock import MockDepsModule
from forze_mock.execution import MockRouteConfig
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #

ONE_PER_NUMBER: Final = UniqueTogether(fields=("number_id",))


class Order(NumberIdMixin, Document):
    name: str = ""


class OrderRead(NumberIdMixin, ReadDocument):
    name: str = ""


class OrderCmd(NumberIdCreateCmdMixin, CreateDocumentCmd):
    name: str = ""


class Unnumbered(Order):
    implied_guarantees: ClassVar[StorageGuarantees] = ()


class Plain(Document):
    name: str = ""


class PlainRead(ReadDocument):
    name: str = ""


class PlainCmd(CreateDocumentCmd):
    name: str = ""


class Serialized(Plain):
    implied_guarantees: ClassVar[StorageGuarantees] = (SerializedBy(key=("name",)),)


def _orders(domain: type[Order] = Order, **kwargs: Any) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec(
        name="orders",
        read=OrderRead,
        write=DocumentWriteTypes(domain=domain, create_cmd=OrderCmd),
        **kwargs,
    )


# ....................... #


class TestTheSpec:
    def test_an_implied_guarantee_of_another_kind_is_added_once(self) -> None:
        serialized = SerializedBy(key=("name",))
        spec = DocumentSpec(
            name="plain",
            read=PlainRead,
            write=DocumentWriteTypes(domain=Serialized, create_cmd=PlainCmd),
        )

        assert spec.guarantees == (serialized,)
        assert attrs.evolve(spec, guarantees=(serialized,)).guarantees == (serialized,)

    def test_a_numbered_domain_declares_its_number_unique(self) -> None:
        assert tuple(_orders().guarantees) == (ONE_PER_NUMBER,)

    def test_a_declared_twin_is_kept_once(self) -> None:
        assert tuple(_orders(guarantees=(ONE_PER_NUMBER,)).guarantees) == (ONE_PER_NUMBER,)

    def test_a_declared_uniqueness_over_the_number_replaces_the_implied_one(self) -> None:
        # Narrowed to the live rows: the author's reading wins, rather than two indexes.
        live = UniqueTogether(fields=("number_id",), where={"$values": {"name": "live"}})

        assert tuple(_orders(guarantees=(live,)).guarantees) == (live,)

    def test_other_guarantees_are_kept_beside_it(self) -> None:
        by_name = UniqueTogether(fields=("name",))

        assert tuple(_orders(guarantees=(by_name,)).guarantees) == (by_name, ONE_PER_NUMBER)

    def test_an_evolved_spec_does_not_add_it_again(self) -> None:
        assert tuple(attrs.evolve(_orders(), name="other").guarantees) == (ONE_PER_NUMBER,)

    def test_a_model_can_opt_out(self) -> None:
        assert tuple(_orders(Unnumbered).guarantees) == ()

    def test_a_read_only_spec_asks_nothing(self) -> None:
        # No writes go through it, and its relation may be a view no index can sit on.
        assert tuple(DocumentSpec(name="orders", read=OrderRead).guarantees) == ()

    @pytest.mark.parametrize(
        "value", [ONE_PER_NUMBER, ("number_id",)], ids=["bare guarantee", "stray item"]
    )
    def test_a_malformed_attribute_is_refused_naming_the_model(self, value: object) -> None:
        class Malformed(Order):
            implied_guarantees: ClassVar[Any] = value

        with pytest.raises(CoreException) as caught:
            _orders(Malformed)

        assert caught.value.code == "implied_guarantees_invalid"
        assert "Malformed.implied_guarantees" in caught.value.summary

    def test_a_backend_without_uniqueness_names_the_opt_out(self) -> None:
        # The spec declares nothing, so "drop the guarantee" alone would point nowhere.
        spec = _orders()
        ctx = context_from_deps(Deps.plain({DocumentQueryDepKey: lambda ctx, spec: object()}))

        with pytest.raises(CoreException) as caught:
            ctx.doc.query(spec)

        assert caught.value.code == GUARANTEE_UNSUPPORTED
        assert "(implied by the domain model)" in caught.value.summary
        assert "`implied_guarantees = ()`" in caught.value.summary

    def test_a_spec_without_a_number_is_unaffected(self) -> None:
        spec = DocumentSpec(
            name="plain",
            read=PlainRead,
            write=DocumentWriteTypes(domain=Plain, create_cmd=PlainCmd),
        )

        assert tuple(spec.guarantees) == ()


# ....................... #


class TestTheMock:
    async def test_a_second_row_with_a_number_is_refused(self) -> None:
        command = context_from_modules(MockDepsModule()).doc.command(_orders())
        await command.create(OrderCmd(number_id=7))

        with pytest.raises(CoreException) as caught:
            await command.create(OrderCmd(number_id=7))

        assert caught.value.kind.value == "conflict"
        assert (caught.value.details or {})["fields"] == ["number_id"]

    async def test_each_tenant_has_its_own_numbers(self) -> None:
        # As the counter allocates on a tenant-aware route: every tenant starts at 1.
        routes = {"orders": MockRouteConfig(tenant_aware=True)}
        ctx = context_from_modules(MockDepsModule(routes=routes))
        command = ctx.doc.command(_orders())

        for tenant in (uuid4(), uuid4()):
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                await command.create(OrderCmd(number_id=1))

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            with pytest.raises(CoreException, match="Unique violation"):
                await command.create(OrderCmd(number_id=1))
