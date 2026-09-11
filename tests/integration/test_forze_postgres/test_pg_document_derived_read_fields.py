"""Integration test: the mock's join answers what a Postgres view answers.

A derived read field is declared, not implemented — on a real backend the view already
produces the column, and the framework changes nothing at runtime. The mock has no view
and performs the join itself, which is the only thing that makes a view-backed aggregate
reachable in memory.

That asymmetry is the design's one real risk: two implementations of one read, which can
disagree. This test is what bounds it. It builds the actual shape — two tables and a view
joining them — reads it through the Postgres gateway, reads the same rows through the mock
with the same spec, and asserts the two agree. Without it, the claim that a
primary-key-only join stays comparable to a view is argued and not measured.
"""

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.domain.models import ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_mock.adapters._derived import (  # pyright: ignore[reportPrivateUsage]
    ResolvedDerivedRead,
)
from forze_postgres.execution.deps.utils import read_gw
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import gateway_context

# ----------------------- #


class _OrderRead(ReadDocument):
    supplier_id: UUID
    supplier: str
    """Required, produced by the view's join. Leniency cannot serve this shape."""


ORDERS = DocumentSpec(
    name="orders",
    read=_OrderRead,
    derived_read_fields={
        "supplier": DerivedReadField(source="suppliers", via="supplier_id", field="name"),
    },
)


async def _build_view(pg_client: PostgresClient) -> tuple[str, str, UUID, UUID, str]:
    """Two tables and a view over them — the shape the mock has to stand in for."""

    suffix = uuid4().hex[:8]
    suppliers = f"pg_derived_suppliers_{suffix}"
    orders = f"pg_derived_orders_{suffix}"
    view = f"pg_derived_orders_view_{suffix}"

    for table, extra in ((suppliers, "name text NOT NULL"), (orders, "supplier_id uuid NOT NULL")):
        await pg_client.execute(
            f"""
            CREATE TABLE public.{table} (
                id uuid PRIMARY KEY,
                rev integer NOT NULL,
                created_at timestamptz NOT NULL,
                last_update_at timestamptz NOT NULL,
                {extra}
            );
            """
        )

    await pg_client.execute(
        f"""
        CREATE VIEW public.{view} AS
        SELECT o.id, o.rev, o.created_at, o.last_update_at, o.supplier_id, s.name AS supplier
        FROM public.{orders} o
        JOIN public.{suppliers} s ON s.id = o.supplier_id;
        """
    )

    supplier_id, order_id, now = uuid4(), uuid4(), utcnow()

    await pg_client.execute(
        f"INSERT INTO public.{suppliers} (id, rev, created_at, last_update_at, name) "
        "VALUES (%s, %s, %s, %s, %s)",
        [supplier_id, 1, now, now, "Acme"],
    )
    await pg_client.execute(
        f"INSERT INTO public.{orders} (id, rev, created_at, last_update_at, supplier_id) "
        "VALUES (%s, %s, %s, %s, %s)",
        [order_id, 1, now, now, supplier_id],
    )

    return view, suppliers, order_id, supplier_id, now.isoformat()


def _mock(state: MockState) -> MockDocumentAdapter:
    return MockDocumentAdapter(
        spec=ORDERS,
        state=state,
        namespace="orders",
        read_model=_OrderRead,
        derived={
            "supplier": ResolvedDerivedRead(
                namespace="suppliers", via="supplier_id", field="name"
            )
        },
    )


# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_the_mocks_join_matches_the_views(pg_client: PostgresClient) -> None:
    view, _suppliers, order_id, supplier_id, stamp = await _build_view(pg_client)

    # Postgres: the view produces `supplier`, so nothing about the declaration is
    # threaded into the gateway — this is the "real backends are unchanged" claim.
    real = read_gw(
        gateway_context(pg_client),
        read_type=_OrderRead,
        read_relation=("public", view),
        tenant_aware=False,
    )
    from_pg = await real.get(order_id)

    # The mock: the same spec over rows carrying no `supplier` at all.
    state = MockState()
    state.documents["suppliers"] = {
        supplier_id: {
            "id": str(supplier_id),
            "rev": 1,
            "created_at": stamp,
            "last_update_at": stamp,
            "name": "Acme",
        }
    }
    state.documents["orders"] = {
        order_id: {
            "id": str(order_id),
            "rev": 1,
            "created_at": stamp,
            "last_update_at": stamp,
            "supplier_id": str(supplier_id),
        }
    }
    from_mock = await _mock(state).get(order_id)

    assert from_pg.supplier == from_mock.supplier == "Acme"
    assert from_pg.supplier_id == from_mock.supplier_id == supplier_id
    assert from_pg.id == from_mock.id == order_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_missing_source_diverges_and_both_refuse_to_invent(
    pg_client: PostgresClient,
) -> None:
    """The one divergence worth naming, measured rather than assumed.

    An inner-join view **omits** an order whose supplier is gone; the mock **refuses**
    the read. Neither invents a value, and they fail differently — so the agreement in
    the test above is about resolvable joins, not about every state of the data. Written
    down here so nobody reads that agreement as total.
    """

    view, suppliers, order_id, supplier_id, stamp = await _build_view(pg_client)

    real = read_gw(
        gateway_context(pg_client),
        read_type=_OrderRead,
        read_relation=("public", view),
        tenant_aware=False,
    )
    assert (await real.get(order_id)).supplier == "Acme"  # the baseline

    await pg_client.execute(
        f"DELETE FROM public.{suppliers} WHERE id = %s", [supplier_id]
    )

    # Postgres: the inner join drops the row, so the order is simply not there.
    with pytest.raises(CoreException):
        await real.get(order_id)

    # The mock: the order row still exists, and its join key resolves to nothing.
    state = MockState()
    state.documents["suppliers"] = {}
    state.documents["orders"] = {
        order_id: {
            "id": str(order_id),
            "rev": 1,
            "created_at": stamp,
            "last_update_at": stamp,
            "supplier_id": str(supplier_id),
        }
    }

    with pytest.raises(CoreException, match="holds no such row"):
        await _mock(state).get(order_id)


# ----------------------- #


class _MarkedOrderRead(ReadDocument):
    supplier_id: UUID
    supplier: str
    """Marked derived, no join declared. Postgres still reads it off the view."""


MARKED = DocumentSpec(
    name="orders",
    read=_MarkedOrderRead,
    derived_read_fields={"supplier": None},
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_marked_field_is_read_from_the_view_unchanged(
    pg_client: PostgresClient,
) -> None:
    """The branch's headline claim about real backends, for the marker specifically.

    "Real backends are unaffected at runtime" was proven for the *resolved* form by the
    differential above. A marked field declares even less — no source, no key — so the
    claim is easy to believe and was equally untested. Here the view produces `supplier`,
    the spec marks it, and the gateway reads it as it always did: nothing about the
    declaration reaches Postgres.
    """

    view, _suppliers, order_id, supplier_id, _stamp = await _build_view(pg_client)

    read = read_gw(
        gateway_context(pg_client),
        read_type=_MarkedOrderRead,
        read_relation=("public", view),
        tenant_aware=False,
    )
    fetched = await read.get(order_id)

    assert fetched.supplier == "Acme"
    assert fetched.supplier_id == supplier_id

    # And the marking is what keeps it out of the query axes, on both backends alike.
    assert "supplier" not in MARKED.filterable_fields()
    assert "supplier_id" in MARKED.filterable_fields()
