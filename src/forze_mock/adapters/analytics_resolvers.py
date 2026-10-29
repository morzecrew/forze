"""Programmable answers for analytics queries, computed from the mock's current documents.

Seeded query hits (``MockState.analytics_query_hits``) are numbers written down before the run.
They never move, so a handler that reads a query to decide what to do sees the same answer after
every write, and a simulation never reaches the branch the real numbers would take it down.

A registered resolver is the query's second implementation: a function of the stored document
rows and the call's params, standing where the warehouse SQL stands in production. It is fixture
logic, and nothing keeps it in agreement with that SQL except a differential test
(``forze_dst.conformance.compare_analytics_query``). Keep it deterministic — a function of the
rows and the params, not of a clock or a counter — or a simulation stops being replayable.
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterable, Mapping
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mock.adapters._mvcc import current_mvcc_tx
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockAnalyticsView:
    """Read-only access to stored document rows, as the calling transaction sees them.

    Inside a transaction the rows come through that transaction's overlay, the same way a
    document read does: its own uncommitted writes are visible, a concurrent transaction's are
    not. Outside one they are the committed rows. Each call returns deep copies, so a resolver
    cannot change the store by mutating what it was handed.
    """

    state: MockState

    scope_for: Callable[[str], tuple[str, UUID | None]] = attrs.field(
        default=lambda name: (name, None),
    )
    """Map a document spec name to the namespace its rows live in, for the calling tenant, and
    the tenant a document read narrows those rows to (``None`` on a route that is not
    tenant-aware).

    The rows are narrowed to that tenant whatever the analytics route's own tenancy, so a
    resolver never sees a row a document read would hide: one another tenant owns, or one with
    no owner, left in the bound tenant's partition."""

    tenant_id: UUID | None = None
    """When set, only rows whose ``tenant_id`` names this tenant are returned.

    Set by a tenant-aware analytics route, on top of the document route's partition: a
    document route that is not partitioned holds every tenant's rows in one namespace, and the
    route's tenancy must not depend on how the document side happens to be wired. A row with no
    ``tenant_id`` is left out, fail closed."""

    # ....................... #

    def rows(self, spec: DocumentSpec[Any, Any, Any, Any] | str) -> list[JsonDict]:
        """Every stored row of *spec* (a document spec or its name), as stored.

        Rows are in storage form, the shape a SQL query over the table would see: no read-model
        decoding, no derived fields, soft-deleted rows included. A field the document spec
        seals arrives as its ciphertext, exactly as SQL sees the column. The analytics read path
        cannot open it, even when the analytics spec declares the field encrypted: the document
        codec binds the ciphertext to the document spec's name, the analytics codec to
        ``forze.field``, so decryption fails authentication.
        """

        namespace, document_tenant = self.scope_for(
            spec if isinstance(spec, str) else str(spec.name)
        )

        with self.state.lock:
            live = self.state.documents.get(namespace, {})
            mvcc = current_mvcc_tx()
            store = live if mvcc is None else mvcc.view(namespace, live)

            return [
                copy.deepcopy(row)
                for row in store.values()
                if _owned_by(row, self.tenant_id) and _owned_by(row, document_tenant)
            ]


# ....................... #


def _owned_by(row: JsonDict, tenant_id: UUID | None) -> bool:
    """Whether *row* names *tenant_id* as its owner; any row when *tenant_id* is ``None``."""

    if tenant_id is None:
        return True

    owner = row.get("tenant_id")

    return owner is not None and str(owner) == str(tenant_id)


# ....................... #

MockAnalyticsResolver = Callable[[MockAnalyticsView, BaseModel], Iterable[Mapping[str, Any]]]
"""Compute one analytics query's rows from the mock's current documents.

Receives the :class:`MockAnalyticsView` and the call's validated params model, and returns the
rows the query would have produced, as mappings. They then take the same path seeded hits take:
``max_rows``, pagination and decoding through the spec's read model.
"""


@final
@attrs.define(slots=True)
class MockAnalyticsRegistry:
    """Programmable analytics resolvers, keyed by analytics spec name and query key.

    A query with no resolver keeps answering from ``MockState.analytics_query_hits``.
    """

    _resolvers: dict[tuple[str, str], MockAnalyticsResolver] = attrs.field(factory=dict)

    def on(
        self,
        spec: AnalyticsSpec[Any, Any],
        query_key: str,
        resolver: MockAnalyticsResolver,
    ) -> MockAnalyticsRegistry:
        """Register *resolver* for *query_key* on *spec*. Returns self (chainable).

        :raises exc.configuration: when *spec* is not an analytics spec, declares no query
            named *query_key*, or already has a resolver for it. A resolver nothing can call is
            a typo, and a second one silently replaces the first; found here either costs one
            line, found at run time it costs a simulation that answered from the wrong rows.
        """

        if not isinstance(spec, AnalyticsSpec):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.configuration(
                f"MockAnalyticsRegistry.on() takes an AnalyticsSpec, got {type(spec).__name__}.",
                code="mock.analytics.not_an_analytics_spec",
            )

        if query_key not in spec.queries:
            raise exc.configuration(
                f"Analytics spec {str(spec.name)!r} declares no query {query_key!r}; declared: "
                f"{sorted(str(key) for key in spec.queries)}.",
                code="mock.analytics.unknown_query",
            )

        key = (str(spec.name), query_key)

        if key in self._resolvers:
            raise exc.configuration(
                f"Analytics query {str(spec.name)!r}/{query_key!r} already has a resolver; "
                "register each query once.",
                code="mock.analytics.duplicate_resolver",
            )

        self._resolvers[key] = resolver
        return self

    def resolver_for(self, spec: str, query_key: str) -> MockAnalyticsResolver | None:
        return self._resolvers.get((spec, query_key))
