"""Federated multi-index Meilisearch search (native federation or RRF merge)."""

import asyncio
from typing import (
    Any,
    Awaitable,
    Callable,
    Final,
    Literal,
    NoReturn,
    Sequence,
    TypeVar,
    cast,
    final,
    overload,
)

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCountlessPage,
    SearchPage,
    SearchSnapshotHandle,
    search_page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    SearchResultSnapshotSpec,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
    prepare_federated_search_options,
    reject_federated_facets,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    build_federated_highlight_index,
    execute_federated_thin_offset,
    federated_highlights_for_hits,
    federated_snapshot_rehydrator,
    federated_thin_eligible,
    federated_thin_format,
)
from forze.base.exceptions import exc
from forze.base.serialization import default_model_codec
from forze_meilisearch.adapters.search._port import MeilisearchSearchPortMixin
from forze_meilisearch.adapters.search._search_params import (
    attributes_to_search_on,
    build_search_query_string,
    build_sort,
    render_user_sorts,
)
from forze_meilisearch.adapters.search._simple_base import (
    MeilisearchSimpleSearchAdapter,
)
from forze_meilisearch.execution.deps.configs import MeilisearchFederatedMerge
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #

_DEFAULT_RRF_K: Final[int] = 60
_DEFAULT_PER_LEG_LIMIT: Final[int] = 5000

# ....................... #

T = TypeVar("T", bound=BaseModel)

# ....................... #


def _hit_index_uid(hit: dict[str, Any]) -> str | None:
    fed = hit.get("_federation")

    if isinstance(fed, dict):
        raw = fed.get("indexUid") or fed.get("index_uid")  # type: ignore[arg-type]
        return str(raw) if raw is not None else None  # type: ignore[arg-type]

    return None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchFederatedSearchAdapter[M: BaseModel](
    MeilisearchSearchPortMixin[FederatedSearchReadModel[M]],
    SearchQueryPort[FederatedSearchReadModel[M]],
):
    """Search multiple Meilisearch indexes with federation or weighted RRF."""

    federated_spec: FederatedSearchSpec[M]
    legs: Sequence[tuple[str, MeilisearchSimpleSearchAdapter[M]]]
    client: MeilisearchClientPort
    merge: MeilisearchFederatedMerge = "federation"
    rrf_k: int = _DEFAULT_RRF_K
    rrf_per_leg_limit: int = _DEFAULT_PER_LEG_LIMIT
    result_snapshot: SearchResultSnapshot | None = None

    spec: FederatedSearchSpec[M] = attrs.field(
        default=attrs.Factory(lambda self: self.federated_spec, takes_self=True),
        init=False,
    )
    model_type: type[FederatedSearchReadModel[M]] = attrs.field(
        default=cast("type[FederatedSearchReadModel[M]]", FederatedSearchReadModel),
        init=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.legs) != len(self.federated_spec.members):
            raise exc.internal(
                "Federated adapter legs must match FederatedSearchSpec.members length.",
            )

        for (leg_member, _), member in zip(
            self.legs, self.federated_spec.members, strict=True
        ):
            if leg_member != member.name:
                raise exc.internal(
                    f"Federated leg member {leg_member!r} does not match spec name {member.name!r}.",
                )

    # ....................... #

    async def _index_to_member(self) -> dict[str, str]:
        return {
            await adapter._resolved_index_uid(): name  # pyright: ignore[reportPrivateUsage]
            for name, adapter in self.legs
        }

    # ....................... #

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: None = None,
    ) -> SearchCountlessPage[FederatedSearchReadModel[M]]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: None = None,
    ) -> SearchPage[FederatedSearchReadModel[M]]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: type[T],
        return_fields: None = None,
    ) -> SearchCountlessPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: type[T],
        return_fields: None = None,
    ) -> SearchPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool,
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> NoReturn: ...

    async def _offset_search_impl(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        if return_fields is not None:
            raise exc.precondition(
                "Field projection is not supported for federated Meilisearch search.",
            )

        reject_federated_facets(options)

        if self.merge == "federation":
            return await self._search_federation(
                query,
                filters,
                pagination,
                sorts,
                options=options,
                snapshot=snapshot,
                return_count=return_count,
                return_type=return_type,
            )

        return await self._search_rrf(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
        )

    # ....................... #

    async def _search_federation(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        *,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
    ) -> Any:
        from meilisearch_python_sdk.models.search import FederationOptions, SearchParams

        if (options or {}).get("highlight"):
            raise exc.precondition(
                "Highlighting is not available for Meilisearch native federation "
                "(it has no per-hit _formatted); use merge='rrf' to get highlights.",
                code="query_feature_unsupported",
            )

        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )

        fp_computed = SearchResultSnapshot.federated_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.federated_spec.name,
            extras={"merge": "federation"},
        )

        rs_spec: SearchResultSnapshotSpec | None = self.federated_spec.snapshot

        if (
            self.result_snapshot is not None
            and rs_spec is not None
            and snapshot is not None
            and "id" in snapshot
        ):
            maybe_page = (
                await self.result_snapshot.read_federated_snapshot_page_if_requested(
                    federated_spec=self.federated_spec,
                    rs_spec=rs_spec,
                    snapshot=snapshot,
                    fp_computed=fp_computed,
                    pagination=dict(pagination or {}),
                    return_type=return_type,
                    return_count=return_count,
                )
            )

            if maybe_page is not None:
                return maybe_page

        terms = tuple(normalize_search_queries(query))
        combine = effective_phrase_combine(leg_opts)
        q = build_search_query_string(terms, combine=combine)
        index_to_member = await self._index_to_member()

        queries: list[SearchParams] = []

        for i, (name, adapter) in enumerate(self.legs):
            weight = member_weights[i]

            if weight <= 0.0:
                continue

            member_spec = next(m for m in self.federated_spec.members if m.name == name)
            filter_str = adapter.build_filter(filters)
            search_attrs = attributes_to_search_on(
                cast(SearchSpec[M], member_spec),
                leg_opts,
                adapter.field_map,
            )
            sort_list = build_sort(render_user_sorts(sorts, adapter.field_map))

            params_kwargs: dict[str, Any] = {
                "index_uid": await adapter._resolved_index_uid(),  # pyright: ignore[reportPrivateUsage]
                "query": q,
            }

            if filter_str is not None:
                params_kwargs["filter"] = filter_str

            if search_attrs is not None:
                params_kwargs["attributes_to_search_on"] = search_attrs

            if sort_list is not None:
                params_kwargs["sort"] = sort_list

            queries.append(
                SearchParams(
                    **params_kwargs,
                    federation_options=FederationOptions(weight=float(weight)),
                )
            )

        if not queries:
            empty: list[FederatedSearchReadModel[M]] = []

            if return_count:
                return search_page_from_limit_offset(empty, pagination or {}, total=0)

            return search_page_from_limit_offset(empty, pagination or {}, total=None)

        offset = int((pagination or {}).get("offset") or 0)
        limit = (pagination or {}).get("limit")
        federation: dict[str, Any] = {"offset": offset}

        if limit is not None:
            federation["limit"] = int(limit)

        result = await self.client.multi_search(queries, federation=federation)
        hits_raw = list(getattr(result, "hits", []) or [])
        total = int(
            getattr(result, "estimated_total_hits", None)
            or getattr(result, "total_hits", None)
            or len(hits_raw)
        )

        out_hits: list[FederatedSearchReadModel[M]] = []

        for raw in hits_raw:
            hit = dict(raw)
            idx_uid = _hit_index_uid(hit)
            member = index_to_member.get(idx_uid or "", "")

            if not member:
                for name, adapter in self.legs:
                    resolved = (
                        await adapter._resolved_index_uid()  # pyright: ignore[reportPrivateUsage]
                    )

                    if resolved == idx_uid:
                        member = name
                        break

            logical = next(a for n, a in self.legs if n == member)
            row = logical.from_hit(hit)
            model = logical.spec.model_type.model_validate(row)
            out_hits.append(FederatedSearchReadModel(hit=model, member=member))

        return await self._finalize_page(
            out_hits,
            pagination,
            total=total if return_count else None,
            return_count=return_count,
            return_type=return_type,
            snapshot=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_computed,
            merged_for_snap=[(h, 0.0) for h in out_hits],
        )

    # ....................... #

    async def _run_legs(
        self,
        makers: Sequence[Callable[[], Awaitable[Any]]],
    ) -> list[Any]:
        """Run leg thunks concurrently (Meilisearch has no shared-connection limit)."""

        return list(await asyncio.gather(*(maker() for maker in makers)))

    # ....................... #

    async def _search_rrf(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        *,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
    ) -> Any:
        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )

        # Spec-level (RRF mode only): thin specs store/replay tiny ``(member, id)``
        # snapshot keys; the marker keeps a thin snapshot from being read as a full one.
        effective_thin = federated_thin_format(
            self.federated_spec.members, thin_merge=self.federated_spec.thin_merge
        )

        fp_extras: dict[str, object] = {"merge": "rrf"}

        if effective_thin:
            fp_extras["thin"] = True

        fp_computed = SearchResultSnapshot.federated_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.federated_spec.name,
            rrf_k=int(self.rrf_k),
            extras=fp_extras,
        )

        rs_spec: SearchResultSnapshotSpec | None = self.federated_spec.snapshot
        offset = int((pagination or {}).get("offset") or 0)
        limit = (pagination or {}).get("limit")

        _hl = (options or {}).get("highlight")
        wants_highlights = _hl is not None and _hl is not False

        if (
            self.result_snapshot is not None
            and rs_spec is not None
            and snapshot is not None
            and "id" in snapshot
        ):
            maybe_page: Any = None

            if effective_thin:
                # A thin snapshot replays by re-fetching hits by id only, so it
                # carries no match highlights. A highlights request must skip the
                # replay and fall through to the live merge below, which builds them.
                if not wants_highlights:
                    maybe_page = await self.result_snapshot.read_federated_thin_snapshot_page_if_requested(
                        rs_spec=rs_spec,
                        snapshot=snapshot,
                        fp_computed=fp_computed,
                        pagination=dict(pagination or {}),
                        return_type=return_type,
                        return_count=return_count,
                        rehydrate=federated_snapshot_rehydrator(
                            ports={name: port for name, port in self.legs},
                            leg_opts=leg_opts,
                            run_legs=self._run_legs,
                        ),
                    )

            else:
                maybe_page = await self.result_snapshot.read_federated_snapshot_page_if_requested(
                    federated_spec=self.federated_spec,
                    rs_spec=rs_spec,
                    snapshot=snapshot,
                    fp_computed=fp_computed,
                    pagination=dict(pagination or {}),
                    return_type=return_type,
                    return_count=return_count,
                )

            if maybe_page is not None:
                return maybe_page

        active = [
            (name, port, member_weights[i])
            for i, (name, port) in enumerate(self.legs)
            if member_weights[i] > 0.0
        ]

        if not active:
            empty: list[FederatedSearchReadModel[M]] = []

            if return_count:
                return search_page_from_limit_offset(empty, pagination or {}, total=0)

            return search_page_from_limit_offset(empty, pagination or {}, total=None)

        leg_cap = max(1, int(self.rrf_per_leg_limit))
        leg_page: PaginationExpression = {"limit": leg_cap}

        snapshot_write = (
            self.result_snapshot is not None
            and rs_spec is not None
            and self.result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        )

        if federated_thin_eligible(
            members=self.federated_spec.members,
            thin_merge=self.federated_spec.thin_merge,
            wants_highlights=wants_highlights,
            sorts=sorts,
        ):
            return await execute_federated_thin_offset(
                legs=active,
                query=query,
                filters=filters,
                pagination=pagination,
                sorts=sorts,
                leg_opts=leg_opts,
                rrf_k=int(self.rrf_k),
                per_leg_limit=leg_cap,
                return_count=return_count,
                return_type=return_type,
                run_legs=self._run_legs,
                result_snapshot=self.result_snapshot,
                rs_spec=rs_spec,
                snapshot=snapshot,
                fp_computed=fp_computed,
                write_snapshot=snapshot_write,
            )

        async def _run_leg(
            name: str,
            port: MeilisearchSimpleSearchAdapter[M],
            weight: float,
        ) -> tuple[str, Any, float]:
            page = await port.search(
                query,
                filters,
                leg_page,
                None,
                options=leg_opts,
            )
            return name, page, weight

        leg_results = await asyncio.gather(
            *(_run_leg(n, p, w) for n, p, w in active),
        )

        hl_index = build_federated_highlight_index(
            [(name, page) for name, page, _w in leg_results]
        )
        merged = SearchResultSnapshot.weighted_rrf_merge_rows(
            leg_rows=[(name, page.hits, w) for name, page, w in leg_results],
            k=int(self.rrf_k),
        )

        SearchResultSnapshot.order_federated_secondary_sorts(
            merged,
            sorts,
            value_of=lambda item, field: SearchResultSnapshot.federated_merged_hit_field(
                item, field=field
            ),
            score_of=lambda item: -item[1],
        )

        window_models = [it[0] for it in merged[offset:]]

        if limit is not None:
            window_models = window_models[: int(limit)]

        return await self._finalize_page(
            window_models,
            pagination,
            total=len(merged) if return_count else None,
            return_count=return_count,
            return_type=return_type,
            snapshot=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_computed,
            merged_for_snap=merged,
            highlights=federated_highlights_for_hits(window_models, hl_index),
            write_snapshot=not effective_thin,  # thin specs only write the thin format
        )

    # ....................... #

    async def _finalize_page(
        self,
        hits: list[FederatedSearchReadModel[M]],
        pagination: PaginationExpression | None,
        *,
        total: int | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        snapshot: SearchResultSnapshotOptions | None,
        rs_spec: SearchResultSnapshotSpec | None,
        fp_computed: str,
        merged_for_snap: list[
            tuple[FederatedSearchReadModel[M], float] | tuple[Any, float]
        ],
        highlights: list[Any] | None = None,
        write_snapshot: bool = True,
    ) -> Any:
        handle_out: SearchSnapshotHandle | None = None

        if (
            write_snapshot
            and self.result_snapshot is not None
            and rs_spec is not None
            and self.result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        ):
            handle_out = await self.result_snapshot.put_ordered_snapshot_keys(
                (
                    SearchResultSnapshot.federated_record_key_string(
                        item[0].member,
                        item[0].hit,
                    )
                    for item in merged_for_snap
                ),
                snap_opt=snapshot,
                rs_spec=rs_spec,
                fp_computed=fp_computed,
                pool_len_before_cap=total or len(merged_for_snap),
            )

        def _attach(result: Any) -> Any:
            if highlights is None:
                return result
            return attrs.evolve(result, highlights=highlights)

        if return_type is not None:
            rows = [
                {"hit": h.hit.model_dump(mode="json"), "member": h.member} for h in hits
            ]
            v = default_model_codec(return_type).decode_mapping_many(rows)

            if return_count:
                return _attach(
                    search_page_from_limit_offset(
                        v, pagination, total=total, snapshot=handle_out
                    )
                )

            return _attach(
                search_page_from_limit_offset(v, pagination, total=None, snapshot=handle_out)
            )

        if return_count:
            return _attach(
                search_page_from_limit_offset(
                    hits, pagination, total=total, snapshot=handle_out
                )
            )

        return _attach(
            search_page_from_limit_offset(hits, pagination, total=None, snapshot=handle_out)
        )

    # ....................... #

    def _raise_federated_cursor_not_supported(self) -> NoReturn:
        raise exc.precondition(
            "search_cursor is not implemented for Meilisearch federated search; use "
            "search or search_page with limit/offset.",
            code="query_feature_unsupported",
        )

    # ....................... #

    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        del query, filters, cursor, sorts, options, return_type, return_fields
        self._raise_federated_cursor_not_supported()
