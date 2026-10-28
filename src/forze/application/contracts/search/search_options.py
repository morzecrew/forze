"""Normalize :class:`~forze.application.contracts.search.SearchOptions` for adapter shape."""

import math
from collections.abc import Mapping, Sequence
from typing import Any, Literal, NoReturn, cast

from forze.application.contracts.search.capabilities import (
    DEFAULT_SEARCH_CAPABILITIES,
    SearchCapabilities,
    validate_vector_supported,
)
from forze.application.contracts.search.specs import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.application.contracts.search.types import (
    MAX_QUERY_VECTOR_LENGTH,
    HighlightOptions,
    MultiSourceSearchOptions,
    SearchOptions,
    SearchResultSnapshotOptions,
)
from forze.base.exceptions import exc

from ..._logger import logger
from ..querying.capabilities import UNSUPPORTED_QUERY_FEATURE_CODE
from ..querying.expressions import QueryFilterExpression, refuse_unknown_keys
from .utils import normalize_search_queries

# ----------------------- #


def refuse_unknown_search_options(
    options: SearchOptions | None,
    snapshot: SearchResultSnapshotOptions | None = None,
) -> None:
    """Refuse a key no search options type defines, in *options*, their highlight options or
    *snapshot*, naming it.

    Every search adapter reads its options through one of the three functions below, which
    call this first, so an option passed straight to a port is held to the closed request
    types. An adapter reads only the keys it knows; a misspelt one ran the search without it.

    :raises exc.precondition: an unknown key.
    """

    refuse_unknown_keys(options, MultiSourceSearchOptions)
    refuse_unknown_keys(snapshot, SearchResultSnapshotOptions)

    if isinstance(highlight := (options or {}).get("highlight"), Mapping):
        refuse_unknown_keys(cast(Mapping[str, Any], highlight), HighlightOptions)


# ....................... #


def search_options_for_simple_adapter(
    options: SearchOptions | None,
    *,
    spec: SearchSpec[Any] | None = None,
    snapshot: SearchResultSnapshotOptions | None = None,
    capabilities: SearchCapabilities = DEFAULT_SEARCH_CAPABILITIES,
    backend: str = "search",
    vector_dimensions: int | None = None,
) -> SearchOptions:
    """Normalize single-index search options, for *spec* when given.

    Refuses a key no search options type defines (in *snapshot* too), and a ``query_vector``
    *capabilities* cannot serve (named after *backend*), or that is not *vector_dimensions*
    finite numbers; a served one is returned as a ``list[float]``, and an explicit ``None`` is
    dropped. Drops hub/federated member keys (warning when callers pass them), and fills
    ``search_count`` from *spec*'s :attr:`~SearchSpec.default_search_count` when the request
    leaves it out. Without *spec* no default is applied.
    """

    refuse_unknown_search_options(options, snapshot)
    opts = dict(options or {})

    if opts.get("query_vector") is None:
        opts.pop("query_vector", None)

    else:
        # A keyword adapter would rank by the blank query and ignore the vector.
        validate_vector_supported(capabilities, backend=backend)
        # Checked once, so the ranking and the snapshot fingerprint read the same list.
        opts["query_vector"] = _checked_query_vector(
            opts["query_vector"], dimensions=vector_dimensions
        )

    if spec is not None and spec.default_search_count is not None:
        opts.setdefault("search_count", spec.default_search_count)

    if "member_weights" in opts or "members" in opts or "member_filters" in opts:
        logger.warning(
            "search_options_member_keys_ignored_simple_index",
            message=(
                "SearchOptions member_weights, members and member_filters are ignored for "
                "simple (single-index) search"
            ),
        )

        opts.pop("member_weights", None)
        opts.pop("members", None)
        opts.pop("member_filters", None)

    return cast(SearchOptions, opts)


# ....................... #


def prepare_hub_search_options(
    hub_spec: HubSearchSpec[Any],
    options: SearchOptions | None,
    *,
    snapshot: SearchResultSnapshotOptions | None = None,
) -> tuple[SearchOptions, list[float]]:
    """Strip simple field-tuning and hub member keys; return leg options + per-member weights.

    Fills ``search_count`` from *hub_spec*'s :attr:`~HubSearchSpec.default_search_count` when
    the request leaves it out.
    """

    refuse_unknown_search_options(options, snapshot)
    _refuse_query_vector(options, scope="hub")
    opts = dict(options or {})

    if opts.get("member_filters"):
        # A hub filter applies to the hub rows, so a leg has no filter of its own to narrow.
        raise exc.precondition(
            "Search feature member_filters is not supported for hub search.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )

    if hub_spec.default_search_count is not None:
        opts.setdefault("search_count", hub_spec.default_search_count)

    if "weights" in opts or "fields" in opts:
        logger.warning(
            "search_options_hub_field_tuning_ignored",
            message=(
                "SearchOptions weights and fields are ignored for hub search "
                "(use per-leg SearchSpec and member_weights / members instead)"
            ),
        )

        opts.pop("weights", None)
        opts.pop("fields", None)

    weights_list = list(multi_source_member_weights(hub_spec, cast(SearchOptions, opts)).values())

    leg_opts = dict(opts)
    leg_opts.pop("member_weights", None)
    leg_opts.pop("members", None)
    leg_opts.pop("member_filters", None)
    leg_opts.pop("fusion", None)

    return cast(SearchOptions, leg_opts), weights_list


# ....................... #


def prepare_federated_search_options(
    federated_spec: FederatedSearchSpec[Any],
    options: SearchOptions | None,
    *,
    snapshot: SearchResultSnapshotOptions | None = None,
) -> tuple[SearchOptions, list[float]]:
    """Strip field-level tuning and federation member keys; return leg options + weights."""

    refuse_unknown_search_options(options, snapshot)
    _refuse_query_vector(options, scope="federated")
    opts = dict(options or {})

    if "weights" in opts or "fields" in opts:
        logger.warning(
            "search_options_federated_field_tuning_ignored",
            message=(
                "SearchOptions weights and fields are ignored for federated search "
                "(use per-member SearchSpec and member_weights / members instead)"
            ),
        )

        opts.pop("weights", None)
        opts.pop("fields", None)

    weights_list = list(
        multi_source_member_weights(federated_spec, cast(SearchOptions, opts)).values()
    )

    leg_opts = dict(opts)
    leg_opts.pop("member_weights", None)
    leg_opts.pop("members", None)
    leg_opts.pop("member_filters", None)
    leg_opts.pop("fusion", None)

    return cast(SearchOptions, leg_opts), weights_list


# ....................... #


def multi_source_member_weights(
    spec: HubSearchSpec[Any] | FederatedSearchSpec[Any],
    options: SearchOptions | None,
) -> dict[str, float]:
    """Each member's weight for *options*, by member name in spec order, as the hub and
    federated adapters read it. A member weighted ``0`` is not searched.

    :raises exc.validation: a ``member_weights`` entry outside ``0.0``-``1.0``.
    """

    names = [m.name for m in spec.members]

    if isinstance(spec, HubSearchSpec):
        base = _member_weights_base(names, spec.default_member_weights)
        weights = _apply_member_weight_overrides(names, base, dict(options or {}), scope="hub")

    else:
        base = _member_weights_base(names, None)
        weights = _apply_member_weight_overrides(
            names, base, dict(options or {}), scope="federated"
        )

    return dict(zip(names, weights, strict=True))


# ....................... #


def federated_leg_filters(
    federated_spec: FederatedSearchSpec[Any],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    options: SearchOptions | None,
) -> dict[str, QueryFilterExpression | None]:  # type: ignore[valid-type]
    """The filters each member's leg runs, by member name: *filters* and the request's
    ``member_filters`` entry for that member, conjoined.

    :raises exc.validation: ``member_filters`` names a member the spec does not have.
    """

    member_filters = cast(MultiSourceSearchOptions, options or {}).get("member_filters") or {}
    names = [m.name for m in federated_spec.members]

    if unknown := sorted(set(member_filters) - set(names)):
        raise exc.validation(
            f"member_filters names no member of federated search "
            f"{federated_spec.name!r}: {unknown}.",
        )

    legs: dict[str, QueryFilterExpression | None] = {}  # type: ignore[valid-type]

    for name in names:
        extra = member_filters.get(name)

        if extra is None or filters is None:
            legs[name] = filters if extra is None else extra

        else:
            legs[name] = {"$and": [filters, extra]}

    return legs


# ....................... #


def requested_query_vector(
    query: str | Sequence[str],
    options: SearchOptions | None,
) -> Sequence[float] | None:
    """The request's ``query_vector``, as :func:`search_options_for_simple_adapter` checked it,
    or ``None``.

    :raises exc.precondition: a vector alongside a non-blank *query*, which it would leave
        unranked.
    """

    vector = (options or {}).get("query_vector")

    if vector is not None and normalize_search_queries(query):
        raise exc.precondition(
            "Pass either a query or a query_vector, not both: the vector replaces the "
            "query's embedding.",
            code="search_query_vector_with_query",
        )

    return vector


def refuse_text_query_without_embedder() -> NoReturn:
    """Refuse a text query on a vector search configured without an embedder.

    :raises exc.precondition: always, with code ``search_query_vector_required`` — the same
        kind as the other refusals of a request the search cannot serve.
    """

    raise exc.precondition(
        "This search ranks by query_vector only: it has no embeddings provider to embed a "
        "text query.",
        code="search_query_vector_required",
    )


def _checked_query_vector(raw: object, *, dimensions: int | None) -> list[float]:
    """*raw* as a list of finite floats, its length checked before any value is converted."""

    if isinstance(raw, (str, bytes)) or not isinstance(raw, Sequence):
        raise exc.validation("query_vector must be a list or tuple of numbers.")

    length = len(raw)

    if dimensions is not None and length != dimensions:
        raise exc.validation(f"query_vector must have {dimensions} values, got {length}.")

    if length > MAX_QUERY_VECTOR_LENGTH:
        raise exc.validation(
            f"query_vector must have at most {MAX_QUERY_VECTOR_LENGTH} values, got {length}."
        )

    try:
        vector = [float(x) for x in raw]

    except (TypeError, ValueError, OverflowError):
        raise exc.validation("query_vector must be a list or tuple of finite numbers.") from None

    if not vector or not all(math.isfinite(x) for x in vector):
        raise exc.validation("query_vector must be a list or tuple of finite numbers.")

    return vector


def _refuse_query_vector(options: SearchOptions | None, *, scope: str) -> None:
    # ``None`` reads as no vector, as a single-index search reads it.
    if (options or {}).get("query_vector") is not None:
        raise exc.precondition(
            f"Search feature query_vector is not supported for {scope} search.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )


# ....................... #


def _member_weights_base(
    names: list[str],
    default_weights: Mapping[str, float] | None,
) -> dict[str, float]:
    if default_weights is not None:
        return {n: float(default_weights[n]) for n in names}

    return dict.fromkeys(names, 1.0)


# ....................... #


def _apply_member_weight_overrides(
    names: list[str],
    base: dict[str, float],
    opts: dict[str, Any],
    *,
    scope: Literal["hub", "federated"],
) -> list[float]:
    label = scope

    if opts.get("member_weights"):
        mw = opts["member_weights"]
        for k, v in mw.items():
            if k not in base:
                logger.warning(
                    f"search_options_{label}_unknown_member_weight",
                    member=k,
                    message=f"Ignoring member_weights entry for unknown {scope} member",
                )
                continue

            w = float(v)

            if w < 0 or w > 1:
                raise exc.validation(
                    f"Member weight for {scope} member {k!r} must be between 0.0 and 1.0.",
                )

            base[k] = w

    elif opts.get("members") is not None:
        listed = set(opts["members"])

        for n in names:
            base[n] = 1.0 if n in listed else 0.0

    return [base[n] for n in names]
