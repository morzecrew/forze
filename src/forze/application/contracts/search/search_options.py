"""Normalize :class:`~forze.application.contracts.search.SearchOptions` for adapter shape."""

from collections.abc import Mapping
from typing import Any, Literal, cast

from forze.application.contracts.search.specs import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.application.contracts.search.types import (
    HighlightOptions,
    MultiSourceSearchOptions,
    SearchOptions,
    SearchResultSnapshotOptions,
)
from forze.base.exceptions import exc

from ..._logger import logger
from ..querying.expressions import refuse_unknown_keys

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
) -> SearchOptions:
    """Normalize single-index search options, for *spec* when given.

    Refuses a key no search options type defines (in *snapshot* too), drops hub/federated
    member keys (warning when callers pass them), and fills ``search_count`` from *spec*'s
    :attr:`~SearchSpec.default_search_count` when the request leaves it out. Without *spec* no
    default is applied.
    """

    refuse_unknown_search_options(options, snapshot)
    opts = dict(options or {})

    if spec is not None and spec.default_search_count is not None:
        opts.setdefault("search_count", spec.default_search_count)

    if "member_weights" in opts or "members" in opts:
        logger.warning(
            "search_options_member_keys_ignored_simple_index",
            message=(
                "SearchOptions member_weights and members are ignored for simple "
                "(single-index) search"
            ),
        )

        opts.pop("member_weights", None)
        opts.pop("members", None)

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
    opts = dict(options or {})

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

    names = [m.name for m in hub_spec.members]
    base = _member_weights_base(names, hub_spec.default_member_weights)
    weights_list = _apply_member_weight_overrides(names, base, opts, scope="hub")

    leg_opts = dict(opts)
    leg_opts.pop("member_weights", None)
    leg_opts.pop("members", None)
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

    names = [m.name for m in federated_spec.members]
    base = _member_weights_base(names, None)
    weights_list = _apply_member_weight_overrides(names, base, opts, scope="federated")

    leg_opts = dict(opts)
    leg_opts.pop("member_weights", None)
    leg_opts.pop("members", None)
    leg_opts.pop("fusion", None)

    return cast(SearchOptions, leg_opts), weights_list


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
