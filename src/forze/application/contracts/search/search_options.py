"""Normalize :class:`~forze.application.contracts.search.SearchOptions` for adapter shape."""

from typing import Any, Literal, Mapping, cast

from forze.application.contracts.search.specs import (
    FederatedSearchSpec,
    HubSearchSpec,
)
from forze.application.contracts.search.types import SearchOptions
from forze.base.errors import CoreError

from ..._logger import logger

# ----------------------- #


def _strip_result_snapshot_leg_options(opts: dict[str, Any]) -> None:
    """Remove the nested result snapshot request from options passed to hub/federation legs."""

    opts.pop("result_snapshot", None)


# ....................... #


def search_options_for_simple_adapter(
    options: SearchOptions | None,
) -> SearchOptions:
    """Drop hub/federated member keys on single-index search; warn when callers pass them."""

    opts = dict(options or {})

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
) -> tuple[SearchOptions, list[float]]:
    """Strip simple field-tuning and hub member keys; return leg options + per-member weights."""

    opts = dict(options or {})

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
    _strip_result_snapshot_leg_options(leg_opts)

    return cast(SearchOptions, leg_opts), weights_list


# ....................... #


def prepare_federated_search_options(
    federated_spec: FederatedSearchSpec[Any],
    options: SearchOptions | None,
) -> tuple[SearchOptions, list[float]]:
    """Strip field-level tuning and federation member keys; return leg options + weights."""

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
    _strip_result_snapshot_leg_options(leg_opts)

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
                raise CoreError(
                    f"Member weight for {scope} member {k!r} must be between 0.0 and 1.0.",
                )

            base[k] = w

    elif opts.get("members") is not None:
        listed = set(opts["members"])

        for n in names:
            base[n] = 1.0 if n in listed else 0.0

    return [base[n] for n in names]
