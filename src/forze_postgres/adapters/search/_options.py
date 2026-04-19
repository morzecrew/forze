"""Normalize :class:`~forze.application.contracts.search.SearchOptions` per adapter kind."""

from __future__ import annotations

from typing import Any, cast

from forze.application.contracts.search import HubSearchSpec, SearchOptions
from forze.base.errors import CoreError

from .._logger import logger

# ----------------------- #


def search_options_for_simple_adapter(
    options: SearchOptions | None,
) -> SearchOptions:
    """Drop hub-only keys; log when callers pass them to simple search."""

    opts = dict(options or {})
    if "member_weights" in opts or "members" in opts:
        logger.warning(
            "postgres_search_options_hub_keys_ignored",
            message=(
                "SearchOptions member_weights and members are ignored for simple "
                "(single-index) search"
            ),
        )
        opts.pop("member_weights", None)
        opts.pop("members", None)

    return cast(SearchOptions, opts)


def prepare_hub_search_options(
    hub_spec: HubSearchSpec[Any],
    options: SearchOptions | None,
) -> tuple[SearchOptions, list[float]]:
    """Strip simple field-tuning and hub member keys; return leg options + per-member weights."""

    opts = dict(options or {})

    if "weights" in opts or "fields" in opts:
        logger.warning(
            "postgres_hub_search_options_field_tuning_ignored",
            message=(
                "SearchOptions weights and fields are ignored for hub search "
                "(use per-leg SearchSpec and member_weights / members instead)"
            ),
        )
        opts.pop("weights", None)
        opts.pop("fields", None)

    weights_list = _resolve_hub_member_weights(hub_spec, opts)

    leg_opts = dict(opts)
    leg_opts.pop("member_weights", None)
    leg_opts.pop("members", None)

    return cast(SearchOptions, leg_opts), weights_list


def _resolve_hub_member_weights(
    hub_spec: HubSearchSpec[Any],
    opts: dict[str, Any],
) -> list[float]:
    names = [m.name for m in hub_spec.members]

    if hub_spec.default_member_weights is not None:
        base = {n: float(hub_spec.default_member_weights[n]) for n in names}

    else:
        base = dict.fromkeys(names, 1.0)

    if opts.get("member_weights"):
        mw = opts["member_weights"]
        for k, v in mw.items():
            if k not in base:
                logger.warning(
                    "postgres_hub_search_unknown_member_weight",
                    member=k,
                    message="Ignoring member_weights entry for unknown hub member",
                )
                continue

            w = float(v)

            if w < 0 or w > 1:
                raise CoreError(
                    f"Member weight for hub member {k!r} must be between 0.0 and 1.0.",
                )

            base[k] = w

    elif opts.get("members") is not None:
        listed = set(opts["members"])

        for n in names:
            base[n] = 1.0 if n in listed else 0.0

    return [base[n] for n in names]
