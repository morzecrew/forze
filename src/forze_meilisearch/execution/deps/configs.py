"""Meilisearch dependency configuration types."""

from __future__ import annotations

from typing import Any, Literal, Mapping, NotRequired, Sequence, TypedDict, final

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.base.exceptions import exc

# ----------------------- #

MeilisearchFederatedMerge = Literal["federation", "rrf"]


class _BaseMeilisearchConfig(TypedDict, total=False):
    tenant_aware: bool


@final
class MeilisearchSearchConfig(_BaseMeilisearchConfig):
    """Physical Meilisearch mapping for one :class:`~forze.application.contracts.search.SearchSpec`."""

    index_uid: str
    """Meilisearch index UID."""

    primary_key: NotRequired[str]
    """Document primary key attribute (default ``id``)."""

    field_map: NotRequired[Mapping[str, str]]
    """Maps logical :class:`SearchSpec` field names to index attribute names."""

    searchable_attributes: NotRequired[Sequence[str]]
    """Override searchable attributes for :meth:`~forze.application.contracts.search.SearchCommandPort.ensure_index`."""

    filterable_attributes: NotRequired[Sequence[str]]
    """Override filterable attributes for ensure_index."""

    sortable_attributes: NotRequired[Sequence[str]]
    """Override sortable attributes for ensure_index."""

    ranking_rules: NotRequired[Sequence[str]]
    """Optional Meilisearch ranking rules."""

    wait_for_tasks: NotRequired[bool]
    """When ``True`` (default), await Meilisearch task completion after writes."""


@final
class MeilisearchFederatedSearchConfig(_BaseMeilisearchConfig):
    """Configuration for :class:`~forze_meilisearch.adapters.search.federated.MeilisearchFederatedSearchAdapter`."""

    members: Mapping[str, MeilisearchSearchConfig]
    """Per-member index configuration (keys are :class:`SearchSpec` names)."""

    merge: NotRequired[MeilisearchFederatedMerge]
    """``federation`` (default) uses Meilisearch multi-search federation; ``rrf`` uses coordinator RRF."""

    rrf_k: NotRequired[int]
    """RRF smoothing constant when ``merge`` is ``rrf`` (default 60)."""

    rrf_per_leg_limit: NotRequired[int]
    """Max hits per leg when ``merge`` is ``rrf`` (default 5000)."""


def validate_meilisearch_search_conf(
    cfg: MeilisearchSearchConfig,
    spec: SearchSpec[Any],
) -> None:
    if not cfg.get("index_uid"):
        raise exc.configuration("Meilisearch search config requires index_uid.")


def validate_meilisearch_federated_search_conf(
    cfg: MeilisearchFederatedSearchConfig,
    spec: FederatedSearchSpec[Any],
) -> None:
    if len(cfg["members"]) < 2:
        raise exc.configuration(
            "Federated Meilisearch search requires at least two member configurations.",
        )

    merge = cfg.get("merge", "federation")

    if merge not in ("federation", "rrf"):
        raise exc.configuration(
            f"Meilisearch federated merge {merge!r} must be 'federation' or 'rrf'.",
        )

    for member in spec.members:
        if isinstance(member, HubSearchSpec):
            raise exc.configuration(
                f"Federated Meilisearch search does not support hub member {member.name!r}.",
            )

        if member.name not in cfg["members"]:
            raise exc.configuration(
                f"Federated member {member.name!r} missing from MeilisearchFederatedSearchConfig['members'].",
            )

        validate_meilisearch_search_conf(cfg["members"][member.name], member)
