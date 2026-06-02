"""Postgres hub search execution configs and validation."""

from typing import Any, Literal, Mapping, Sequence

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, frozen_mapping
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import (
    RelationSpec,
    coerce_relation_spec,
    is_static_relation,
)

from ....kernel.catalog.hub_fk_columns import normalize_hub_fk_columns
from .search import PostgresSearchConfig

# ----------------------- #


HubCombineStrategy = Literal["or", "and"]
HubMergeStrategy = Literal["max", "sum"]
HubExecutionMode = Literal["sql", "parallel"]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchMemberConfig(PostgresSearchConfig):
    """Configuration for a Postgres hub search member leg."""

    hub_fk: str | Sequence[str]
    """Hub foreign key column(s)."""

    heap_pk: str = ID_FIELD
    """Heap primary key column."""

    same_heap_as_hub: bool = False
    """When True, evaluate the leg on the hub relation (see integration docs)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchConfig(TenantAwareIntegrationConfig):
    """Postgres configuration for :class:`PostgresHubSearchAdapter`."""

    hub: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Hub relation (schema, table / view) for filters and row shape or resolver."""

    members: Mapping[StrKey, PostgresHubSearchMemberConfig] = attrs.field(
        converter=frozen_mapping,
    )
    """Per-member leg configurations keyed by ``SearchSpec.name``."""

    combine_strategy: HubCombineStrategy = "or"
    """How leg text matches combine."""

    merge_strategy: HubMergeStrategy = "max"
    """How leg scores merge for ordering."""

    nested_field_hints: Mapping[str, Any] | None = None
    """Per-path type hints for filters/sorts on the hub read projection."""

    per_leg_limit: int = 5000
    """Max ranked rows retained per hub leg before merge."""

    combo_limit: int | None = None
    """Cap merged hub rows before outer pagination; ``None`` derives from :attr:`per_leg_limit` and pagination."""

    execution: HubExecutionMode = "sql"
    """``sql``: single ``WITH`` query; ``parallel``: one ranked query per leg merged in the app."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.validate()

    # ....................... #

    def validate(self) -> None:
        """Validate hub members and FK wiring."""

        legs = list(self.members.values())

        if not legs:
            raise exc.internal("Hub search requires at least one leg configuration.")

        if self.per_leg_limit < 1:
            raise exc.internal("per_leg_limit must be at least 1.")

        if self.combo_limit is not None and self.combo_limit < 1:
            raise exc.internal("combo_limit must be at least 1 when set.")

        if self.execution not in ("sql", "parallel"):
            raise exc.internal("execution must be 'sql' or 'parallel'.")

        fk_seen: set[str] = set()

        for i, leg in enumerate(legs):
            for col in normalize_hub_fk_columns(leg.hub_fk):
                if col in fk_seen:
                    raise exc.internal(
                        "Each hub_fk column may belong to at most one leg "
                        "(duplicate column across legs).",
                    )
                fk_seen.add(col)

            if leg.engine == "fts" and not leg.fts_groups:
                raise exc.internal(
                    f"Hub search leg {i} with engine 'fts' requires fts_groups."
                )

            if leg.same_heap_as_hub:
                _validate_same_heap_as_hub(leg, i, self)


# ....................... #


def _validate_same_heap_as_hub(
    leg: PostgresHubSearchMemberConfig,
    leg_index: int,
    cfg: PostgresHubSearchConfig,
) -> None:
    if leg.engine == "fts":
        raise exc.internal(
            f"Hub search leg {leg_index} cannot use same_heap_as_hub with engine 'fts'.",
        )

    if leg.field_map:
        raise exc.internal(
            f"Hub search leg {leg_index} cannot use same_heap_as_hub together with 'field_map'.",
        )

    hub_pair: RelationSpec = cfg.hub
    heap_read: RelationSpec = leg.heap_relation

    if not is_static_relation(hub_pair) or not is_static_relation(heap_read):
        raise exc.internal(
            f"Hub search leg {leg_index} with same_heap_as_hub requires static "
            "hub and leg read/heap relations.",
        )

    if tuple(hub_pair) != tuple(heap_read):
        raise exc.internal(
            f"Hub search leg {leg_index} with same_heap_as_hub must use the same "
            "qualified relation as the hub in 'read' or 'heap'.",
        )

    fk_cols = normalize_hub_fk_columns(leg.hub_fk)

    if len(fk_cols) != 1 or fk_cols[0] != leg.heap_pk:
        raise exc.internal(
            f"Hub search leg {leg_index} with same_heap_as_hub requires 'hub_fk' "
            "to be a single column name equal to 'heap_pk' (default 'id').",
        )

    if leg.engine == "pgroonga" and leg.pgroonga_score_version != "v2":
        raise exc.internal(
            f"Hub search leg {leg_index} with same_heap_as_hub and engine 'pgroonga' "
            "requires 'pgroonga_score_version' 'v2'.",
        )
