"""Postgres federated search dep factory."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.search import (
    FederatedSearchQueryDepPort,
    FederatedSearchSpec,
    HubSearchSpec,
)
from forze.base.exceptions import exc

from ....adapters import PostgresFederatedSearchAdapter
from ..configs import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegHub,
    PostgresFederatedSearchLegSearch,
)
from ..configs.search import validate_fts_groups_for_search_spec
from ..keys import PostgresClientDepKey
from ._snapshot import snapshot_coord
from .hub import ConfigurablePostgresHubSearch
from .search import postgres_search_port_for_config

if TYPE_CHECKING:
    from forze.application.contracts.search import SearchQueryPort
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresFederatedSearch(FederatedSearchQueryDepPort):
    """Build :class:`PostgresFederatedSearchAdapter` from spec + config."""

    config: PostgresFederatedSearchConfig
    """Per-member search or embedded hub configuration."""

    # ....................... #

    def __call__(
        self,
        context: "ExecutionContext",
        spec: FederatedSearchSpec[Any],
    ) -> PostgresFederatedSearchAdapter[Any]:
        legs: list[tuple[str, "SearchQueryPort[Any]"]] = []

        for m in spec.members:
            leg = self.config.members.get(m.name)

            if leg is None:
                raise exc.internal(
                    f"Member '{m.name}' not found in PostgresFederatedSearchConfig.members.",
                )

            if isinstance(leg, PostgresFederatedSearchLegHub):
                if not isinstance(m, HubSearchSpec):
                    raise exc.internal(
                        f"Federated member {m.name!r} uses embedded hub config but "
                        "spec member is not a HubSearchSpec.",
                    )

                hub_cfg = leg.hub
                if not hub_cfg.tenant_aware and self.config.tenant_aware:
                    hub_cfg = attrs.evolve(hub_cfg, tenant_aware=True)

                port = ConfigurablePostgresHubSearch(config=hub_cfg)(context, m)
                legs.append((m.name, port))
                continue

            if not isinstance(
                leg, PostgresFederatedSearchLegSearch
            ):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise exc.internal(
                    f"Unsupported federated leg type for member {m.name!r}.",
                )

            if isinstance(m, HubSearchSpec):
                raise exc.internal(
                    f"Federated member {m.name!r} is a HubSearchSpec but config is "
                    "PostgresFederatedSearchLegSearch, not PostgresFederatedSearchLegHub.",
                )

            search_cfg = leg.search
            if search_cfg.engine == "fts":
                if search_cfg.fts_groups is None:
                    raise exc.internal(
                        "FTS groups are required for FTS federated member."
                    )
                validate_fts_groups_for_search_spec(m, search_cfg.fts_groups)

            port_plain = postgres_search_port_for_config(
                context,
                m,
                search_cfg,
            )
            legs.append((m.name, port_plain))

        return PostgresFederatedSearchAdapter(
            federated_spec=spec,
            legs=tuple(legs),
            rrf_k=self.config.rrf_k,
            rrf_per_leg_limit=self.config.rrf_per_leg_limit,
            postgres_client=context.deps.provide(PostgresClientDepKey),
            snapshot_coord=snapshot_coord(context, spec.snapshot),
        )
