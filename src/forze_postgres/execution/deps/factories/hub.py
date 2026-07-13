"""Postgres hub search dep factory."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.crypto import (
    DeterministicCipherDepKey,
    KeyringDepKey,
)
from forze.application.contracts.search import HubSearchQueryDepPort
from forze.application.integrations.search import (
    resolve_search_read_codec_spec,
    search_spec_encrypts,
)

from ....adapters import PostgresHubSearchAdapter
from ..keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from ._snapshot import result_snapshot
from .hub_builder import build_hub_leg_runtimes

if TYPE_CHECKING:
    from forze.application.contracts.search import HubSearchSpec
    from forze.application.execution.context import ExecutionContext

    from ..configs import PostgresHubSearchConfig


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresHubSearch(HubSearchQueryDepPort):
    """Build :class:`PostgresHubSearchAdapter` from spec + :class:`PostgresHubSearchConfig`."""

    config: "PostgresHubSearchConfig"
    """Postgres hub relation, per-leg indexes/heaps, merge options."""

    # ....................... #

    def __call__(
        self,
        context: "ExecutionContext",
        spec: "HubSearchSpec[Any]",
    ) -> PostgresHubSearchAdapter[Any]:
        members, vector_embedders = build_hub_leg_runtimes(context, spec, self.config)

        # Decrypt encrypted hub-row fields out of hub search results (mirrors the document
        # write config; wraps the read codec, fail-closed without a keyring).
        spec = resolve_search_read_codec_spec(
            spec,
            keyring=(
                context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
            ),
            deterministic=(
                context.deps.provide(DeterministicCipherDepKey)
                if context.deps.exists(DeterministicCipherDepKey)
                else None
            ),
            tenant_provider=context.inv_ctx.get_tenant,
        )

        return PostgresHubSearchAdapter(
            hub_spec=spec,
            members=members,
            vector_embedders=vector_embedders,
            combine=self.config.combine_strategy,
            score_merge=self.config.merge_strategy,
            per_leg_limit=self.config.per_leg_limit,
            combo_limit=self.config.combo_limit,
            execution=self.config.execution,
            parallel_hub_cte_materialized=self.config.parallel_hub_cte_materialized,
            relation=self.config.hub,
            client=context.deps.provide(PostgresClientDepKey),
            codec=spec.resolved_read_codec,
            model_type=spec.model_type,
            introspector=context.deps.provide(PostgresIntrospectorDepKey),
            tenant_provider=context.inv_ctx.get_tenant,
            tenant_aware=self.config.tenant_aware,
            filter_table_alias="h",
            lenient_read_fields=spec.resolved_lenient_read_fields,
            nested_field_hints=self.config.nested_field_hints,
            result_snapshot=result_snapshot(
                context, spec.snapshot, encrypted=search_spec_encrypts(spec)
            ),
            read_validation=self.config.read_validation,
        )
