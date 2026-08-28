"""Typing-only base declaring what the composed analytics mixins reach on ``self``.

Each mixin lives in its own module but only ever runs as part of
:class:`~.adapter.PostgresAnalyticsAdapter`, so a type checker reading one in isolation
cannot see what its siblings contribute. Declaring that surface here under
``TYPE_CHECKING`` is how the rest of the codebase states it (see
:class:`~forze.application.integrations.document.DocumentAdapterMixinBase`): the block is
empty at runtime and adds nothing to the MRO.

Unlike a separate host protocol reached through a ``cast``, these declarations sit in the
real MRO, so a signature that drifts from the implementation in
:class:`~._query.PostgresAnalyticsQueryMixin` is an ``override`` error rather than a
silent lie. Keep them exact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from forze.application.contracts.analytics import AnalyticsRunOptions, AnalyticsSpec
    from forze.base.primitives import JsonDict, StrKey
    from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig
    from forze_postgres.kernel.client import PostgresClientPort
    from forze_postgres.kernel.gateways import PostgresQualifiedName

# ----------------------- #


class PostgresAnalyticsMixinBase[R: BaseModel, Ing: BaseModel]:
    """Attributes and methods the analytics mixins rely on their siblings to provide."""

    if TYPE_CHECKING:
        # Supplied by the adapter itself.
        client: PostgresClientPort
        spec: AnalyticsSpec[R, Ing]
        config: PostgresAnalyticsConfig

        # Supplied by PostgresAnalyticsQueryMixin.
        def _validated_params(self, query_key: StrKey, params: BaseModel) -> BaseModel: ...

        async def _ingest_qname(self) -> PostgresQualifiedName: ...

        def _max_append_rows(self) -> int: ...

        def _cursor_column(self, query_key: StrKey) -> str | None: ...

        def _param_dict(self, params: BaseModel | JsonDict) -> dict[str, object]: ...

        async def _fetch_rows(
            self,
            query_key: StrKey,
            params: BaseModel | JsonDict,
            *,
            options: AnalyticsRunOptions | None,
            limit: int | None,
            offset: int | None,
        ) -> list[JsonDict]: ...

        async def _run_with_timeout(
            self,
            options: AnalyticsRunOptions | None,
            fn: Callable[[], Awaitable[Any]],
        ) -> Any: ...
