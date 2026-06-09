"""Async Meilisearch client wrapper."""

from forze_meilisearch._compat import require_meilisearch

require_meilisearch()

# ....................... #

from datetime import timedelta
from typing import Any, final

import attrs
from meilisearch_python_sdk import AsyncClient
from meilisearch_python_sdk.models.search import Federation, SearchParams
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .errors import exc_interceptor
from .port import MeilisearchClientPort
from .value_objects import MeilisearchConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class MeilisearchClient(MeilisearchClientPort):
    """Thin wrapper around :class:`meilisearch_python_sdk.AsyncClient`."""

    __client: AsyncClient | None = attrs.field(default=None, init=False)

    # ....................... #

    async def initialize(
        self,
        url: str,
        api_key: str | SecretStr | None = None,
        *,
        config: MeilisearchConfig | None = None,
    ) -> None:
        if self.__client is not None:
            return

        key: str | None

        if isinstance(api_key, SecretStr):
            key = api_key.get_secret_value()

        else:
            key = api_key

        cfg = config or MeilisearchConfig()
        self.__client = AsyncClient(
            url,
            key,
            timeout=int(cfg.timeout.total_seconds()),
        )

    # ....................... #

    def _require_client(self) -> AsyncClient:
        if self.__client is None:
            from forze.base.exceptions import exc

            raise exc.internal("MeilisearchClient is not initialized.")

        return self.__client

    # ....................... #

    @exc_interceptor.coroutine("meilisearch.aclose")  # type: ignore[untyped-decorator]
    async def aclose(self) -> None:
        if self.__client is not None:
            await self.__client.aclose()
            self.__client = None

    # ....................... #

    async def close(self) -> None:
        """Alias for :meth:`aclose` (the standard ``close()`` disposal contract)."""

        await self.aclose()

    # ....................... #

    @exc_interceptor.coroutine("meilisearch.health")  # type: ignore[untyped-decorator]
    async def health(self) -> bool:
        client = self._require_client()
        result = await client.health()
        return str(getattr(result, "status", "")).lower() == "available"

    # ....................... #

    def index(self, uid: str) -> Any:
        return self._require_client().index(uid)

    # ....................... #

    @exc_interceptor.coroutine("meilisearch.get_or_create_index")  # type: ignore[untyped-decorator]
    async def get_or_create_index(
        self,
        uid: str,
        *,
        primary_key: str | None = None,
    ) -> Any:
        client = self._require_client()
        return await client.get_or_create_index(uid, primary_key=primary_key)

    # ....................... #

    @exc_interceptor.coroutine("meilisearch.multi_search")  # type: ignore[untyped-decorator]
    async def multi_search(
        self,
        queries: list[SearchParams],
        *,
        federation: JsonDict | None = None,
    ) -> Any:
        client = self._require_client()

        fed_model: Federation | None = None

        if federation is not None:
            fed_model = Federation(
                offset=int(federation.get("offset", 0)),
                limit=int(federation.get("limit", 20)),
            )

        return await client.multi_search(queries, federation=fed_model)  # type: ignore[return-value]

    # ....................... #

    @exc_interceptor.coroutine("meilisearch.wait_for_task")  # type: ignore[untyped-decorator]
    async def wait_for_task(
        self,
        task_uid: int,
        *,
        timeout: timedelta | None = None,
    ) -> Any:
        if timeout is not None and timeout.total_seconds() <= 0:
            raise exc.internal("Timeout must be positive")

        client = self._require_client()
        return await client.wait_for_task(
            task_uid,
            timeout_in_ms=int(timeout.total_seconds() * 1000) if timeout else None,
        )
