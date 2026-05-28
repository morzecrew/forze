"""Structural protocol for the Meilisearch async client."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Protocol

from forze.base.primitives import JsonDict

if TYPE_CHECKING:
    from meilisearch_python_sdk.models.search import SearchParams

# ----------------------- #


class MeilisearchClientPort(Protocol):
    """Operations implemented by :class:`~forze_meilisearch.kernel.platform.client.MeilisearchClient`."""

    def aclose(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[bool]: ...  # pragma: no cover

    def index(self, uid: str) -> Any: ...  # pragma: no cover

    def get_or_create_index(
        self,
        uid: str,
        *,
        primary_key: str | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def multi_search(
        self,
        queries: list["SearchParams"],
        *,
        federation: JsonDict | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def wait_for_task(
        self,
        task_uid: int,
        *,
        timeout: timedelta | None = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover
