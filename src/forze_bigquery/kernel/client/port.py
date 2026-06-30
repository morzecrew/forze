"""Structural protocol for BigQuery clients."""

from datetime import timedelta
from typing import Any, AsyncGenerator, Awaitable, Protocol, Sequence

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .value_objects import BigQueryInsertResult, BigQueryQueryResult

# ----------------------- #


class BigQueryClientPort(Protocol):
    """Operations implemented by :class:`BigQueryClient`."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        dry_run: bool = False,
        maximum_bytes_billed: int | None = None,
        max_results: int | None = None,
        start_index: int | None = None,
        page_token: str | None = None,
        timeout: timedelta | None = None,
        default_dataset: str | None = None,
    ) -> Awaitable[BigQueryQueryResult]: ...  # pragma: no cover

    def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
        default_dataset: str | None = None,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover

    def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
        default_dataset: str | None = None,
    ) -> AsyncGenerator[Sequence[JsonDict]]: ...  # pragma: no cover

    def insert_rows(
        self,
        dataset: str,
        table: str,
        rows: list[JsonDict],
        *,
        insert_id_field: str | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[BigQueryInsertResult]: ...  # pragma: no cover

    def table(self, dataset: str, table: str) -> Any: ...  # pragma: no cover

    def job(self, job_id: str | None = None) -> Any: ...  # pragma: no cover
