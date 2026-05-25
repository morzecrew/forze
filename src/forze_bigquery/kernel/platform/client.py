from forze_bigquery._compat import require_bigquery

require_bigquery()

# ....................... #

import asyncio
import os
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, final

import attrs
from gcloud.aio.bigquery import Job, Table, query_response_to_dict
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from .errors import bigquery_handled
from .port import BigQueryClientPort
from .query import build_sync_query_request, params_to_query_parameters
from .value_objects import BigQueryConfig, BigQueryQueryResult

# ----------------------- #

_DEFAULT_TIMEOUT = 60
_POLL_INTERVAL_SEC = 0.25
_MAX_POLL_ATTEMPTS = 240

# ....................... #


@final
@attrs.define(slots=True)
class BigQueryClient(BigQueryClientPort):
    """Async BigQuery client backed by :mod:`gcloud.aio.bigquery`."""

    __project_id: str | None = attrs.field(default=None, init=False)
    __config: BigQueryConfig | None = attrs.field(default=None, init=False)
    __service_file: str | None = attrs.field(default=None, init=False)
    __api_root: str | None = attrs.field(default=None, init=False)
    __session: Any = attrs.field(default=None, init=False)

    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("bigquery_depth", default=0),
        init=False,
    )

    # ....................... #

    async def initialize(
        self,
        project_id: str,
        *,
        service_file: str | None = None,
        config: BigQueryConfig | None = None,
    ) -> None:
        """Configure project, credentials, and shared HTTP session."""

        if self.__project_id is not None:
            return

        self.__project_id = project_id
        self.__config = config
        self.__service_file = service_file

        if host := os.environ.get("BIGQUERY_EMULATOR_HOST"):
            self.__api_root = host.rstrip("/")

        from aiohttp import ClientSession

        self.__session = ClientSession()

    # ....................... #

    async def close(self) -> None:
        session = self.__session

        if session is not None:
            await session.close()
            self.__session = None

        self.__project_id = None
        self.__config = None

    # ....................... #

    def __timeout(self, override: int | None) -> int:
        if override is not None:
            return override

        if self.__config is not None:
            return max(1, int(self.__config.timeout.total_seconds()))

        return _DEFAULT_TIMEOUT

    # ....................... #

    def __use_legacy_sql(self) -> bool:
        if self.__config is not None:
            return self.__config.use_legacy_sql

        return False

    # ....................... #

    def __default_max_bytes(self) -> int | None:
        if self.__config is not None:
            return self.__config.maximum_bytes_billed

        return None

    # ....................... #

    def __require_project_id(self) -> str:
        if self.__project_id is None:
            raise CoreError("BigQuery client is not initialized")

        return self.__project_id

    # ....................... #

    def __require_session(self) -> Any:
        if self.__session is None:
            raise CoreError("BigQuery client is not initialized")

        return self.__session

    # ....................... #

    @property
    def project_id(self) -> str:
        """Configured GCP project id."""

        return self.__require_project_id()

    # ....................... #

    @property
    def session(self) -> Any:
        """Shared aiohttp session for ``Job`` / ``Table`` constructors."""

        return self.__require_session()

    # ....................... #

    @property
    def api_root(self) -> str | None:
        """API root when ``BIGQUERY_EMULATOR_HOST`` is set in the environment."""

        return self.__api_root

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncIterator[Any]:
        depth = self.__ctx_depth.get()

        if depth > 0:
            self.__ctx_depth.set(depth + 1)
            try:
                yield self
            finally:
                self.__ctx_depth.set(depth)
            return

        token = self.__ctx_depth.set(1)
        try:
            yield self
        finally:
            self.__ctx_depth.reset(token)

    # ....................... #

    def job(self, job_id: str | None = None) -> Job:
        return Job(
            job_id=job_id,
            project=self.__require_project_id(),
            service_file=self.__service_file,
            session=self.__require_session(),
            api_root=self.__api_root,
        )

    # ....................... #

    def table(self, dataset: str, table: str) -> Table:
        return Table(
            dataset_name=dataset,
            table_name=table,
            project=self.__require_project_id(),
            service_file=self.__service_file,
            session=self.__require_session(),
            api_root=self.__api_root,
        )

    # ....................... #

    def __parse_rows(self, response: JsonDict) -> BigQueryQueryResult:
        rows: list[JsonDict] = []

        if "schema" in response and response.get("rows") is not None:
            rows = query_response_to_dict(response)

        total_raw = response.get("totalRows")
        total_rows = int(total_raw) if total_raw is not None else None

        job_ref: JsonDict = response.get("jobReference") or {}
        job_id: str | None = job_ref.get("jobId")

        return BigQueryQueryResult(
            rows=rows,
            total_rows=total_rows,
            page_token=response.get("pageToken"),
            job_id=job_id,
        )

    # ....................... #

    @bigquery_handled("bigquery.run_query")  # type: ignore[untyped-decorator]
    async def run_query(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        dry_run: bool = False,
        maximum_bytes_billed: int | None = None,
        max_results: int | None = None,
        start_index: int | None = None,
        page_token: str | None = None,
        timeout: int | None = None,
    ) -> BigQueryQueryResult:
        query_parameters = (
            params_to_query_parameters(params) if params is not None else None
        )
        max_billed = (
            maximum_bytes_billed
            if maximum_bytes_billed is not None
            else self.__default_max_bytes()
        )
        body = build_sync_query_request(
            sql,
            query_parameters=query_parameters,
            dry_run=dry_run,
            use_legacy_sql=self.__use_legacy_sql(),
            maximum_bytes_billed=max_billed,
            max_results=max_results,
            start_index=start_index,
            page_token=page_token,
        )

        job = self.job()
        timeout_sec = self.__timeout(timeout)
        response = await job.query(body, timeout=timeout_sec)

        if dry_run:
            return BigQueryQueryResult(rows=[], total_rows=0, page_token=None)

        if response.get("jobComplete", True) and (
            "schema" in response or not response.get("errors")
        ):
            return self.__parse_rows(response)

        job_ref: JsonDict = response.get("jobReference") or {}
        job_id: str | None = job_ref.get("jobId")

        if not job_id:
            raise CoreError("BigQuery query did not return a job id.")

        await self.__poll_job_done(job_id, timeout=timeout_sec)

        results = await self.job(job_id).get_query_results(
            timeout=timeout_sec,
            params={"maxResults": max_results} if max_results else None,
        )

        return self.__parse_rows(results)

    # ....................... #

    async def __poll_job_done(self, job_id: str, *, timeout: int) -> None:
        job = self.job(job_id)
        attempts = min(_MAX_POLL_ATTEMPTS, max(1, timeout * 4))

        for _ in range(attempts):
            status = await job.get_job(timeout=timeout)
            status_dict: JsonDict = status.get("status") or {}
            state: str | None = status_dict.get("state")

            if state == "DONE":
                errors = status.get("status", {}).get("errors")

                if errors:
                    raise CoreError(f"BigQuery job failed: {errors!r}")

                return

            if state in {"FAILED", "CANCELLED"}:
                raise CoreError(f"BigQuery job ended with state {state!r}")

            await asyncio.sleep(_POLL_INTERVAL_SEC)

        raise CoreError("BigQuery job polling timed out.")

    # ....................... #

    @bigquery_handled("bigquery.run_query_all_pages")  # type: ignore[untyped-decorator]
    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        if fetch_batch_size < 1:
            raise CoreError("fetch_batch_size must be >= 1")

        all_rows: list[JsonDict] = []
        page_token: str | None = None

        while True:
            result = await self.run_query(
                sql,
                params,
                maximum_bytes_billed=maximum_bytes_billed,
                max_results=fetch_batch_size,
                page_token=page_token,
                timeout=timeout,
            )
            all_rows.extend(result.rows)

            if max_rows is not None and len(all_rows) >= max_rows:
                return all_rows[:max_rows]

            page_token = result.page_token
            if not page_token:
                break

        return all_rows

    # ....................... #

    @bigquery_handled("bigquery.insert_rows")  # type: ignore[untyped-decorator]
    async def insert_rows(
        self,
        dataset: str,
        table: str,
        rows: list[JsonDict],
        *,
        insert_id_field: str | None = None,
        timeout: int | None = None,
    ) -> int:
        if not rows:
            return 0

        if insert_id_field is not None:

            def insert_id_fn(row: JsonDict, *, field: str = insert_id_field) -> str:
                val = row.get(field)
                return str(val) if val is not None else Table._mk_unique_insert_id(row)  # type: ignore[reportPrivateUsage]

        else:
            insert_id_fn = None  # type: ignore[assignment]

        bq_table = self.table(dataset, table)
        response = await bq_table.insert(
            rows,
            timeout=self.__timeout(timeout),
            insert_id_fn=insert_id_fn,
        )

        insert_errors: list[JsonDict] = response.get("insertErrors") or []
        accepted = len(rows) - len(insert_errors)

        if insert_errors and accepted == 0:
            raise CoreError(f"BigQuery insert failed: {insert_errors!r}")

        return accepted
