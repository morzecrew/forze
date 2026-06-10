from forze_bigquery._compat import require_bigquery

require_bigquery()

# ....................... #

import asyncio
import os
from datetime import timedelta
from typing import Any, Awaitable, Callable, TypeVar, final

import attrs
from aiohttp import ClientSession
from gcloud.aio.bigquery import Job, Table, query_response_to_dict
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.owned_temp_path import OwnedTempPath

from .errors import exc_interceptor
from .port import BigQueryClientPort
from .query import build_sync_query_request, params_to_query_parameters
from .value_objects import (
    BigQueryConfig,
    BigQueryInsertResult,
    BigQueryQueryResult,
)

# ----------------------- #

T = TypeVar("T")

_READ_RETRY_EXC = (TimeoutError, OSError, ConnectionError)
_MAX_INSERT_ERRORS = 50

# ....................... #


@final
@attrs.define(slots=True)
class BigQueryClient(BigQueryClientPort):
    """Async BigQuery client backed by :mod:`gcloud.aio.bigquery`."""

    __project_id: str | None = attrs.field(default=None, init=False)
    __config: BigQueryConfig | None = attrs.field(default=None, init=False)
    __credential_path: OwnedTempPath = attrs.field(
        factory=OwnedTempPath.empty,
        init=False,
    )
    __api_root: str | None = attrs.field(default=None, init=False)
    __session: Any = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(
        self,
        project_id: str,
        *,
        service_file: str | None = None,
        service_file_owned: bool = False,
        config: BigQueryConfig | None = None,
    ) -> None:
        """Configure project, credentials, and shared HTTP session."""

        async with self.__init_lock:
            # Guard on the last-assigned field so a partial failure (e.g. session
            # creation) doesn't make later calls early-return on a broken client.
            if self.__session is not None:
                return

            self.__project_id = project_id
            self.__config = config or BigQueryConfig()
            self.__credential_path = OwnedTempPath(
                path=service_file,
                owned=service_file_owned,
            )

            if host := os.environ.get("BIGQUERY_EMULATOR_HOST"):
                self.__api_root = host.rstrip("/")

            self.__session = ClientSession()

    # ....................... #

    async def close(self) -> None:
        async with self.__init_lock:
            session_error: Exception | None = None
            cred_error: Exception | None = None

            try:
                session = self.__session

                if session is not None:
                    await session.close()

            except Exception as exc:
                session_error = exc

            finally:
                self.__session = None

            try:
                self.__credential_path.release()

            except Exception as exc:
                cred_error = exc

            finally:
                self.__credential_path = OwnedTempPath.empty()
                self.__project_id = None
                self.__config = None
                self.__api_root = None

            errors = [e for e in (session_error, cred_error) if e is not None]

            if len(errors) == 1:
                raise errors[0]

            if len(errors) > 1:
                raise ExceptionGroup(
                    "BigQuery client close failed", errors
                ) from errors[0]

    # ....................... #

    def __require_config(self) -> BigQueryConfig:
        return self.__config or BigQueryConfig()

    # ....................... #

    def __timeout(self, override: timedelta | None) -> int:
        if override is not None:
            return max(1, int(override.total_seconds()))

        return max(1, int(self.__require_config().timeout.total_seconds()))

    # ....................... #

    def __use_legacy_sql(self) -> bool:
        return self.__require_config().use_legacy_sql

    # ....................... #

    def __default_max_bytes(self) -> int | None:
        return self.__require_config().maximum_bytes_billed

    # ....................... #

    def __require_project_id(self) -> str:
        if self.__project_id is None:
            raise exc.internal("BigQuery client is not initialized")

        return self.__project_id

    # ....................... #

    def __require_session(self) -> Any:
        if self.__session is None:
            raise exc.internal("BigQuery client is not initialized")

        return self.__session

    # ....................... #

    async def __maybe_read_retry(
        self,
        op: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        cfg = self.__require_config()
        attempts = max(0, cfg.read_retry_attempts)
        base = max(0.0, cfg.read_retry_base_delay.total_seconds())
        last: BaseException | None = None

        for i in range(attempts + 1):
            try:
                return await fn()

            except _READ_RETRY_EXC as e:
                last = e

                if i >= attempts:
                    raise

                await asyncio.sleep(base * (2**i))

        if last is None:
            raise exc.internal("Last exception is None")

        raise last

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

    def job(self, job_id: str | None = None) -> Job:
        return Job(
            job_id=job_id,
            project=self.__require_project_id(),
            service_file=self.__credential_path.path,
            session=self.__require_session(),
            api_root=self.__api_root,
        )

    # ....................... #

    def table(self, dataset: str, table: str) -> Table:
        return Table(
            dataset_name=dataset,
            table_name=table,
            project=self.__require_project_id(),
            service_file=self.__credential_path.path,
            session=self.__require_session(),
            api_root=self.__api_root,
        )

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Check BigQuery connectivity with a lightweight dry-run query."""

        try:

            async def _probe() -> None:
                await self.run_query("SELECT 1", dry_run=True, timeout=None)

            await self.__maybe_read_retry("health", _probe)
            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #

    def __parse_rows(self, response: JsonDict) -> BigQueryQueryResult:
        rows: list[JsonDict] = []

        if "schema" in response and response.get("rows") is not None:
            rows = query_response_to_dict(response)

        total_raw = response.get("totalRows")
        total_rows = int(total_raw) if total_raw is not None else None

        job_ref: JsonDict = response.get("jobReference") or {}
        job_id: str | None = job_ref.get("jobId")

        bytes_raw = response.get("totalBytesProcessed")
        total_bytes = int(bytes_raw) if bytes_raw is not None else None

        return BigQueryQueryResult(
            rows=rows,
            total_rows=total_rows,
            page_token=response.get("pageToken"),
            job_id=job_id,
            total_bytes_processed=total_bytes,
        )

    # ....................... #

    async def __poll_job_done(self, job_id: str, *, timeout: int) -> None:
        cfg = self.__require_config()
        poll_interval = max(0.05, cfg.poll_interval.total_seconds())
        attempts = min(
            cfg.max_poll_attempts,
            max(1, int(timeout / poll_interval) if poll_interval else 1),
        )
        job = self.job(job_id)

        for _ in range(attempts):
            status = await job.get_job(timeout=timeout)
            status_dict: JsonDict = status.get("status") or {}
            state: str | None = status_dict.get("state")

            if state == "DONE":
                errors = status.get("status", {}).get("errors")

                if errors:
                    raise exc.internal(f"BigQuery job failed: {errors!r}")

                return

            if state in {"FAILED", "CANCELLED"}:
                raise exc.internal(f"BigQuery job ended with state {state!r}")

            await asyncio.sleep(poll_interval)

        raise exc.internal("BigQuery job polling timed out.")

    # ....................... #

    @exc_interceptor.coroutine("bigquery.run_query")  # type: ignore[untyped-decorator]
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
        timeout: timedelta | None = None,
    ) -> BigQueryQueryResult:
        async def _run() -> BigQueryQueryResult:
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
                parsed = self.__parse_rows(response)
                return BigQueryQueryResult(
                    rows=[],
                    total_rows=0,
                    page_token=None,
                    total_bytes_processed=parsed.total_bytes_processed,
                )

            if response.get("jobComplete", True) and (
                "schema" in response or not response.get("errors")
            ):
                return self.__parse_rows(response)

            job_ref: JsonDict = response.get("jobReference") or {}
            job_id: str | None = job_ref.get("jobId")

            if not job_id:
                raise exc.internal("BigQuery query did not return a job id.")

            await self.__poll_job_done(job_id, timeout=timeout_sec)

            results = await self.job(job_id).get_query_results(
                timeout=timeout_sec,
                params={"maxResults": max_results} if max_results else None,
            )

            return self.__parse_rows(results)

        return await self.__maybe_read_retry("run_query", _run)

    # ....................... #

    @exc_interceptor.coroutine("bigquery.run_query_all_pages")  # type: ignore[untyped-decorator]
    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        if fetch_batch_size < 1:
            raise exc.internal("fetch_batch_size must be >= 1")

        async def _run() -> list[JsonDict]:
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

        return await self.__maybe_read_retry("run_query_all_pages", _run)

    # ....................... #

    @exc_interceptor.coroutine("bigquery.insert_rows")  # type: ignore[untyped-decorator]
    async def insert_rows(
        self,
        dataset: str,
        table: str,
        rows: list[JsonDict],
        *,
        insert_id_field: str | None = None,
        timeout: timedelta | None = None,
    ) -> BigQueryInsertResult:
        if not rows:
            return BigQueryInsertResult(accepted=0)

        cfg = self.__require_config()
        batch_size = max(1, cfg.insert_batch_size)
        accepted_total = 0
        rejected_total = 0
        all_errors: list[JsonDict] = []

        if insert_id_field is not None:

            def insert_id_fn(row: JsonDict, *, field: str = insert_id_field) -> str:
                val = row.get(field)
                return (
                    str(val)
                    if val is not None
                    else Table._mk_unique_insert_id(row)  # type: ignore[reportPrivateUsage]
                )

        else:
            insert_id_fn = None  # type: ignore[assignment]

        bq_table = self.table(dataset, table)
        timeout_sec = self.__timeout(timeout)

        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            response = await bq_table.insert(
                batch,
                timeout=timeout_sec,
                insert_id_fn=insert_id_fn,
            )
            insert_errors: list[JsonDict] = response.get("insertErrors") or []
            accepted = len(batch) - len(insert_errors)
            accepted_total += accepted
            rejected_total += len(insert_errors)

            if insert_errors:
                remaining = _MAX_INSERT_ERRORS - len(all_errors)
                if remaining > 0:
                    all_errors.extend(insert_errors[:remaining])

        if all_errors and accepted_total == 0:
            raise exc.internal(f"BigQuery insert failed: {all_errors!r}")

        return BigQueryInsertResult(
            accepted=accepted_total,
            rejected=rejected_total,
            errors=tuple(all_errors),
        )
