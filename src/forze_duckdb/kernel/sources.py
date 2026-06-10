"""Typed lake / lakehouse source declarations that compile to DuckDB scan expressions.

A :class:`DuckDbSource` is a declarative stand-in for the raw scan string a user would
otherwise hand-author (``read_parquet('s3://bucket/*.parquet')``, ``delta_scan(...)``,
``iceberg_scan(...)``). It compiles to that expression via :meth:`DuckDbSource.scan_expr`
and reports the DuckDB extensions it needs via :meth:`DuckDbSource.required_extensions`,
so the lifecycle hook can register the view and auto-load the right extensions.

Source paths are operator-controlled configuration, not request input. They are still
single-quote-escaped defensively so a stray quote breaks neither the view DDL nor a query.
"""

from __future__ import annotations

import abc
from typing import final

import attrs

# ----------------------- #

_REMOTE_SCHEMES: tuple[str, ...] = (
    "s3://",
    "gs://",
    "gcs://",
    "r2://",
    "az://",
    "azure://",
    "http://",
    "https://",
)


def _sql_str(value: str) -> str:
    """Render *value* as a single-quoted SQL string literal, escaping embedded quotes."""

    return "'" + value.replace("'", "''") + "'"


def _httpfs_if_remote(path: str) -> tuple[str, ...]:
    """Return ``("httpfs",)`` when *path* points at object storage / HTTP, else ``()``."""

    return ("httpfs",) if path.startswith(_REMOTE_SCHEMES) else ()


# ----------------------- #


class DuckDbSource(abc.ABC):
    """A declarative lake / lakehouse source that compiles to a DuckDB scan expression."""

    @abc.abstractmethod
    def scan_expr(self) -> str:
        """Return the DuckDB scan expression (the right-hand side of ``FROM``)."""

    # ....................... #

    @abc.abstractmethod
    def required_extensions(self) -> tuple[str, ...]:
        """Return the DuckDB extensions this source needs ``INSTALL`` + ``LOAD``-ed."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class ParquetSource(DuckDbSource):
    """Read one or more Parquet files (local path, glob, or object-storage URI)."""

    path: str
    """File path, glob, or URI (e.g. ``s3://bucket/events/*.parquet``)."""

    hive_partitioning: bool = False
    """Infer columns from Hive-style ``key=value`` path segments."""

    union_by_name: bool = False
    """Unify files by column name rather than position (tolerates schema drift)."""

    # ....................... #

    def scan_expr(self) -> str:
        opts: list[str] = []

        if self.hive_partitioning:
            opts.append("hive_partitioning = true")

        if self.union_by_name:
            opts.append("union_by_name = true")

        args = _sql_str(self.path)

        if opts:
            args += ", " + ", ".join(opts)

        return f"read_parquet({args})"

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return _httpfs_if_remote(self.path)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class CsvSource(DuckDbSource):
    """Read one or more CSV files (local path, glob, or object-storage URI)."""

    path: str
    """File path, glob, or URI."""

    header: bool = True
    """First row is a header."""

    delim: str | None = None
    """Field delimiter; ``None`` lets DuckDB auto-detect."""

    auto_detect: bool = True
    """Auto-detect types and dialect."""

    # ....................... #

    def scan_expr(self) -> str:
        opts = [
            f"header = {'true' if self.header else 'false'}",
            f"auto_detect = {'true' if self.auto_detect else 'false'}",
        ]

        if self.delim is not None:
            opts.append(f"delim = {_sql_str(self.delim)}")

        return f"read_csv({_sql_str(self.path)}, {', '.join(opts)})"

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return _httpfs_if_remote(self.path)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class JsonSource(DuckDbSource):
    """Read one or more JSON files (local path, glob, or object-storage URI)."""

    path: str
    """File path, glob, or URI."""

    format: str = "auto"
    """DuckDB JSON read format: ``auto``, ``newline_delimited``, ``array``, ``unstructured``."""

    # ....................... #

    def scan_expr(self) -> str:
        return f"read_json({_sql_str(self.path)}, format = {_sql_str(self.format)})"

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return _httpfs_if_remote(self.path)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class IcebergSource(DuckDbSource):
    """Read an Apache Iceberg table by its metadata location (metadata-path scan).

    Catalog-managed access (REST / Glue ``ATTACH``) is intentionally out of scope here;
    point :attr:`metadata_path` at the table root or a specific ``*.metadata.json``.
    """

    metadata_path: str
    """Iceberg table metadata location (table root or a ``*.metadata.json`` file)."""

    allow_moved_paths: bool = True
    """Resolve data files even if the table has been relocated since the metadata was written."""

    # ....................... #

    def scan_expr(self) -> str:
        opts = ", allow_moved_paths = true" if self.allow_moved_paths else ""

        return f"iceberg_scan({_sql_str(self.metadata_path)}{opts})"

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return ("iceberg", *_httpfs_if_remote(self.metadata_path))


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DeltaSource(DuckDbSource):
    """Read a Delta Lake table by its table directory (local path or object-storage URI)."""

    table_path: str
    """Delta table directory (e.g. ``s3://bucket/warehouse/events``)."""

    # ....................... #

    def scan_expr(self) -> str:
        return f"delta_scan({_sql_str(self.table_path)})"

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return ("delta", *_httpfs_if_remote(self.table_path))


# ----------------------- #


def compile_source(source: str | DuckDbSource) -> str:
    """Return the scan expression for *source*; a raw string passes through unchanged."""

    return source if isinstance(source, str) else source.scan_expr()


# ....................... #


def source_extensions(source: str | DuckDbSource) -> tuple[str, ...]:
    """Return the extensions *source* requires; a raw string requires none (caller owns it)."""

    return () if isinstance(source, str) else source.required_extensions()
