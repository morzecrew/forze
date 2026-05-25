"""Compatibility helpers."""


def require_clickhouse() -> None:
    """Raise a clear error when the ``clickhouse`` extra is not installed."""

    try:
        import clickhouse_connect  # pyright: ignore[reportUnusedImport, reportMissingTypeStubs]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_clickhouse requires 'forze[clickhouse]' extra") from e
