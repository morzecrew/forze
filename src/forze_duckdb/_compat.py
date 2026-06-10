"""Compatibility helpers."""


def require_duckdb() -> None:
    """Raise a clear error when the ``duckdb`` extra is not installed."""

    try:
        import duckdb  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import pyarrow  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_duckdb requires 'forze[duckdb]' extra") from e
