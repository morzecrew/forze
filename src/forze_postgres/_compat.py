"""Compatibility helpers."""


def require_psycopg() -> None:
    """Raise a clear error when ``postgres`` extra is not installed."""

    try:
        import psycopg  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import psycopg_pool  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_postgres requires 'forze[postgres]' extra") from e
