"""Compatibility helpers."""


def require_redis() -> None:
    """Raise a clear error when ``redis`` extra is not installed."""

    try:
        import redis  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_redis requires 'forze[redis]' extra") from e
