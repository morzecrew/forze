"""Compatibility helpers."""


def require_mongo() -> None:
    """Raise a clear error when ``mongo`` extra is not installed."""

    try:
        import pymongo  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_mongo requires 'forze[mongo]' extra") from e
