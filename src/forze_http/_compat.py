"""Compatibility helpers."""


def require_http() -> None:
    """Raise a clear error when the ``http`` extra is not installed."""

    try:
        import httpx  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_http requires 'forze[http]' extra") from e
