"""Compatibility helpers."""


def require_fastapi() -> None:
    """Raise a clear error when ``fastapi`` extra is not installed."""

    try:
        import fastapi  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_fastapi requires 'forze[fastapi]' extra") from e
