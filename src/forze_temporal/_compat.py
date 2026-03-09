"""Compatibility helpers."""


def require_temporal() -> None:
    """Raise a clear error when ``temporal`` extra is not installed."""

    try:
        import temporalio  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_temporal requires 'forze[temporal]' extra") from e
