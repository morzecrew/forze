"""Compatibility helpers."""


def require_kafka() -> None:
    """Raise a clear error when ``kafka`` extra is not installed."""

    try:
        import aiokafka  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_kafka requires 'forze[kafka]' extra") from e
