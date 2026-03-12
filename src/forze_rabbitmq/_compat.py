"""Compatibility helpers."""


def require_rabbitmq() -> None:
    """Raise a clear error when ``rabbitmq`` extra is not installed."""

    try:
        import aio_pika  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_rabbitmq requires 'forze[rabbitmq]' extra") from e
