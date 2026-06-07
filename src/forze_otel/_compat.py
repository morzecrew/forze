"""Compatibility helpers."""


def require_otel() -> None:
    """Raise a clear error when the ``otel`` extra is not installed."""

    try:
        import opentelemetry  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_otel requires 'forze[otel]' extra") from e
