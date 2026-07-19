"""Compatibility helpers."""


def require_inference_http() -> None:
    """Raise a clear error when the ``inference-http`` extra is not installed."""

    try:
        import httpx  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_inference.http requires 'forze[inference-http]' extra") from e
