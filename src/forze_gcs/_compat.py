"""Compatibility helpers."""


def require_gcs() -> None:
    """Raise a clear error when ``gcs`` extra is not installed."""

    try:
        import gcloud.aio.storage  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import magic  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_gcs requires 'forze[gcs]' extra") from e
