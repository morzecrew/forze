"""Compatibility helpers."""


def require_bigquery() -> None:
    """Raise a clear error when the ``bigquery`` extra is not installed."""

    try:
        import gcloud.aio.bigquery  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_bigquery requires 'forze[bigquery]' extra") from e
