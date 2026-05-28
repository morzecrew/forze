"""Compatibility helpers."""


def require_meilisearch() -> None:
    """Raise a clear error when the ``meilisearch`` extra is not installed."""

    try:
        import meilisearch_python_sdk  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_meilisearch requires 'forze[meilisearch]' extra") from e
