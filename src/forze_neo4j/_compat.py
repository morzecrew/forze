"""Compatibility helpers."""


def require_neo4j() -> None:
    """Raise a clear error when the ``neo4j`` extra is not installed."""

    try:
        import neo4j  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_neo4j requires 'forze[neo4j]' extra") from e
