"""Compatibility helpers."""


def require_firestore() -> None:
    """Raise a clear error when ``firestore`` extra is not installed."""

    try:
        import google.cloud.firestore  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_firestore requires 'forze[firestore]' extra") from e
