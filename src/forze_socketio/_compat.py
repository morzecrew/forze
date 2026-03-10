"""Compatibility helpers."""


def require_socketio() -> None:
    """Raise a clear error when ``socketio`` extra is not installed."""

    try:
        import socketio  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_socketio requires 'forze[socketio]' extra") from e
