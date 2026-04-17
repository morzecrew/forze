"""Compatibility helpers."""


def require_simple_auth() -> None:
    """Raise a clear error when ``simple-auth`` extra is not installed."""

    try:
        import argon2  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import jwt  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "forze_simple_auth requires 'forze[simple-auth]' extra"
        ) from e
