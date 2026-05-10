"""Compatibility helpers for the optional OIDC extra."""


def require_oidc() -> None:
    """Raise a clear error when the ``oidc`` extra is not installed."""

    try:
        import jwt  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_oidc requires 'forze[oidc]' extra") from e
