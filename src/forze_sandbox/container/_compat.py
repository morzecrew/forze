"""Compatibility helpers."""


def require_sandbox_container() -> None:
    """Raise a clear error when the ``sandbox-container`` extra is not installed."""

    try:
        import httpx  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "forze_sandbox.container requires 'forze[sandbox-container]' extra"
        ) from e
