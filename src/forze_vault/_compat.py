"""Compatibility helpers for the optional Vault extra."""


def require_vault() -> None:
    """Raise a clear error when the ``vault`` extra is not installed."""

    try:
        import hvac  # type: ignore[import-untyped]  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_vault requires 'forze[vault]' extra") from e
