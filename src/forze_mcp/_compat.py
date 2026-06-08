"""Dependency guard for the optional ``mcp`` extra."""


def require_mcp() -> None:
    """Raise a clear error when the ``mcp`` extra is not installed."""

    try:
        import fastmcp  # noqa: F401  # pyright: ignore[reportUnusedImport]

    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise RuntimeError(
            "forze_mcp requires the 'forze[mcp]' extra (the 'fastmcp' package)."
        ) from e
