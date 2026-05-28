"""Compatibility helpers."""


def require_inngest() -> None:
    """Raise a clear error when the ``inngest`` extra is not installed."""

    try:
        import inngest  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError("forze_inngest requires 'forze[inngest]' extra") from e


def require_fastapi() -> None:
    """Raise a clear error when the ``fastapi`` extra is not installed."""

    try:
        import fastapi  # pyright: ignore[reportUnusedImport]  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "forze_inngest FastAPI helpers require 'forze[fastapi]' extra",
        ) from e
