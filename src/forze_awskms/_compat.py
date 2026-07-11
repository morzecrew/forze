"""Compatibility helpers for the optional AWS KMS extra."""

import importlib.util


def require_awskms() -> None:
    """Raise a clear error when the ``awskms`` extra is not installed."""

    try:
        import aioboto3  # pyright: ignore[reportUnusedImport]  # noqa: F401
        from botocore.config import (
            Config,  # pyright: ignore[reportUnusedImport]  # noqa: F401
        )
    except ImportError as e:
        raise RuntimeError("forze_awskms requires 'forze[awskms]' extra") from e

    # ``types-aiobotocore-kms`` is a type-only stub (used under ``TYPE_CHECKING``);
    # verify presence without paying its runtime import cost.
    if importlib.util.find_spec("types_aiobotocore_kms") is None:
        raise RuntimeError("forze_awskms requires 'forze[awskms]' extra")
