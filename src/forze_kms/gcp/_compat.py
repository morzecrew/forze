"""Compatibility helpers for the optional GCP KMS extra."""


def require_kms_gcp() -> None:
    """Raise a clear error when the ``kms-gcp`` extra is not installed."""

    try:
        import grpc  # pyright: ignore[reportUnusedImport]  # noqa: F401
        from google.cloud import (
            kms,  # pyright: ignore[reportUnusedImport]  # noqa: F401
        )
    except ImportError as e:
        raise RuntimeError("forze_kms.gcp requires 'forze[kms-gcp]' extra") from e
