"""Preview binding: what you saw is what you submit.

- :class:`PreviewBinding` — one flow, declared once: :meth:`~PreviewBinding.reviewed` for the
  preview, :meth:`~PreviewBinding.bind` for the confirming operation.
- :class:`Reviewed` / :class:`ReviewedCommand` — the preview's result and the confirming
  command's fingerprint field.
- :func:`canonical_fingerprint` — the fingerprint both sides compute, in a canonical form of its
  own that does not move with a dependency upgrade.

Idempotency answers "did I already send this?"; preview binding answers "is this still what I
saw?".
"""

from .binding import (
    FINGERPRINT_PREFIX,
    PREVIEW_CHANGED,
    PreviewBinding,
    PreviewProjector,
    Reviewed,
    ReviewedCommand,
    canonical_fingerprint,
)

# ----------------------- #

__all__ = [
    "FINGERPRINT_PREFIX",
    "PREVIEW_CHANGED",
    "PreviewBinding",
    "PreviewProjector",
    "Reviewed",
    "ReviewedCommand",
    "canonical_fingerprint",
]
