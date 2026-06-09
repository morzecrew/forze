"""Shipped-in identity presets (config, verifiers, wiring) for demos and common vendors.

Subpackages:

- :mod:`forze_identity.builtin.local` — file/env API-key identity for demos.
- :mod:`forze_identity.builtin.idp` — Google / VK ID / Telegram Login OIDC bootstrap presets.

Distinct from core :mod:`forze_identity.authn` (orchestration and first-party document
auth) and generic :mod:`forze_identity.oidc`. Not intended as production defaults unless
you explicitly accept each preset's trust model.
"""
