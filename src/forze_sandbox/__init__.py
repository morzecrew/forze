"""Isolating sandbox backends for the ``forze.application.contracts.sandbox`` seam.

One submodule per tier, each gated by its own extra (the ``forze_kms`` shape):

- :mod:`forze_sandbox.container` — a container daemon over the Docker Engine API,
  which Podman serves unchanged; extra ``forze[sandbox-container]``.

The tiers that need no infrastructure are not here. ``SubprocessSandbox`` ships in
``forze.application.integrations.sandbox`` because it is stdlib ``asyncio`` and needs
nothing installed, exactly as the local inference adapter does; what lives in this
package is the tiers that need something running.

This top-level package imports nothing at runtime so installing one extra never
requires the others.
"""
