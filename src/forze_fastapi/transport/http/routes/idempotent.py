"""Idempotent API route (logic is applied in handlers via :func:`idempotency.runner.run_idempotent`)."""

from fastapi.routing import APIRoute


class IdempotentAPIRoute(APIRoute):
    """Marker route class for idempotent endpoints."""
