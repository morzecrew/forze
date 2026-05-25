"""ETag API route (logic is applied in handlers via :func:`etag.response.apply_etag`)."""

from fastapi.routing import APIRoute


class ETagAPIRoute(APIRoute):
    """Marker route class for ETag-enabled endpoints."""
