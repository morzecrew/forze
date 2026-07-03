"""Shared payload model for forze_kafka integration tests."""

from pydantic import BaseModel

# ----------------------- #


class Payload(BaseModel):
    value: str
