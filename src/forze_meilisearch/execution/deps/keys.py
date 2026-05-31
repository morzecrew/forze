"""Meilisearch dependency keys."""

from forze.application.contracts.deps import DepKey
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

MeilisearchClientDepKey = DepKey[MeilisearchClientPort]("meilisearch_client")

__all__ = ["MeilisearchClientDepKey"]
