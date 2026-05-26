from forze.application.contracts.base import DepKey

from ...kernel.platform import BigQueryClientPort

# ----------------------- #

BigQueryClientDepKey = DepKey[BigQueryClientPort]("bigquery_client")
"""Dependency key for the shared :class:`BigQueryClientPort`."""
