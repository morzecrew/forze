from forze.application.contracts.deps import DepKey

from ...kernel.client import InngestClientPort

# ----------------------- #

InngestClientDepKey = DepKey[InngestClientPort]("inngest_client")
"""Key used to register the :class:`~forze_inngest.kernel.client.InngestClientPort`."""
