"""A wiring module whose deps module refuses its configuration while it imports.

``forze schema --deps`` must report this as a configuration error, exit 78, not a traceback.
"""

from datetime import timedelta

from forze_postgres import PostgresClient, PostgresDepsModule

# ----------------------- #

deps = PostgresDepsModule(client=PostgresClient(), introspector_cache_ttl=timedelta(0))
