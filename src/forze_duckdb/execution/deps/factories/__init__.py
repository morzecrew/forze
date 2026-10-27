"""DuckDB dependency factories."""

from .analytics import ConfigurableDuckDbAnalytics
from .dynamic_read import ConfigurableDuckDbDynamicRead

# ----------------------- #

__all__ = ["ConfigurableDuckDbAnalytics", "ConfigurableDuckDbDynamicRead"]
