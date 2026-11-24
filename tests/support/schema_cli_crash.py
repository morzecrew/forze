"""A wiring module that fails while it imports with a bug, not a configuration error.

``forze schema --deps`` must let it surface as the internal error it is.
"""

from forze.base.exceptions import CoreException

# ----------------------- #

raise CoreException.internal("a bug in the wiring module")
