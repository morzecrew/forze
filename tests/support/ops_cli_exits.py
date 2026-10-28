"""An application module that exits while it imports, as a misconfigured one may.

Shell completion loads it like any declaration and must offer nothing, quietly.
"""

import sys

sys.exit("refusing to start: no configuration")
