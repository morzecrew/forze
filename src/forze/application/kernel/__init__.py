"""Core application kernel primitives.

This package defines the core contracts and infrastructure used by the
application layer:

* :mod:`forze.application.kernel.usecase` – base usecase abstractions.
* :mod:`forze.application.kernel.plan` – composition plan for guards/effects.
* :mod:`forze.application.kernel.registry` – registry for usecase factories.
* :mod:`forze.application.kernel.dependencies` – dependency resolution helpers.
* :mod:`forze.application.kernel.ports` – abstract ports to infrastructure.
* :mod:`forze.application.kernel.specs` – specifications for domain resources.

The package is intentionally light on concrete behavior and focuses on
describing how the application talks to the outside world.
"""

