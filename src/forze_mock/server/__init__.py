"""Serve a real forze app on in-memory backends, with a control plane.

The app's **own** routes, handlers, middleware, identity wiring and error mapping run
unchanged; only the ``DepsRegistry`` is swapped for mock modules. Contract drift is therefore
structurally impossible — there is no second artifact to keep in sync, because the served
routes are generated from the same frozen registry production serves.

    from forze_mock.server import MockApp, serve

    mock_app = MockApp(build_app=build_app, seed=plan)   # `myapp.mock:mock_app`
    serve(mock_app)                                       # or: forze mock serve myapp.mock:mock_app

Two rules shape everything here:

* **Nothing transport- or identity-shaped is owned by this package.** ``forze_mock`` must not
  import a sibling integration package, so routes, middleware and the authn ingress all come
  from the user's ``build_app`` — which means the server has no way to mint a principal.
* **It refuses to serve a real runtime.** ``serve`` needs ``FORZE_MOCK_SERVER=1`` and a
  composition that actually contains a fallback-marked mock module, and the control plane
  takes a :class:`MockSession` that only :func:`build_mock_server` can create.

Requires the ``mock-server`` extra; importing this module without it raises with that
instruction, and ``forze_mock`` itself never imports it.
"""

from forze_mock._compat import require_server

require_server()

# ....................... #

from .clock import ControlledTimeSource
from .control import build_control_app
from .declaration import ControlPlane, MockApp
from .faults import ArmedFault, ArmedLatency, ControlInterceptor, FaultBoard
from .runner import SERVE_ENV_GATE, build_mock_server, serve
from .session import MockSession

# ----------------------- #

__all__ = [
    "SERVE_ENV_GATE",
    "ArmedFault",
    "ArmedLatency",
    "ControlInterceptor",
    "ControlPlane",
    "ControlledTimeSource",
    "FaultBoard",
    "MockApp",
    "MockSession",
    "build_control_app",
    "build_mock_server",
    "serve",
]
