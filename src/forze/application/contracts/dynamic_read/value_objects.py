"""Value objects for the governed dynamic-read plane."""

from typing import Literal

# ----------------------- #

DynamicReadProvenance = Literal["trusted", "untrusted"]
"""Who authored the statement text — the single most important wiring input on this plane.

The framework cannot read the statement, so it cannot infer this; the wiring author declares
it, and the declaration decides what confinement the wiring guard demands.

- ``trusted`` — the app's own release artifacts, selected at runtime: a shared visualization
  catalog, a semantic compiler's output from reviewed templates. Confined by the engine's
  read-only transaction, namespace routing and the route's limits.
- ``untrusted`` — a program whose output is not reviewed per statement and is not *crafted to
  escape*: a generator working from our templates, user-configurable report definitions. The
  wiring guard additionally demands a schema-confined role or a routed (dedicated) client.

There is deliberately no third value for an **adversarial** author — someone who may construct
escape gadgets on purpose. Nothing distinguishes that case mechanically from ``untrusted`` on a
shared connection: the statement and the adapter wield the same identity, so any privilege the
adapter can invoke mid-session a hostile statement can invoke too. The only scoping key a
statement cannot forge from inside the session is the connection's login identity, which makes
"adversarial" an operator's choice of dedicated topology rather than a config value. The
framework has the right not to know a better answer; it does not have the right to imply one
exists.
"""
