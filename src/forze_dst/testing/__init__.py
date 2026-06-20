"""Run DST inside your normal pytest suite.

The headline is one function: point :func:`assert_no_violation` at a :class:`~forze_dst.Simulation`
and it sweeps, shrinks on failure, and fails the test with the minimized counterexample — DST
stops being "a thing you remember to run" and becomes a test like any other::

    from forze_dst.testing import assert_no_violation

    def test_payments_have_no_race():
        assert_no_violation(payments_simulation)

No plugin is required for that. For ``--dst-seeds`` scaling, ``--dst-save-bundle`` artifacts, and
the ``dst`` marker, opt into :mod:`forze_dst.testing.plugin` from your ``conftest.py``.

A failing test can drop a portable bundle (``--dst-save-bundle=DIR``); :func:`assert_no_regressions`
replays a directory of those bundles against a Simulation, so a locked-in bug stays fixed.
"""

from __future__ import annotations

from forze_dst.testing.assertions import assert_no_regressions, assert_no_violation

__all__ = ["assert_no_violation", "assert_no_regressions"]
