"""Shared logging test helpers."""

import logging

import structlog

from forze.base.logging.logger import set_configured_min_rank


def reset_forze_stdlib_loggers(*extra_names: str) -> None:
    """Reset structlog defaults, forze's level gates, and every ``forze*`` (plus *extra_names*) stdlib logger.

    ``configure_logging`` / ``bootstrap_logging`` mutate global stdlib logger state
    (handlers, ``propagate=False``, level); left in place it leaks across tests — a parent
    configured with ``propagate=False`` then swallows a later test's records. Call in
    teardown to restore a clean baseline.
    """

    structlog.reset_defaults()
    set_configured_min_rank("info")

    targets = [n for n in logging.root.manager.loggerDict if n.startswith("forze")]
    targets.extend(extra_names)

    for name in targets:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(logging.NOTSET)
