"""Application contracts: ports, dependency keys, and specifications.

Defines interfaces (ports) for document storage, counters, transactions,
storage, streams, durable orchestration (workflow and function), idempotency,
query DSL, and graph modules. Durable contracts live under
``forze.application.contracts.durable.workflow`` and
``forze.application.contracts.durable.function``.
Dependency keys and routers live in :mod:`deps`; domain-specific ports in
their respective subpackages.
"""
