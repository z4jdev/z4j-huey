"""Capability tokens advertised by the Huey engine adapter."""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        "retry_task",
        "cancel_task",
    },
)
"""Actions implemented in :class:`z4j_huey.engine.HueyEngineAdapter`.

Honest absences (NOT in this set):

- ``bulk_retry`` - Huey's storage layer doesn't enumerate completed
  jobs by filter; deferred to v1.1.
- ``purge_queue`` - same; ``huey.flush()`` exists but only nukes
  the entire instance, not one queue.
- ``requeue_dead_letter`` - Huey has no DLQ concept.
- ``restart_worker`` / ``rate_limit`` - Huey consumers expose no
  remote-control channel.
"""


__all__ = ["DEFAULT_CAPABILITIES"]
