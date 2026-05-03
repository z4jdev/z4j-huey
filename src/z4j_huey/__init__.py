"""z4j-huey - Huey queue engine adapter for z4j.

Public API:

- :class:`HueyEngineAdapter` - adapter implementing the
  :class:`z4j_core.protocols.QueueEngineAdapter` Protocol against
  any ``huey.api.Huey`` instance (RedisHuey, SqliteHuey, MemoryHuey).

Apache 2.0.
"""

from __future__ import annotations

from z4j_huey.engine import HueyEngineAdapter

__version__ = "1.4.0"

__all__ = ["HueyEngineAdapter", "__version__"]
