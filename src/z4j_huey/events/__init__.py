"""Huey event capture - wires ``huey.signals`` into the agent's
event queue.

Public API:
- :func:`install` registers signal handlers on a Huey instance.
- :func:`uninstall` removes them.
- :class:`HueyEventCapture` holds the asyncio.Queue + signal sink.
"""

from __future__ import annotations

from z4j_huey.events.capture import HueyEventCapture, install, uninstall

__all__ = ["HueyEventCapture", "install", "uninstall"]
