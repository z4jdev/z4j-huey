"""Huey signal-handler installer + event mapper.

Huey 2.x / 3.x exposes a ``@huey.signal()`` decorator that fires
on every task lifecycle event. Signal handlers run synchronously
on the consumer thread, so we marshal events back to the agent's
asyncio loop via ``loop.call_soon_threadsafe``.

Mapping (huey.signals.SIGNAL_X → z4j EventKind):

| Huey signal       | EventKind        |
|-------------------|------------------|
| SIGNAL_ENQUEUED   | TASK_RECEIVED    |
| SIGNAL_SCHEDULED  | TASK_RECEIVED    |
| SIGNAL_EXECUTING  | TASK_STARTED     |
| SIGNAL_COMPLETE   | TASK_SUCCEEDED   |
| SIGNAL_ERROR      | TASK_FAILED      |
| SIGNAL_RETRYING   | TASK_RETRIED     |
| SIGNAL_REVOKED    | TASK_REVOKED     |
| SIGNAL_CANCELED   | TASK_REVOKED     |
| SIGNAL_INTERRUPTED| TASK_FAILED      |
| SIGNAL_EXPIRED    | TASK_FAILED      |
| SIGNAL_LOCKED     | (skipped - too noisy at info level) |
| SIGNAL_RATE_LIMITED | (skipped) |
| SIGNAL_TIMEOUT    | TASK_FAILED      |
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from z4j_core.models import Event
from z4j_core.models.event import EventKind

logger = logging.getLogger("z4j.adapter.huey.events")

ENGINE_NAME = "huey"


class HueyEventCapture:
    """Holds the queue + the loop ref so signal handlers (which run
    on the consumer thread) can hop back to the agent's asyncio
    loop via ``call_soon_threadsafe``.
    """

    __slots__ = ("queue", "_loop", "_redaction", "_huey", "_handlers")

    def __init__(
        self,
        *,
        queue: asyncio.Queue[Event],
        loop: asyncio.AbstractEventLoop,
        huey: Any,
        redaction: Any | None = None,
    ) -> None:
        self.queue = queue
        self._loop = loop
        self._huey = huey
        self._redaction = redaction
        self._handlers: list[Any] = []

    def install(self) -> None:
        """Register handlers on the Huey instance for every relevant signal."""
        from huey import signals

        # Each key is a huey signal-constant NAME; we resolve via getattr
        # so the code stays compatible across huey versions that add or
        # drop signal kinds. Unknown names are skipped silently.
        wanted = {
            "SIGNAL_ENQUEUED":    EventKind.TASK_RECEIVED,
            "SIGNAL_SCHEDULED":   EventKind.TASK_RECEIVED,
            "SIGNAL_EXECUTING":   EventKind.TASK_STARTED,
            "SIGNAL_COMPLETE":    EventKind.TASK_SUCCEEDED,
            "SIGNAL_ERROR":       EventKind.TASK_FAILED,
            "SIGNAL_RETRYING":    EventKind.TASK_RETRIED,
            "SIGNAL_REVOKED":     EventKind.TASK_REVOKED,
            "SIGNAL_CANCELED":    EventKind.TASK_REVOKED,
            "SIGNAL_INTERRUPTED": EventKind.TASK_FAILED,
            "SIGNAL_EXPIRED":     EventKind.TASK_FAILED,
            "SIGNAL_LOCKED":      EventKind.TASK_FAILED,
        }
        mapping = {
            sig: ek
            for name, ek in wanted.items()
            if (sig := getattr(signals, name, None)) is not None
        }

        for signal_kind, event_kind in mapping.items():
            handler = self._make_handler(signal_kind, event_kind)
            # Huey's signal decorator returns the handler unchanged
            # but registers it on the instance.
            self._huey.signal(signal_kind)(handler)
            self._handlers.append((signal_kind, handler))

    def uninstall(self) -> None:
        """Remove every handler we registered."""
        # Huey's signal infrastructure is dict-of-list keyed by
        # signal kind. Defensive lookup so a future shape change
        # doesn't crash the agent on shutdown.
        sigs = getattr(self._huey, "_signal", None)
        if sigs is None:
            return
        registry = getattr(sigs, "_signals", None)
        if registry is None:
            return
        for signal_kind, handler in self._handlers:
            for_kind = registry.get(signal_kind, [])
            try:
                for_kind.remove(handler)
            except ValueError:
                pass
        self._handlers.clear()

    def _make_handler(self, signal_kind: str, event_kind: EventKind):
        capture = self

        def _handler(_signal: str, task: Any, exc: Exception | None = None) -> None:
            try:
                evt = capture._build_event(event_kind, task, exc)
            except Exception:  # noqa: BLE001
                logger.exception("z4j-huey: failed to build event")
                return
            try:
                capture._loop.call_soon_threadsafe(
                    capture.queue.put_nowait, evt,
                )
            except RuntimeError:
                # Loop already closed - drop silently. The host
                # process is shutting down.
                logger.debug(
                    "z4j-huey: event loop closed; dropping %s", signal_kind,
                )
            except asyncio.QueueFull:
                logger.warning(
                    "z4j-huey: event queue full; dropping %s for %s",
                    signal_kind, getattr(task, "id", "?"),
                )

        return _handler

    def _build_event(
        self, kind: EventKind, task: Any, exc: Exception | None,
    ) -> Event:
        now = datetime.now(UTC)
        task_id = str(getattr(task, "id", "") or "")
        task_name = getattr(task, "name", None) or type(task).__name__
        data: dict[str, Any] = {
            "task_name": task_name,
        }
        if exc is not None:
            data["exception"] = f"{type(exc).__name__}: {exc}"
        if kind == EventKind.TASK_RECEIVED:
            # Huey passes the task instance with .args + .kwargs at
            # this point. Redact via the engine's RedactionEngine.
            args = getattr(task, "args", ()) or ()
            kwargs = getattr(task, "kwargs", {}) or {}
            if self._redaction is not None:
                try:
                    args = self._redaction.redact_args(args)
                    kwargs = self._redaction.redact_kwargs(kwargs)
                except Exception:  # noqa: BLE001
                    args, kwargs = [], {}
            data["args"] = list(args)
            data["kwargs"] = kwargs
        return Event(
            id=uuid4(),
            project_id=uuid4(),  # placeholder; brain re-stamps from agent
            agent_id=uuid4(),    # placeholder; brain re-stamps from session
            engine=ENGINE_NAME,
            task_id=task_id,
            kind=kind,
            occurred_at=now,
            data=data,
        )


def install(
    *,
    huey: Any,
    queue: asyncio.Queue[Event],
    loop: asyncio.AbstractEventLoop,
    redaction: Any | None = None,
) -> HueyEventCapture:
    """One-shot install. Returns the :class:`HueyEventCapture` so
    the caller can ``uninstall()`` on agent shutdown.
    """
    capture = HueyEventCapture(
        queue=queue, loop=loop, huey=huey, redaction=redaction,
    )
    capture.install()
    return capture


def uninstall(capture: HueyEventCapture) -> None:
    capture.uninstall()


__all__ = ["HueyEventCapture", "install", "uninstall"]
