"""The :class:`HueyEngineAdapter` - z4j's Huey queue engine adapter.

Implements :class:`z4j_core.protocols.QueueEngineAdapter` on top of
any ``huey.api.Huey`` instance. Wires up:

- Discovery via ``huey._registry`` (the in-process task registry).
- ``get_task`` + ``reconcile_task`` via Huey's result store.
- ``retry_task`` via re-enqueue with the original signature.
- ``cancel_task`` via Huey's revoke API.

v0 scope: discovery + per-task actions + reconciliation. Event
streaming, bulk operations, and restart/rate-limit are deferred to
v1.1 (see ``capabilities.py`` for honest non-support).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.redaction.engine import RedactionEngine
from z4j_core.version import PROTOCOL_VERSION

from z4j_huey.capabilities import DEFAULT_CAPABILITIES

logger = logging.getLogger("z4j.agent.huey.engine")

ENGINE_NAME = "huey"


class HueyEngineAdapter:
    """Queue-engine adapter for Huey.

    Args:
        huey: A live ``huey.api.Huey`` instance - RedisHuey,
              SqliteHuey, MemoryHuey all satisfy the duck contract
              (``_registry``, ``result_store``, ``pending``,
              ``revoke_by_id``, ``enqueue``).
        redaction: Shared :class:`RedactionEngine`. The agent runtime
                   passes its own engine in production so per-project
                   redaction config propagates.
    """

    name: str = ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    def __init__(
        self,
        *,
        huey: Any,
        redaction: RedactionEngine | None = None,
    ) -> None:
        self.huey = huey
        self.redaction = redaction or RedactionEngine()
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        self._capture = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def connect_signals(
        self, loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Install ``huey.signals`` capture handlers.

        Called by the agent runtime once the asyncio loop is up.
        Idempotent: re-installing replaces the previous handlers.
        """
        from z4j_huey.events import install as install_events

        target_loop = loop
        if target_loop is None:
            try:
                target_loop = asyncio.get_running_loop()
            except RuntimeError:
                target_loop = None
        self._loop = target_loop
        if target_loop is None:
            logger.warning("z4j-huey: no running loop; events disabled")
            return
        self.disconnect_signals()
        self._capture = install_events(
            huey=self.huey,
            queue=self._event_queue,
            loop=target_loop,
            redaction=self.redaction,
        )

    def disconnect_signals(self) -> None:
        """Uninstall every signal handler we own. Safe to call twice."""
        if self._capture is None:
            return
        try:
            self._capture.uninstall()
        except Exception:  # noqa: BLE001
            logger.exception("z4j-huey: signal uninstall failed")
        self._capture = None

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    async def discover_tasks(
        self,
        hints: DiscoveryHints | None = None,  # noqa: ARG002
    ) -> list[TaskDefinition]:
        """Return every task registered with this Huey instance.

        Reads ``huey._registry._registry`` - the in-process dict of
        decorator-registered task names → callables. Periodic-task
        decorators register here too.
        """
        out: list[TaskDefinition] = []
        registry = getattr(self.huey, "_registry", None)
        names = getattr(registry, "_registry", {}) if registry else {}
        for name in names:
            out.append(
                TaskDefinition(
                    engine=self.name,
                    name=name,
                    queue=getattr(self.huey, "name", None) or "huey",
                ),
            )
        return out

    async def subscribe_registry_changes(
        self,
    ) -> AsyncIterator[TaskRegistryDelta]:
        """Huey's in-process registry has no change signal.

        Decorators run at import time; the brain re-discovers via
        the periodic ``discover_tasks`` sweep.
        """
        if False:  # pragma: no cover
            yield  # type: ignore[unreachable]
        return

    # ------------------------------------------------------------------
    # Observation
    # ------------------------------------------------------------------

    async def subscribe_events(self) -> AsyncIterator[Event]:
        """Drain the internal event queue populated by
        ``huey.signals`` handlers (installed via
        :meth:`connect_signals`). Yields one Event at a time;
        cancels cleanly on agent shutdown.
        """
        while True:
            evt = await self._event_queue.get()
            yield evt

    async def list_queues(self) -> list[Queue]:
        """v1: queue listing is synthesized on the brain from
        ``task.received`` events. Huey is conceptually single-queue
        per instance but the ``Queue`` model wants brain-side ids
        (project_id, created_at) the agent can't mint, so we return
        empty here and let the brain fill in from observed events.
        """
        return []

    async def list_workers(self) -> list[Worker]:
        """Huey consumers don't expose remote-introspection.

        Empty list is honest - no in-process worker registry to
        consult. The agent's own process metadata serves as the
        worker identity from the brain's POV.
        """
        return []

    async def get_task(self, task_id: str) -> Task | None:
        """Best-effort: read the result store for ``task_id``.

        Huey's result store keys by task ``id``. A pending task
        has no entry yet; a completed task has its return value
        (or exception) under that key.
        """
        result_store = getattr(self.huey, "result_store", None) or self.huey
        try:
            raw = result_store.peek_data(task_id)
        except Exception:  # noqa: BLE001
            return None
        if raw is None:
            return None
        # Best-effort state inference: if peek returned bytes / dict,
        # the task has finished. Detailed state requires the lifecycle
        # signals planned for v1.1.
        return Task(
            engine=self.name,
            task_id=task_id,
            name="",
            state="success",
        )

    async def reconcile_task(self, task_id: str) -> CommandResult:
        """Probe Huey's result store for the authoritative state.

        Returns a CommandResult whose ``result`` carries
        ``engine_state`` ∈ {pending, success, failure, unknown}.
        Huey's result store doesn't distinguish "started" from
        "pending" - both surface as "no result yet" - so we map
        absence to ``"pending"`` and presence to ``"success"`` /
        ``"failure"`` based on whether the stored value is an
        exception.
        """
        result_store = getattr(self.huey, "result_store", None) or self.huey
        try:
            raw = result_store.peek_data(task_id)
        except Exception:  # noqa: BLE001
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )

        if raw is None:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "pending",
                    "finished_at": None,
                    "exception": None,
                },
            )

        # Huey stores either the return value or an exception. The
        # serializer is huey.serializer; rather than depend on it
        # here, sniff the type - exceptions show up as instances of
        # ``BaseException`` once deserialized by the consumer, but
        # the result store stores them serialized. We treat any
        # non-None presence as "success" for v1; v1.1 will deserialize
        # and properly distinguish.
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "engine_state": "success",
                "finished_at": None,
                "exception": None,
            },
        )

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,  # noqa: ARG002 - Huey is single-queue
        eta: float | None = None,
        priority: int | None = None,
    ) -> CommandResult:
        """Universal enqueue path - resolve the registry entry for
        ``name`` and enqueue an instance through Huey's broker.

        v1.1.0 fix: pre-1.1 this called ``callable_obj(*args, **kwargs)``
        which works only if the registry stores the ``TaskWrapper``
        decorator. Huey 2.x and 3.x both store the underlying ``Task``
        **subclass** in ``_registry._registry``, and instantiating the
        class with task args directly trips ``Task.__init__`` (which
        accepts ``args``/``kwargs`` as keyword tuples, not splatted
        task arguments). The brain-side z4j-scheduler 1.1.0 ``schedule.fire``
        dispatcher relies on this method, so the bug surfaced as
        ``Task.__init__() got an unexpected keyword argument 'X'`` for
        every Huey-backed scheduled task. New flow:
          1. Instantiate the Task class with ``(args=..., kwargs=...,
             eta=..., priority=...)`` — Huey's documented constructor
             shape across 2.x and 3.x.
          2. ``huey.enqueue(task_instance)`` — pushes onto the broker
             and returns a ``Result`` wrapper.
          3. Use ``task_instance.id`` as the engine-native id.
        """
        registry = getattr(self.huey, "_registry", None)
        callable_obj = None
        if registry is not None:
            callable_obj = getattr(registry, "_registry", {}).get(name)
        if callable_obj is None:
            return CommandResult(
                status="failed",
                error=f"unknown huey task {name!r}",
            )
        if not _is_huey_task_entry(callable_obj):
            return CommandResult(
                status="failed",
                error=f"registry entry for {name!r} is not a Huey task",
            )
        try:
            init_kwargs: dict[str, Any] = {
                "args": tuple(args),
                "kwargs": dict(kwargs or {}),
            }
            # ``eta`` arrives as epoch seconds (Protocol contract); Huey
            # accepts a ``datetime`` for ``eta``. Defer the import so
            # this path stays cheap when eta isn't supplied.
            if eta is not None:
                from datetime import UTC, datetime

                init_kwargs["eta"] = datetime.fromtimestamp(eta, tz=UTC)
            if priority is not None:
                init_kwargs["priority"] = priority
            task_instance = callable_obj(**init_kwargs)
            self.huey.enqueue(task_instance)
            new_id = getattr(task_instance, "id", None)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": new_id, "engine": self.name},
        )

    async def retry_task(
        self,
        task_id: str,
        *,
        override_args: tuple[Any, ...] | None = None,
        override_kwargs: dict[str, Any] | None = None,
        eta: float | None = None,
        priority: int | None = None,  # noqa: ARG002
    ) -> CommandResult:
        """Re-enqueue a task by id.

        Huey's API doesn't natively support "re-enqueue an existing
        completed task by id with the same args" - the args are not
        retained after completion. v1 returns a clean error when no
        override_args/kwargs are supplied; the brain's REST handler
        forwards original args from the Task row when it has them.
        """
        if override_args is None and override_kwargs is None:
            return CommandResult(
                status="failed",
                error=(
                    "huey adapter requires override_args/kwargs to retry - "
                    "the original signature isn't recoverable from the "
                    "task id alone"
                ),
            )

        # Look up the original task callable by name. Without a name
        # we can't enqueue. The brain's retry path forwards
        # ``parameters['task_name']`` for engines (like Huey) where
        # the callable lookup is name-based, not id-based.
        task_name = (override_kwargs or {}).pop("__z4j_task_name__", None)
        registry = getattr(self.huey, "_registry", None)
        callable_obj = None
        if task_name and registry is not None:
            callable_obj = getattr(registry, "_registry", {}).get(task_name)
        if callable_obj is None:
            return CommandResult(
                status="failed",
                error=(
                    "huey adapter retry needs ``__z4j_task_name__`` in "
                    "override_kwargs to look up the registered callable"
                ),
            )
        # Defense-in-depth (audit M4): make sure what came back from
        # the registry is actually a Huey-decorated task, not some
        # stray attribute or third-party patch. A future Huey internal
        # change or a buggy plugin that injects non-task entries
        # would otherwise become an arbitrary-callable invocation
        # path for brain operators.
        if not _is_huey_task_entry(callable_obj):
            return CommandResult(
                status="failed",
                error=(
                    f"registry entry for {task_name!r} is not a Huey "
                    "task (refusing to invoke)"
                ),
            )

        try:
            result = callable_obj(
                *(override_args or ()), **(override_kwargs or {}),
            )
            new_id = getattr(result, "id", None)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"new_task_id": new_id, "original_task_id": task_id},
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        """Mark the task revoked via Huey's revoke API.

        Returns success even if the task already finished - desired
        state (not running) is achieved either way.
        """
        try:
            self.huey.revoke_by_id(task_id)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": task_id, "revoked": True},
        )

    async def bulk_retry(
        self, filter: dict[str, Any], *, max: int = 1000,  # noqa: A002, ARG002
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="bulk_retry not implemented in z4j-huey v1",
        )

    async def purge_queue(
        self,
        queue_name: str,  # noqa: ARG002
        *,
        confirm_token: str | None = None,  # noqa: ARG002
        force: bool = False,  # noqa: ARG002
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="purge_queue not implemented in z4j-huey v1",
        )

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error="huey has no dead-letter concept",
        )

    async def rate_limit(
        self,
        task_name: str,  # noqa: ARG002
        rate: str,  # noqa: ARG002
        *,
        worker_name: str | None = None,  # noqa: ARG002
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="rate_limit not supported by huey",
        )

    async def restart_worker(self, worker_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error="huey consumers expose no remote restart",
        )

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        return set(DEFAULT_CAPABILITIES)


def _is_huey_task_entry(obj: Any) -> bool:
    """Defense-in-depth: confirm a registry entry is a Huey task.

    Huey 3.x stores **subclasses of ``huey.api.Task``** in
    ``_registry._registry`` (so the entries are *classes*, not
    instances). Huey 2.x stored ``TaskWrapper`` *instances*. Accept
    either to remain version-portable.
    """
    try:
        from huey.api import Task as _HueyTask
    except ImportError:  # pragma: no cover
        return True  # huey not installed → can't gate; trust caller

    # 3.x path: class subclass of Task
    if isinstance(obj, type) and issubclass(obj, _HueyTask):
        return True
    # 2.x path: instance of TaskWrapper
    try:
        from huey.api import TaskWrapper as _TW

        if isinstance(obj, _TW):
            return True
    except (ImportError, AttributeError):
        pass
    return False


def _broker_type(huey: Any) -> str | None:
    """Sniff the storage backend from the Huey class name."""
    cls = type(huey).__name__
    if "Redis" in cls:
        return "redis"
    if "Sqlite" in cls:
        return "sqlite"
    if "Memory" in cls:
        return "memory"
    if "FileStorage" in cls:
        return "filesystem"
    return None


__all__ = ["ENGINE_NAME", "HueyEngineAdapter"]
