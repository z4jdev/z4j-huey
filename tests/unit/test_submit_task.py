"""Tests for ``HueyEngineAdapter.submit_task``.

The bare-agent dispatcher's v1.1.0 ``schedule.fire`` path routes
brain-side scheduler ticks to ``engine.submit_task(...)``. These tests
pin the contract for the Huey engine: the adapter MUST resolve the
task name through Huey's registry and invoke the task callable, which
returns a Result wrapper carrying the ``id`` of the enqueued message.

Huey is single-queue at the engine level, so the ``queue`` kwarg is
intentionally a no-op. ``eta`` / ``priority`` are not yet wired (a
1.1.x backlog item) but the method must still accept them without
raising.
"""

from __future__ import annotations

import pytest

pytest.importorskip("huey")

from huey import MemoryHuey  # noqa: E402

from z4j_huey import HueyEngineAdapter  # noqa: E402


@pytest.fixture
def huey():
    inst = MemoryHuey("submit-test", immediate=False)

    @inst.task()
    def send_email(to, *, template):
        return (to, template)

    @inst.task()
    def boom():
        raise RuntimeError("nope")

    return inst


@pytest.fixture
def adapter(huey):
    return HueyEngineAdapter(huey=huey)


def _registered_name(huey, suffix: str) -> str:
    """Pull the fully qualified registry name for ``suffix``."""
    keys = list(huey._registry._registry.keys())
    matches = [k for k in keys if k.endswith(suffix)]
    assert matches, f"no registered task ends with {suffix!r}; have {keys}"
    return matches[0]


@pytest.mark.asyncio
class TestSubmitTask:
    async def test_capability_advertised(self, adapter) -> None:
        assert "submit_task" in adapter.capabilities()

    async def test_known_task_enqueues_and_returns_id(
        self, adapter, huey,
    ) -> None:
        name = _registered_name(huey, ".send_email")
        result = await adapter.submit_task(
            name,
            args=("alice@example.com",),
            kwargs={"template": "welcome"},
        )
        assert result.status == "success", f"got {result.error!r}"
        assert result.result["engine"] == "huey"
        assert result.result["task_id"] is not None

    async def test_unknown_task_fails_cleanly(self, adapter) -> None:
        result = await adapter.submit_task("not.a.task")
        assert result.status == "failed"
        assert "unknown huey task" in (result.error or "")

    async def test_queue_kwarg_accepted_no_op(self, adapter, huey) -> None:
        """Huey is single-queue; ``queue`` is intentionally ignored.
        The call must still succeed (no TypeError, no warning).
        """
        name = _registered_name(huey, ".send_email")
        result = await adapter.submit_task(
            name,
            args=("a@example.com",),
            kwargs={"template": "x"},
            queue="ignored-by-huey",
        )
        assert result.status == "success"

    async def test_broker_failure_returns_failed(
        self, adapter, huey,
    ) -> None:
        """If ``huey.enqueue`` raises (e.g. broker network blip), the
        adapter returns a clean failed CommandResult instead of
        bubbling the exception out into the dispatcher.
        """
        name = _registered_name(huey, ".send_email")
        original_enqueue = huey.enqueue

        def boom(_task):
            raise RuntimeError("redis unreachable")

        huey.enqueue = boom  # type: ignore[method-assign]
        try:
            result = await adapter.submit_task(name)
        finally:
            huey.enqueue = original_enqueue  # type: ignore[method-assign]
        assert result.status == "failed"
        assert "redis unreachable" in (result.error or "")

    async def test_eta_passed_through_as_datetime(
        self, adapter, huey,
    ) -> None:
        """``eta`` (epoch seconds) is converted to a tz-aware datetime
        for Huey's Task constructor. Without this z4j-scheduler's
        ``schedule.fire`` couldn't pin a specific fire time."""
        from datetime import UTC, datetime

        name = _registered_name(huey, ".send_email")
        target = datetime(2030, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = await adapter.submit_task(
            name,
            args=("a@example.com",),
            kwargs={"template": "x"},
            eta=target.timestamp(),
        )
        assert result.status == "success", f"got {result.error!r}"
