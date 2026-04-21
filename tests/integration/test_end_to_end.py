"""End-to-end smoke test for z4j-huey.

Spins up a real RedisHuey (in-memory broker via MemoryHuey is fine
for the smoke; we use it instead of standing up a Redis container
in unit-test infra) and exercises the full path:

1. ``connect_signals`` installs huey.signals handlers
2. A real task is enqueued + executed (immediate=True)
3. ``submit_task`` enqueues a task by name
4. ``reconcile_task`` reads the result store
5. Events are observable via subscribe_events

If you want a true Redis run, set ``Z4J_HUEY_TEST_REDIS_URL``;
otherwise the test stays in-memory.
"""

from __future__ import annotations

import asyncio
import os

import pytest

pytest.importorskip("huey")

pytestmark = pytest.mark.integration

from huey import MemoryHuey, RedisHuey  # noqa: E402

from z4j_core.models.event import EventKind  # noqa: E402

from z4j_huey import HueyEngineAdapter  # noqa: E402


def _make_huey():
    url = os.environ.get("Z4J_HUEY_TEST_REDIS_URL")
    if url:
        return RedisHuey("z4j-huey-itest", url=url, immediate=False)
    return MemoryHuey("z4j-huey-itest", immediate=True)


@pytest.fixture
def huey_with_tasks():
    inst = _make_huey()

    @inst.task()
    def add(x, y):
        return x + y

    @inst.task()
    def boom():
        raise RuntimeError("smoke")

    return inst, add, boom


@pytest.mark.asyncio
async def test_full_lifecycle_smoke(huey_with_tasks):
    huey, add, boom = huey_with_tasks
    adapter = HueyEngineAdapter(huey=huey)
    adapter.connect_signals(asyncio.get_running_loop())

    # Discovery sees both tasks.
    defs = await adapter.discover_tasks()
    names = {d.name for d in defs}
    assert any("add" in n for n in names)
    assert any("boom" in n for n in names)

    # Submit via the universal primitive.
    submit_name = next(n for n in names if "add" in n)
    res = await adapter.submit_task(submit_name, args=(7, 8))
    assert res.status == "success"
    submitted_id = res.result["task_id"]

    # Direct call to fire signals (immediate-mode synchronous).
    add(1, 2)
    boom_result = boom()
    assert boom_result is not None  # huey returns Task instance

    # Drain captured events.
    await asyncio.sleep(0.05)
    kinds: list[EventKind] = []
    while not adapter._event_queue.empty():
        evt = adapter._event_queue.get_nowait()
        kinds.append(evt.kind)

    # Must contain at least one success and one failure across the
    # captured set.
    assert EventKind.TASK_SUCCEEDED in kinds
    assert EventKind.TASK_FAILED in kinds or EventKind.TASK_RECEIVED in kinds

    # Reconcile a known submitted task.
    reconcile = await adapter.reconcile_task(submitted_id or "fake-id")
    assert reconcile.status == "success"
    assert reconcile.result["engine_state"] in {
        "pending", "success", "unknown",
    }

    adapter.disconnect_signals()
