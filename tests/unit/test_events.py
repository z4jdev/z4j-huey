"""Huey event capture via signal handlers."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("huey")

from huey import MemoryHuey  # noqa: E402

from z4j_core.models.event import EventKind  # noqa: E402

from z4j_huey import HueyEngineAdapter  # noqa: E402
from z4j_huey.events import install as install_capture  # noqa: E402


@pytest.fixture
def huey_with_task():
    """Returns ``(huey_instance, add_wrapped_callable)`` - the
    decorator-returned callable is what fires huey signals on
    invocation. Looking up the entry from ``_registry`` gives the
    underlying Task class which doesn't trigger signals when called.
    """
    inst = MemoryHuey("evt-test", immediate=True)

    @inst.task()
    def add(x, y):
        return x + y

    return inst, add


@pytest.fixture
def huey(huey_with_task):
    return huey_with_task[0]


@pytest.fixture
def add_task(huey_with_task):
    return huey_with_task[1]


@pytest.mark.asyncio
async def test_signal_handler_emits_complete_event(huey, add_task):
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    loop = asyncio.get_running_loop()
    capture = install_capture(huey=huey, queue=queue, loop=loop)
    try:
        # Trigger via the decorator-wrapped callable so signals fire.
        add_task(1, 2)
        # Allow the signal handler thread → loop hop to drain.
        await asyncio.sleep(0.05)
        kinds: list[EventKind] = []
        while not queue.empty():
            evt = queue.get_nowait()
            kinds.append(evt.kind)
        assert kinds, "no events captured"
        assert EventKind.TASK_SUCCEEDED in kinds
        assert EventKind.TASK_RECEIVED in kinds
    finally:
        capture.uninstall()


@pytest.mark.asyncio
async def test_uninstall_is_idempotent(huey):
    queue: asyncio.Queue = asyncio.Queue(maxsize=10)
    loop = asyncio.get_running_loop()
    capture = install_capture(huey=huey, queue=queue, loop=loop)
    capture.uninstall()
    # Second call must not raise.
    capture.uninstall()


@pytest.mark.asyncio
async def test_engine_connect_signals_round_trip(huey, add_task):
    adapter = HueyEngineAdapter(huey=huey)
    adapter.connect_signals(asyncio.get_running_loop())
    add_task(3, 4)
    await asyncio.sleep(0.05)
    async def _take_one():
        async for evt in adapter.subscribe_events():
            return evt
    evt = await asyncio.wait_for(_take_one(), timeout=1.0)
    assert evt.engine == "huey"
    adapter.disconnect_signals()
