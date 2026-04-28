"""End-to-end dispatcher integration: real Huey engine + bare dispatcher."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("huey")

from huey import MemoryHuey  # noqa: E402

from z4j_bare.buffer import BufferStore  # noqa: E402
from z4j_bare.dispatcher import CommandDispatcher  # noqa: E402
from z4j_core.transport.frames import CommandFrame, CommandPayload  # noqa: E402

from z4j_huey import HueyEngineAdapter  # noqa: E402


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.fixture
def huey():
    inst = MemoryHuey("dispatch-test", immediate=False)

    @inst.task()
    def send_email(to, *, template):
        return (to, template)

    return inst


@pytest.mark.asyncio
async def test_schedule_fire_end_to_end_through_dispatcher(
    huey, buf: BufferStore,
) -> None:
    engine = HueyEngineAdapter(huey=huey)
    dispatcher = CommandDispatcher(
        engines={"huey": engine},
        schedulers={},
        buffer=buf,
    )

    name = next(
        k for k in huey._registry._registry if k.endswith(".send_email")
    )

    frame = CommandFrame(
        id="cmd_e2e_huey_01",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "schedule_id": "s1",
                "schedule_name": "nightly-emails",
                "task_name": name,
                "engine": "huey",
                "args": ["alice@example.com"],
                "kwargs": {"template": "welcome"},
                "fire_id": "f1",
            },
        ),
        hmac="deadbeef" * 8,
    )

    await dispatcher.handle(frame)

    results = [e for e in buf.drain(10) if e.kind == "command_result"]
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success", (
        f"got {parsed['payload'].get('error')!r}"
    )
    assert parsed["payload"]["result"]["engine"] == "huey"
    assert parsed["payload"]["result"]["task_id"] is not None
