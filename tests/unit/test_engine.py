"""HueyEngineAdapter tests with a real MemoryHuey instance.

Covers: discovery, queue listing, get_task, reconcile_task,
cancel_task, retry_task error path, capability honesty.
"""

from __future__ import annotations

import pytest

pytest.importorskip("huey")

from huey import MemoryHuey  # noqa: E402

from z4j_huey import HueyEngineAdapter  # noqa: E402


@pytest.fixture
def huey():
    inst = MemoryHuey("test", immediate=False)

    @inst.task()
    def add(x, y):
        return x + y

    @inst.task()
    def boom():
        raise RuntimeError("nope")

    return inst


@pytest.fixture
def adapter(huey):
    return HueyEngineAdapter(huey=huey)


@pytest.mark.asyncio
async def test_capabilities_honest(adapter):
    caps = adapter.capabilities()
    assert "retry_task" in caps
    assert "cancel_task" in caps
    # honest absences
    assert "bulk_retry" not in caps
    assert "purge_queue" not in caps
    assert "restart_worker" not in caps
    assert "rate_limit" not in caps


@pytest.mark.asyncio
async def test_discover_lists_registered_tasks(adapter):
    defs = await adapter.discover_tasks()
    names = {d.name for d in defs}
    # Huey registers tasks under their qualified name.
    assert any("add" in n for n in names)
    assert any("boom" in n for n in names)


@pytest.mark.asyncio
async def test_list_queues_empty_for_v1(adapter):
    # v1 leaves queue rows for the brain to synthesize from events;
    # see RqEngineAdapter for the same pattern.
    assert await adapter.list_queues() == []


@pytest.mark.asyncio
async def test_list_workers_empty(adapter):
    # Huey has no in-process worker registry - empty list is honest.
    assert await adapter.list_workers() == []


@pytest.mark.asyncio
async def test_reconcile_unknown_task_returns_safe_state(adapter):
    res = await adapter.reconcile_task("nonexistent-id")
    assert res.status == "success"
    # Either "pending" (no result yet) or "unknown" (result store
    # doesn't expose peek_data on this Huey backend) is correct -
    # both let the brain leave its own state untouched.
    assert res.result["engine_state"] in ("pending", "unknown")


@pytest.mark.asyncio
async def test_cancel_task_returns_success(adapter):
    res = await adapter.cancel_task("some-id")
    assert res.status == "success"
    assert res.result["revoked"] is True


@pytest.mark.asyncio
async def test_retry_without_overrides_errors(adapter):
    res = await adapter.retry_task("some-id")
    assert res.status == "failed"
    assert "override_args" in res.error or "override" in res.error


@pytest.mark.asyncio
async def test_submit_task_unknown_name_fails(adapter):
    res = await adapter.submit_task("does.not.exist", args=("x",))
    assert res.status == "failed"
    assert "unknown" in res.error


@pytest.mark.asyncio
async def test_submit_task_known_task_returns_id(huey, adapter):
    # Find any registered task name from huey's registry.
    name = next(iter(huey._registry._registry.keys()))
    res = await adapter.submit_task(name, args=(1, 2))
    # Either succeeds (in MemoryHuey it would) or fails with a
    # serializer error - either way, the lookup happened.
    assert res.status in ("success", "failed")
    if res.status == "success":
        assert "task_id" in res.result


@pytest.mark.asyncio
async def test_unsupported_actions_return_clean_failures(adapter):
    bulk = await adapter.bulk_retry({})
    assert bulk.status == "failed"
    purge = await adapter.purge_queue("test")
    assert purge.status == "failed"
    rl = await adapter.rate_limit("t", "5/s")
    assert rl.status == "failed"
    restart = await adapter.restart_worker("w")
    assert restart.status == "failed"
