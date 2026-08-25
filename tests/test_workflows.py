"""Workflow tests against Temporal's own test environment — no server, no network, no waiting.

Two things worth knowing before reading these:

1. `WorkflowEnvironment.start_time_skipping()` runs a real Temporal server in-process with a
   FAKE CLOCK. A workflow that sleeps for an hour completes instantly, because the environment
   fast-forwards whenever every workflow is blocked on a timer. Retry backoff is skipped the
   same way — which is why the retry test below runs in milliseconds despite a 1s..10s policy.

2. Activities are REPLACED with test doubles. The workflow under test never touches the network.
   That is only possible because the workflow refers to activities by name and by contract, not
   by importing them — the same property that keeps the sandbox happy.
"""

import uuid

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from durable_ingest.workflows import TopStoriesWorkflow

TASK_QUEUE = "test-durable-ingest"


# --- test doubles ---------------------------------------------------------------
# Registered under the SAME names the workflow dispatches to.


@activity.defn(name="fetch_top_ids")
async def fake_top_ids(limit: int) -> list[int]:
    return list(range(1, limit + 1))


@activity.defn(name="fetch_batch")
async def fake_batch(run_id: str, ids: list[int], delay_ms: int = 0, fail_first_n: int = 0) -> dict:
    return {"written": len(ids), "skipped": 0, "not_found": 0}


@activity.defn(name="count_landed")
async def fake_count(run_id: str) -> int:
    return 0


_ATTEMPTS: dict[str, int] = {}


@activity.defn(name="fetch_batch")
async def flaky_batch(run_id: str, ids: list[int], delay_ms: int = 0, fail_first_n: int = 0) -> dict:
    """Fails its first two attempts, then succeeds — driven by Temporal's own attempt counter."""
    attempt = activity.info().attempt
    _ATTEMPTS[str(ids)] = attempt
    if attempt <= 2:
        raise RuntimeError(f"injected failure on attempt {attempt}")
    return {"written": len(ids), "skipped": 0, "not_found": 0}


@activity.defn(name="fetch_batch")
async def always_fails(run_id: str, ids: list[int], delay_ms: int = 0, fail_first_n: int = 0) -> dict:
    raise RuntimeError("this batch never succeeds")


async def _run(env: WorkflowEnvironment, batch_impl, **kwargs) -> dict:
    client: Client = env.client
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[TopStoriesWorkflow],
        activities=[fake_top_ids, batch_impl, fake_count],
    ):
        return await client.execute_workflow(
            TopStoriesWorkflow.run,
            args=[kwargs.get("limit", 10), kwargs.get("batch_size", 4), 0, 0],
            id=f"test-{uuid.uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
        )


# --- tests ----------------------------------------------------------------------


async def test_fans_out_and_sums_the_batches():
    """10 stories at batch size 4 is 3 batches, and every story is accounted for."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result = await _run(env, fake_batch, limit=10, batch_size=4)

    assert result["stories"] == 10
    assert result["batches_total"] == 3      # 4 + 4 + 2
    assert result["batches_done"] == 3
    assert result["written"] == 10
    assert result["failed_batches"] == 0
    assert result["phase"] == "done"


async def test_a_transient_failure_is_retried_not_fatal():
    """Two failed attempts, then success — and the workflow never notices.

    The retry policy's backoff is real (1s, 2s, ...); the time-skipping clock makes it free.
    """
    _ATTEMPTS.clear()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result = await _run(env, flaky_batch, limit=4, batch_size=4)

    assert result["written"] == 4
    assert result["failed_batches"] == 0
    assert max(_ATTEMPTS.values()) == 3, "should have succeeded on the third attempt"


async def test_a_permanently_failing_batch_degrades_the_run_instead_of_sinking_it():
    """This is the `return_exceptions=True` contract, asserted.

    The batch exhausts its retries and is counted as failed; the workflow still completes and
    still returns a summary. A partial result is banked rather than thrown away.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result = await _run(env, always_fails, limit=8, batch_size=4)

    assert result["failed_batches"] == 2
    assert result["batches_done"] == 0
    assert result["written"] == 0
    assert result["phase"] == "done", "the workflow must finish, not raise"


async def test_progress_query_is_answerable_after_completion():
    """A query works against a CLOSED workflow too — the server replays the history to answer."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        client: Client = env.client
        wid = f"test-{uuid.uuid4().hex[:8]}"
        async with Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[TopStoriesWorkflow],
            activities=[fake_top_ids, fake_batch, fake_count],
        ):
            await client.execute_workflow(
                TopStoriesWorkflow.run,
                args=[6, 3, 0, 0],
                id=wid,
                task_queue=TASK_QUEUE,
            )
            progress = await client.get_workflow_handle(wid).query("progress")

    assert progress["stories"] == 6
    assert progress["phase"] == "done"


@pytest.mark.parametrize("limit,batch_size,expected", [(10, 4, 3), (10, 10, 1), (1, 10, 1)])
async def test_batch_arithmetic(limit, batch_size, expected):
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result = await _run(env, fake_batch, limit=limit, batch_size=batch_size)
    assert result["batches_total"] == expected
