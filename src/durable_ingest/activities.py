"""Activities — the only place in this project that is allowed to touch the outside world.

Two rules make an activity well-behaved, and both are visible below.

IDEMPOTENT. Running it twice has the same effect as running it once, because every write is
guarded by a skip-if-present check on a deterministic path. That is what makes Temporal's
retries free rather than merely tolerable: a retried attempt resumes, it does not repeat.

DEFINED WITH `def`, NOT `async def`. Everything underneath blocks — HTTP calls, file writes,
sleeps. An `async def` activity that blocks holds the event loop for its whole duration, which
stalls every other activity on the worker AND the heartbeats that are supposed to prove this one
is alive. Sync activities run in the worker's thread pool instead, so batches genuinely progress
at once. (See worker.py, where the pool is created.)
"""

import logging

from temporalio import activity

from . import config, hn, pipeline

logger = logging.getLogger(__name__)


class InjectedFailure(RuntimeError):
    """A failure we caused on purpose, so a retry policy has something to retry.

    Nothing like this belongs in real code. It is here so you can watch backoff happen in the
    Web UI instead of reading about it.
    """


@activity.defn(name="fetch_top_ids")
def fetch_top_ids(limit: int) -> list[int]:
    """Stage 1 — discover the work-list.

    Returns the ids themselves rather than a count. For 30 stories that is a few hundred bytes,
    it crosses the workflow boundary comfortably, and it means the batches are VISIBLE IN THE
    WORKFLOW HISTORY. When someone asks what run X actually covered, the answer is in the event
    list rather than inferred from a file somewhere.
    """
    ids = hn.top_story_ids(limit)
    activity.logger.info("work-list frozen: %d stories", len(ids))
    return ids


@activity.defn(name="fetch_batch")
def fetch_batch(
    run_id: str,
    ids: list[int],
    delay_ms: int = 0,
    fail_first_n: int = 0,
) -> dict:
    """Stage 2 — fetch and land one batch.

    HEARTBEATS AFTER EVERY STORY. This is what lets Temporal tell a slow attempt from a wedged
    one. A batch crawling through a throttled API keeps heartbeating and is left alone to finish;
    a batch that has reported nothing for the heartbeat timeout is genuinely stuck and gets
    retried. A wall-clock deadline cannot distinguish those two, and killing the slow-but-working
    case is how a healthy run gets failed.

    ``fail_first_n`` uses ``activity.info().attempt`` — the real retry counter Temporal maintains,
    starting at 1. Pass 2 and the first two attempts raise, the third succeeds, and the whole
    sequence is legible in the UI's event history.
    """
    attempt = activity.info().attempt
    if fail_first_n and attempt <= fail_first_n:
        activity.logger.warning(
            "attempt %d of this activity is failing ON PURPOSE (fail_first_n=%d)",
            attempt,
            fail_first_n,
        )
        raise InjectedFailure(f"injected failure on attempt {attempt}")

    summary = pipeline.fetch_many(
        run_id,
        ids,
        delay_ms=delay_ms,
        # The heartbeat carries a payload — the index reached. Temporal keeps the last one, so a
        # retried attempt could read it back via activity.info().heartbeat_details and skip ahead.
        # We do not need that here, because skip-if-present already resumes at story granularity;
        # it matters when work is NOT idempotent and re-doing it is expensive.
        on_progress=lambda index, result: activity.heartbeat(index + 1),
    )
    activity.logger.info("batch complete (attempt %d): %s", attempt, summary)
    return summary


@activity.defn(name="count_landed")
def count_landed(run_id: str) -> int:
    """How many stories this run has on disk.

    Deliberately trivial, and deliberately an ACTIVITY. Reading the filesystem is I/O, and I/O in
    a workflow is exactly the thing that breaks replay. If you are ever tempted to "just check
    something quickly" from workflow code, this is the shape the fix takes.
    """
    from . import store

    return len(store.landed(run_id))


ALL = [fetch_top_ids, fetch_batch, count_landed]

# Referenced by the workflow BY STRING NAME, never imported there — see workflows.py for why.
NAMES = {"fetch_top_ids", "fetch_batch", "count_landed"}
assert {a.__name__ for a in ALL} == NAMES, "activity registry drifted from its name list"
assert config.TASK_QUEUE, "a task queue name is required"
