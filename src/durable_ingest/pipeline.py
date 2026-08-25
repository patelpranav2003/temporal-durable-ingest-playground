"""The actual work: fetch stories and land them. Still no Temporal in this file.

Everything Temporal adds — retries, fan-out, scheduling, durability — is orchestration ON TOP of
these functions, not inside them. You can prove that by running ``durable-ingest local``, which executes
the whole pipeline with no server and no worker.
"""

import logging
import time
from collections.abc import Callable

from . import config, hn, store

logger = logging.getLogger(__name__)


def fetch_one(run_id: str, item_id: int, delay_ms: int = 0) -> dict:
    """Fetch and land a single story. Idempotent.

    Returns a small status dict rather than raising for the two SETTLED outcomes (already
    landed, does not exist), because neither is a failure of the run. Anything genuinely wrong
    propagates and becomes the activity's problem, and therefore Temporal's.
    """
    if store.exists(run_id, item_id):
        return {"status": "skipped", "id": item_id}

    if delay_ms:
        # Purely so a human can watch a run in the UI and kill the worker mid-flight.
        time.sleep(delay_ms / 1000)

    try:
        item = hn.fetch_item(item_id)
    except hn.NotFoundError:
        # Settled. Retrying cannot make a dead id resolve, so do not spend the budget.
        logger.warning("item %s does not resolve — skipping", item_id)
        return {"status": "not_found", "id": item_id}

    store.write(run_id, item_id, item)
    return {"status": "written", "id": item_id, "title": item.get("title", "")[:80]}


def fetch_many(
    run_id: str,
    ids: list[int],
    delay_ms: int = 0,
    on_progress: Callable[[int, dict], None] | None = None,
) -> dict:
    """Fetch a batch, one story at a time, reporting progress as it goes.

    ``on_progress`` is how the Temporal layer heartbeats without this module importing Temporal.
    A plain callback keeps the dependency pointing one way.
    """
    summary = {"written": 0, "skipped": 0, "not_found": 0}
    for index, item_id in enumerate(ids):
        result = fetch_one(run_id, item_id, delay_ms=delay_ms)
        summary[result["status"]] += 1
        if on_progress:
            on_progress(index, result)
    return summary


def run_local(limit: int | None = None, batch_size: int | None = None, delay_ms: int = 0) -> dict:
    """The entire pipeline, in one process, with no Temporal at all.

    This exists to make a point you should verify before reading any workflow code: Temporal is
    not doing the work. It is deciding what runs, in what order, and what happens when a piece
    of it fails.
    """
    limit = limit or config.DEFAULT_LIMIT
    batch_size = batch_size or config.DEFAULT_BATCH_SIZE
    run_id = "local"

    ids = hn.top_story_ids(limit)
    logger.info("work-list: %d stories", len(ids))

    totals = {"written": 0, "skipped": 0, "not_found": 0}
    for start in range(0, len(ids), batch_size):
        batch = ids[start : start + batch_size]
        summary = fetch_many(run_id, batch, delay_ms=delay_ms)
        for k, v in summary.items():
            totals[k] += v
        logger.info("batch %d done: %s", start // batch_size + 1, summary)

    return {"run_id": run_id, "stories": len(ids), **totals}
