"""The workflow — the recipe, and nothing else.

READ THE IMPORT BLOCK BELOW FIRST. It is the most surprising thing about writing Temporal code
in Python, and it explains a class of startup failure that is otherwise baffling.

A workflow function must produce the SAME DECISIONS every time it is replayed against its
recorded history, because replay is how Temporal reconstructs state after a worker dies. So
workflow code may not do I/O, read the wall clock, or depend on anything that varies between
runs. Temporal enforces a good chunk of that with a sandbox that inspects what this module
imports — which is why the activities are referenced by string name below and never imported.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Only the light, side-effect-free config module is imported into the sandbox. Importing
# `activities` here would pull in `requests` (and transitively `http.client`, sockets, ssl),
# which the workflow sandbox restricts — the worker would fail validation AT STARTUP with a
# message about a restricted import, long before any workflow runs.
#
# Try it: change this to `from . import activities` and start the worker. That error is worth
# meeting once in a lab rather than for the first time in production.
with workflow.unsafe.imports_passed_through():
    from . import config

# --- the contract -------------------------------------------------------------
#
# Two timeouts per activity, and they answer different questions:
#   start_to_close    — how long may ONE attempt take?
#   schedule_to_close — how long may ALL attempts take, together?
# Plus, for anything long-running:
#   heartbeat_timeout — how long may it go WITHOUT REPORTING PROGRESS?

IDS_TIMEOUT = timedelta(seconds=30)
IDS_DEADLINE = timedelta(minutes=2)

BATCH_TIMEOUT = timedelta(minutes=5)
BATCH_DEADLINE = timedelta(minutes=20)
BATCH_HEARTBEAT = timedelta(seconds=30)

# Bounded, not unlimited. The activities are idempotent so retrying is always safe, but an
# activity that has failed this many times is failing for a reason retries do not fix, and
# should surface as a failed batch rather than quietly absorb the whole deadline.
RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=4,
)


@workflow.defn
class TopStoriesWorkflow:
    """Discover the top stories, then fetch them in concurrent batches.

    Notice what is NOT here: any story id, any schedule, any knowledge of where files go. The
    workflow decides *what happens in what order*; everything else is an activity's problem.
    """

    def __init__(self) -> None:
        # Ordinary instance state. It is safe because it is rebuilt deterministically on replay —
        # every value below is derived from activity results, which Temporal has recorded.
        self._progress = {
            "phase": "starting",
            "stories": 0,
            "batches_total": 0,
            "batches_done": 0,
            "written": 0,
            "skipped": 0,
            "not_found": 0,
            "failed_batches": 0,
        }
        self._extra_ids: list[int] = []
        self._stop_requested = False

    # --- signals and queries ---------------------------------------------------
    #
    # A SIGNAL is input to a running workflow: fire-and-forget, may change what it does next.
    # A QUERY is a read of a running workflow's state: synchronous, and must not mutate anything.
    # Neither exists in the production ingest pipelines, but both are core Temporal, and this is
    # the cheapest place to meet them.

    @workflow.signal
    def add_ids(self, ids: list[int]) -> None:
        """Inject extra story ids into a run that is already going.

        This is the human-in-the-loop shape in miniature: something outside the workflow learns
        a fact after it started, and hands it in without restarting anything.
        """
        workflow.logger.info("signal add_ids: %s", ids)
        self._extra_ids.extend(ids)

    @workflow.signal
    def stop(self) -> None:
        """Ask the workflow to finish after the current batches, rather than killing it.

        A graceful stop is a signal. A hard stop is `temporal workflow terminate`, which kills it
        mid-flight and runs no cleanup — the difference matters when the work has side effects.
        """
        workflow.logger.info("signal stop: will finish after current work")
        self._stop_requested = True

    @workflow.query
    def progress(self) -> dict:
        """Read the live counters. Queries must be side-effect free — no I/O, no mutation.

        Try this against a running workflow (`durable-ingest query <id>`) and against a finished one:
        it works on both, because the server replays the closed history to answer.
        """
        return dict(self._progress)

    # --- the run ---------------------------------------------------------------

    @workflow.run
    async def run(
        self,
        limit: int = 0,
        batch_size: int = 0,
        delay_ms: int = 0,
        fail_first_n: int = 0,
    ) -> dict:
        limit = limit or config.DEFAULT_LIMIT
        batch_size = batch_size or config.DEFAULT_BATCH_SIZE

        # workflow.now() — NOT datetime.now(). On replay the wall clock has moved on, so a real
        # clock would produce a different value than the original run and the reconstruction
        # would diverge. workflow.now() replays to the same instant every time.
        run_id = workflow.now().strftime("%Y%m%dT%H%M%S")
        workflow.logger.info("run %s starting (limit=%d, batch=%d)", run_id, limit, batch_size)

        # --- stage 1: discover ------------------------------------------------
        self._progress["phase"] = "discovering"
        ids: list[int] = await workflow.execute_activity(
            "fetch_top_ids",
            args=[limit],
            start_to_close_timeout=IDS_TIMEOUT,
            schedule_to_close_timeout=IDS_DEADLINE,
            retry_policy=RETRY,
        )
        self._progress["stories"] = len(ids)

        # --- stage 2: fan out -------------------------------------------------
        batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]
        self._progress["batches_total"] = len(batches)
        self._progress["phase"] = "fetching"
        workflow.logger.info("%d stories in %d batches", len(ids), len(batches))

        await self._run_batches(run_id, batches, delay_ms, fail_first_n)

        # --- drain anything signalled in while we were working ----------------
        #
        # `wait_condition` is how a workflow waits on its own state changing. Here it is a
        # zero-length pause that simply lets any already-delivered signal be applied; in a
        # human-in-the-loop workflow it is how you block for an approval that may take days.
        await workflow.wait_condition(lambda: True)
        if self._extra_ids and not self._stop_requested:
            extra = list(dict.fromkeys(self._extra_ids))  # dedupe, ORDER-STABLE
            self._extra_ids.clear()
            workflow.logger.info("draining %d signalled ids", len(extra))
            self._progress["phase"] = "draining signalled ids"
            await self._run_batches(
                run_id, [extra[i : i + batch_size] for i in range(0, len(extra), batch_size)],
                delay_ms, 0,
            )

        # A cheap final activity, so the summary reports what is actually on disk rather than
        # what we believe we wrote. Cross-checking the two has caught more bugs than it costs.
        landed = await workflow.execute_activity(
            "count_landed",
            args=[run_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )

        self._progress["phase"] = "done"
        summary = {"run_id": run_id, "landed_on_disk": landed, **self._progress}
        workflow.logger.info("run complete: %s", summary)
        return summary

    async def _run_batches(
        self, run_id: str, batches: list[list[int]], delay_ms: int, fail_first_n: int
    ) -> None:
        """Run every batch concurrently, and survive the ones that fail.

        `return_exceptions=True` is the whole trick. Without it, the first failing batch cancels
        its siblings and a run that was 90% successful returns nothing. With it, a partial run is
        banked: what landed is correct, and the rest is picked up next time at no cost, because
        every already-landed story is skipped.
        """
        results = await asyncio.gather(
            *[
                workflow.execute_activity(
                    "fetch_batch",
                    args=[run_id, batch, delay_ms, fail_first_n],
                    start_to_close_timeout=BATCH_TIMEOUT,
                    schedule_to_close_timeout=BATCH_DEADLINE,
                    heartbeat_timeout=BATCH_HEARTBEAT,
                    retry_policy=RETRY,
                )
                for batch in batches
            ],
            return_exceptions=True,
        )

        for result in results:
            # A deliberate cancellation is not a data failure and must not be counted as one.
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                self._progress["failed_batches"] += 1
                workflow.logger.error("batch failed after all retries: %r", result)
                continue
            for key in ("written", "skipped", "not_found"):
                self._progress[key] += result.get(key, 0)
            self._progress["batches_done"] += 1
