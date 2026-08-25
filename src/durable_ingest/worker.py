"""The worker: a long-running process that holds your code and waits to be given work.

The direction of the connection is the thing to internalise. The worker DIALS OUT to the server
and holds that connection open, asking "anything for me?" — long-polling. When a workflow needs
to advance, the server hands the task back down that already-open channel.

Nothing ever dials in. That is why a deployed worker needs no inbound firewall rule, no public
address and no load balancer, and it is also why a disconnected worker looks perfectly healthy
from the outside: the process is up, nothing is red, and no work is being done.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from . import activities, config
from .workflows import TopStoriesWorkflow

logger = logging.getLogger(__name__)


async def connect() -> Client:
    """One place that knows how to reach Temporal.

    A local dev server needs neither TLS nor an API key; Temporal Cloud needs both. Neither is
    ever inferred from the address — a guess fails in the confusing direction, connecting to
    the wrong thing rather than refusing to connect at all.
    """
    logger.info(
        "connecting to %s (namespace=%s)", config.TEMPORAL_ADDRESS, config.TEMPORAL_NAMESPACE
    )
    return await Client.connect(config.TEMPORAL_ADDRESS, namespace=config.TEMPORAL_NAMESPACE)


async def run_worker(stop: asyncio.Event | None = None) -> None:
    client = await connect()

    concurrency = max(1, config.MAX_CONCURRENT_ACTIVITIES)

    # The activities are synchronous, so they run in a thread pool rather than on the event loop.
    # One spare thread above the activity ceiling leaves room for a short activity to start while
    # the long ones are saturating the pool, instead of queueing behind them.
    with ThreadPoolExecutor(max_workers=concurrency + 1) as pool:
        worker = Worker(
            client,
            task_queue=config.TASK_QUEUE,
            workflows=[TopStoriesWorkflow],
            activities=activities.ALL,
            activity_executor=pool,
            max_concurrent_activities=concurrency,
        )
        logger.info(
            "polling task_queue=%s concurrency=%d — Ctrl-C to stop",
            config.TASK_QUEUE,
            concurrency,
        )
        logger.info("Web UI: http://localhost:8233")
        if stop is None:
            await worker.run()
        else:
            async with worker:
                await stop.wait()
