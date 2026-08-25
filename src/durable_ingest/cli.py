"""The operator surface. Every experiment in the README is a subcommand here.

Exit codes, because a scheduler reads those and not your prose:
    0 clean · 1 a failure · 2 bad arguments · 130 interrupted
"""

import argparse
import asyncio
import json
import logging
import sys
import uuid

from . import config, pipeline, schedules, store, worker


def _log(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,  # progress to stderr, results to stdout, so `| jq` still works
    )
    if not verbose:
        logging.getLogger("temporalio").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)


def _out(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


# --- commands -------------------------------------------------------------------


def cmd_local(args) -> int:
    """The whole pipeline with NO Temporal. Run this first."""
    _out(pipeline.run_local(args.limit, args.batch_size, args.delay_ms))
    return 0


def cmd_worker(args) -> int:
    asyncio.run(worker.run_worker())
    return 0


async def _start(args) -> int:
    client = await worker.connect()
    workflow_id = args.id or f"{config.WORKFLOW_ID_PREFIX}-{uuid.uuid4().hex[:8]}"

    handle = await client.start_workflow(
        "TopStoriesWorkflow",
        args=[args.limit, args.batch_size, args.delay_ms, args.fail_first_n],
        id=workflow_id,
        task_queue=args.task_queue or config.TASK_QUEUE,
    )
    print(f"started  workflow_id={handle.id}", file=sys.stderr)
    print(f"         run_id={handle.result_run_id}", file=sys.stderr)
    print(f"         UI: http://localhost:8233/namespaces/{config.TEMPORAL_NAMESPACE}"
          f"/workflows/{handle.id}", file=sys.stderr)

    if not args.wait:
        _out({"workflow_id": handle.id, "started": True})
        return 0

    result = await handle.result()
    _out(result)
    return 0 if result.get("failed_batches", 0) == 0 else 1


async def _signal(args) -> int:
    client = await worker.connect()
    handle = client.get_workflow_handle(args.id)
    ids = [int(x) for x in args.ids.split(",") if x.strip()]
    await handle.signal("add_ids", ids)
    _out({"signalled": args.id, "add_ids": ids})
    return 0


async def _stop(args) -> int:
    client = await worker.connect()
    await client.get_workflow_handle(args.id).signal("stop")
    _out({"signalled": args.id, "stop": True})
    return 0


async def _query(args) -> int:
    client = await worker.connect()
    _out(await client.get_workflow_handle(args.id).query("progress"))
    return 0


async def _describe(args) -> int:
    client = await worker.connect()
    desc = await client.get_workflow_handle(args.id).describe()
    _out({
        "workflow_id": desc.id,
        "run_id": desc.run_id,
        "status": desc.status.name if desc.status else None,
        "task_queue": desc.task_queue,
        "start_time": desc.start_time,
        "close_time": desc.close_time,
    })
    return 0


async def _schedule(args) -> int:
    client = await worker.connect()
    action = args.action
    if action == "create":
        _out({"schedule": await schedules.create(client, args.limit, args.batch_size)})
    elif action == "update":
        _out({"schedule": await schedules.update(client, args.limit, args.batch_size)})
    elif action == "trigger":
        _out({"triggered": await schedules.trigger(client)})
    elif action == "pause":
        _out({"state": await schedules.set_paused(client, True)})
    elif action == "unpause":
        _out({"state": await schedules.set_paused(client, False)})
    elif action == "describe":
        _out(await schedules.describe(client))
    elif action == "delete":
        _out({"schedule": await schedules.delete(client)})
    return 0


def cmd_landed(args) -> int:
    ids = store.landed(args.run_id)
    _out({"run_id": args.run_id, "count": len(ids), "ids": ids[:20]})
    return 0


def cmd_reset(args) -> int:
    _out({"removed_files": store.clear(args.run_id)})
    return 0


# --- wiring ---------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="durable-ingest",
        description="A hands-on Temporal playground over the Hacker News API.",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging, including temporalio's")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_run_args(sp):
        sp.add_argument("--limit", type=int, default=config.DEFAULT_LIMIT, help="stories to take")
        sp.add_argument("--batch-size", type=int, default=config.DEFAULT_BATCH_SIZE)
        sp.add_argument("--delay-ms", type=int, default=0,
                        help="artificial pause per story, so you can watch / interrupt a run")

    sp = sub.add_parser("local", help="run the pipeline with NO Temporal at all")
    add_run_args(sp)
    sp.set_defaults(func=cmd_local)

    sp = sub.add_parser("worker", help="start the long-poll worker (blocks)")
    sp.set_defaults(func=cmd_worker)

    sp = sub.add_parser("start", help="start one workflow")
    add_run_args(sp)
    sp.add_argument("--fail-first-n", type=int, default=0,
                    help="make each batch activity fail its first N attempts, on purpose")
    sp.add_argument("--id", help="workflow id (default: a generated one)")
    sp.add_argument("--task-queue", help="override the task queue — try a wrong one on purpose")
    sp.add_argument("--wait", action="store_true", help="block until the workflow finishes")
    sp.set_defaults(func=lambda a: asyncio.run(_start(a)))

    sp = sub.add_parser("signal", help="send ids into a RUNNING workflow")
    sp.add_argument("id")
    sp.add_argument("--ids", required=True, help="comma-separated story ids")
    sp.set_defaults(func=lambda a: asyncio.run(_signal(a)))

    sp = sub.add_parser("stop", help="ask a running workflow to finish early (a signal)")
    sp.add_argument("id")
    sp.set_defaults(func=lambda a: asyncio.run(_stop(a)))

    sp = sub.add_parser("query", help="read a workflow's live progress (running OR finished)")
    sp.add_argument("id")
    sp.set_defaults(func=lambda a: asyncio.run(_query(a)))

    sp = sub.add_parser("describe", help="status of one workflow")
    sp.add_argument("id")
    sp.set_defaults(func=lambda a: asyncio.run(_describe(a)))

    sp = sub.add_parser("schedule", help="server-side schedule management")
    sp.add_argument("action", choices=["create", "update", "trigger", "pause", "unpause",
                                       "describe", "delete"])
    sp.add_argument("--limit", type=int, default=config.DEFAULT_LIMIT)
    sp.add_argument("--batch-size", type=int, default=config.DEFAULT_BATCH_SIZE)
    sp.set_defaults(func=lambda a: asyncio.run(_schedule(a)))

    sp = sub.add_parser("landed", help="what is on disk for a run")
    sp.add_argument("run_id")
    sp.set_defaults(func=cmd_landed)

    sp = sub.add_parser("reset", help="delete landed data so a demo can be repeated")
    sp.add_argument("--run-id", default=None, help="one run, or all runs if omitted")
    sp.set_defaults(func=cmd_reset)

    args = p.parse_args(argv)
    _log(args.verbose)

    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as e:  # noqa: BLE001 — the CLI boundary; a traceback helps nobody here
        logging.getLogger("durable_ingest").error("%s: %s", type(e).__name__, e)
        if args.verbose:
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
