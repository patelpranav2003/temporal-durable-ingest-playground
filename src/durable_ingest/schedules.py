"""Schedules — the server fires them, not your code.

The important idea: a schedule is an object that lives ON THE TEMPORAL SERVER. Nothing in this
project is a cron, and the worker has no idea what time it is. When a schedule comes due the
server starts a workflow and hands the task to whichever worker is polling.

Consequences worth noticing:
  * a schedule keeps firing while every worker is down — the runs queue up and drain when one
    comes back, rather than being silently missed;
  * pausing is server-side state, so it survives a deploy;
  * the run history of a schedule is queryable, so "did it fire on the 12th?" has an answer.
"""

import logging
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
)

from . import config
from .workflows import TopStoriesWorkflow

logger = logging.getLogger(__name__)


def _spec() -> ScheduleSpec:
    """When the schedule fires — an interval, or one wall-clock time a day.

    A ScheduleSpec's times are the UNION of its calendars, intervals and cron expressions, so
    exactly one of them is populated here. Mixing them is legal and almost never what anyone
    means.

    The time zone matters more than it looks. A calendar spec with no `time_zone_name` is
    evaluated by the server in UTC, so "hour 15" fires at 20:30 in India. Naming the zone also
    means the schedule follows daylight saving, which a fixed offset cannot.
    """
    if config.SCHEDULE_MODE == "daily":
        hour, _, minute = config.SCHEDULE_DAILY_AT.partition(":")
        return ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    hour=[ScheduleRange(int(hour))],
                    minute=[ScheduleRange(int(minute or 0))],
                    # second, day_of_month, month and day_of_week keep their defaults: second 0,
                    # and every day of every month. So this is "once a day, at that time".
                    comment=f"daily at {config.SCHEDULE_DAILY_AT} {config.SCHEDULE_TIMEZONE}",
                )
            ],
            time_zone_name=config.SCHEDULE_TIMEZONE,
        )

    if config.SCHEDULE_MODE != "interval":
        # Fail loudly. A typo that silently fell back to a 5-minute interval would be a nasty
        # surprise on something meant to run once a day.
        raise ValueError(
            f"HN_SCHEDULE_MODE must be 'interval' or 'daily', got {config.SCHEDULE_MODE!r}"
        )

    return ScheduleSpec(
        intervals=[ScheduleIntervalSpec(every=timedelta(minutes=config.SCHEDULE_INTERVAL_MINUTES))]
    )


def _describe_spec() -> str:
    """One line for a log, so the operator sees which shape they just created."""
    if config.SCHEDULE_MODE == "daily":
        return f"daily at {config.SCHEDULE_DAILY_AT} ({config.SCHEDULE_TIMEZONE})"
    return f"every {config.SCHEDULE_INTERVAL_MINUTES} minutes"


def _schedule(limit: int, batch_size: int) -> Schedule:
    return Schedule(
        action=ScheduleActionStartWorkflow(
            TopStoriesWorkflow.run,
            # Arguments are passed HERE, as real values. Note what we do NOT do: pass a Go
            # template like "{{.ScheduledTime}}". That works in Temporal's Go SDK and is silently
            # NOT substituted by the Python SDK, so the literal string would end up in your data.
            # The workflow derives its own run id from workflow.now() instead.
            args=[limit, batch_size, 0, 0],
            id=f"{config.WORKFLOW_ID_PREFIX}-scheduled",
            task_queue=config.TASK_QUEUE,
        ),
        spec=_spec(),
        state=ScheduleState(note=f"created by durable-ingest — {_describe_spec()}"),
    )


async def create(client: Client, limit: int, batch_size: int) -> str:
    """Create the schedule if it is absent; leave an existing one alone.

    CREATE-ONLY, and the distinction matters more than it looks. Updating a schedule replaces its
    STATE as well as its spec — and state includes `paused`. A process that reconciled on every
    start would silently un-pause a schedule somebody had deliberately switched off, with nothing
    anywhere recording that a restart is what resumed it.
    """
    try:
        await client.create_schedule(config.SCHEDULE_ID, _schedule(limit, batch_size))
    except ScheduleAlreadyRunningError:
        logger.info("schedule %s already exists — left as it is", config.SCHEDULE_ID)
        return "exists"
    logger.info(
        "created schedule %s — %s",
        config.SCHEDULE_ID,
        _describe_spec(),
    )
    return "created"


async def update(client: Client, limit: int, batch_size: int) -> str:
    """Push the spec in config onto an existing schedule. A deliberate act, never automatic."""
    handle = client.get_schedule_handle(config.SCHEDULE_ID)
    # The callback must return a ScheduleUpdate, not a bare Schedule.
    await handle.update(lambda _: ScheduleUpdate(schedule=_schedule(limit, batch_size)))
    return "updated"


async def trigger(client: Client) -> str:
    """Fire it now, without waiting for the next slot.

    Goes THROUGH the schedule rather than starting a bare workflow, so the run appears in the
    schedule's own history where anyone looking for it expects to find it.
    """
    await client.get_schedule_handle(config.SCHEDULE_ID).trigger()
    return config.SCHEDULE_ID


async def set_paused(client: Client, paused: bool) -> str:
    handle = client.get_schedule_handle(config.SCHEDULE_ID)
    if paused:
        await handle.pause(note="paused by durable-ingest")
    else:
        await handle.unpause(note="resumed by durable-ingest")
    return "paused" if paused else "running"


async def describe(client: Client) -> dict:
    desc = await client.get_schedule_handle(config.SCHEDULE_ID).describe()
    return {
        "id": config.SCHEDULE_ID,
        # What the SERVER holds, which is not necessarily what config says now — an existing
        # schedule keeps the spec it was created with until `schedule update`.
        "spec": {
            "time_zone": desc.schedule.spec.time_zone_name,
            "calendars": [str(c) for c in desc.schedule.spec.calendars],
            "intervals": [str(i.every) for i in desc.schedule.spec.intervals],
        },
        "paused": desc.schedule.state.paused,
        "note": desc.schedule.state.note,
        "recent_runs": [a.started_at.isoformat() for a in desc.info.recent_actions[-5:]],
        "next_runs": [t.isoformat() for t in desc.info.next_action_times[:3]],
    }


async def delete(client: Client) -> str:
    await client.get_schedule_handle(config.SCHEDULE_ID).delete()
    return "deleted"
