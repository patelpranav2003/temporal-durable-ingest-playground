"""Every knob, in one place, read from the environment.

Nothing here names a *story*. The list of what to fetch is discovered at run time — that is the
same rule the production source repos follow, and it is what lets the same code cover a normal
run and a catch-up without a second mode.
"""

import os

# THIS MODULE IS IMPORTED INTO THE WORKFLOW SANDBOX, so it must stay inert: plain values read
# from the environment, and nothing else. No filesystem calls, no network, no clock.
#
# It did not start out that way. The first version of this file computed a default data
# directory with `Path(__file__).resolve()`, and every workflow test failed at worker startup
# with "Failed validating workflow TopStoriesWorkflow" and a warning that
# `pathlib.Path.resolve` was restricted. The sandbox blocks it because a path that resolves
# differently on another machine is exactly the kind of thing that makes a replay diverge.
#
# The fix is the general one: keep the value the workflow needs (a string) here, and do the
# filesystem work lazily in store.py, which the workflow never imports. See store.dest_root().

# --- temporal ------------------------------------------------------------------

# Where `temporal server start-dev` listens. The Web UI is on 8233, not this port.
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")

# The dev server ships with `default` already created. A real deployment uses
# <team>-<domain>-(prod|staging|dev) and has to create it first.
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")

# The channel the worker polls and the workflow dispatches to. BOTH SIDES MUST AGREE — if they
# do not, the workflow sits in the UI as Running forever with no error anywhere. Break this on
# purpose once (see README, experiment 7); it is the single most confusing Temporal failure.
TASK_QUEUE = os.environ.get("HN_TASK_QUEUE", "durable-ingest")

WORKFLOW_ID_PREFIX = os.environ.get("HN_WORKFLOW_PREFIX", "hn-top-stories")
SCHEDULE_ID = os.environ.get("HN_SCHEDULE_ID", "hn-top-stories-every-5m")

# "interval" fires every N minutes and is what a demo wants — you see several runs while you
# watch. "daily" fires at one wall-clock time, which is what a real ingest wants.
SCHEDULE_MODE = os.environ.get("HN_SCHEDULE_MODE", "interval").strip().lower()

SCHEDULE_INTERVAL_MINUTES = int(os.environ.get("HN_SCHEDULE_INTERVAL_MINUTES", "5"))

# "HH:MM", used only when SCHEDULE_MODE is "daily". Kept as a STRING here and parsed in
# schedules.py — this module is imported into the workflow sandbox and must stay inert.
SCHEDULE_DAILY_AT = os.environ.get("HN_SCHEDULE_AT", "15:00")

# An IANA time zone name, e.g. "Asia/Kolkata", "US/Central", "UTC". The server evaluates the
# schedule in this zone, so it follows daylight saving — which a fixed UTC offset would not.
# WITHOUT THIS, Temporal interprets a calendar spec in UTC, and your 15:00 fires at 20:30 IST.
SCHEDULE_TIMEZONE = os.environ.get("HN_SCHEDULE_TIMEZONE", "Asia/Kolkata")

# --- the source ----------------------------------------------------------------

HN_BASE = os.environ.get("HN_BASE", "https://hacker-news.firebaseio.com/v0")
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HN_HTTP_TIMEOUT", "10"))

# --- the run -------------------------------------------------------------------

# How many of the ~500 top-story ids to take. 30 keeps a demo run under a minute while still
# producing several batches.
DEFAULT_LIMIT = int(os.environ.get("HN_LIMIT", "30"))

# Stories per stage-2 activity. The ACTIVITY IS THE UNIT OF RETRY, so this number is the
# blast radius of one failure — see README, "Why batches at all".
DEFAULT_BATCH_SIZE = int(os.environ.get("HN_BATCH_SIZE", "10"))

# How many batches one worker runs at once.
MAX_CONCURRENT_ACTIVITIES = int(os.environ.get("HN_MAX_CONCURRENT_ACTIVITIES", "4"))

# Where landed files go — as a STRING, or None for "work it out relative to the package".
# Resolved into a real path by store.dest_root(), lazily, outside the sandbox. One JSON file per
# story per run is what makes a run resumable: the file's existence is the record of the work.
DEST_ROOT_ENV = os.environ.get("HN_DEST_ROOT") or None
