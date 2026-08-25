# temporal-durable-ingest-playground

[![test](https://github.com/patelpranav2003/temporal-durable-ingest-playground/actions/workflows/test.yml/badge.svg)](https://github.com/patelpranav2003/temporal-durable-ingest-playground/actions/workflows/test.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A hands-on [Temporal](https://temporal.io) playground: a two-stage ingest of Hacker News top
stories, **built to be broken on purpose**.

The pipeline itself is deliberately boring — discover a work-list, fetch each item, land it as
JSON. That is the point. The interesting part is what Temporal adds *around* boring work:
retries with backoff, fan-out, heartbeats, durability across a worker crash, signals, queries,
and server-side schedules. Every one of those has an experiment below that you run yourself and
watch fail.

Nothing here is a toy abstraction over Temporal. It is ~1,250 lines of commented Python you can
read in one sitting, and the comments explain *why* each decision is the way it is — including
the mistakes that cost real time to diagnose the first time.

---

## Table of contents

- [Why this exists](#why-this-exists)
- [How it works](#how-it-works)
- [Install](#install)
- [Quickstart](#quickstart)
- [Command reference](#command-reference)
- [The experiments](#the-experiments)
- [Configuration](#configuration)
- [Tests](#tests)
- [Where data lands](#where-data-lands)
- [Design notes](#design-notes)
- [Troubleshooting](#troubleshooting)
- [Project layout](#project-layout)

---

## Why this exists

Most Temporal tutorials show you a workflow that succeeds. The hard part of durable execution is
what happens when things go wrong, and you cannot learn that from a happy path.

So this repo is arranged around failure:

- an activity that **fails its first N attempts on purpose**, so you can watch backoff in the UI
- a batch that **never** succeeds, so you can see a run degrade instead of collapse
- a `--task-queue` override, so you can create the single most confusing Temporal failure
  deliberately and learn to recognise it
- a `--delay-ms` knob, so a run lasts long enough for you to kill the worker mid-flight
- workflow tests that prove the retry contract in **milliseconds**, using Temporal's fake clock

It also mirrors the structure of production ingestion repos: the fetching layer knows nothing
about Temporal, and the workflow knows nothing about HTTP. See [Design notes](#design-notes).

---

## How it works

Two stages, which is the shape most ingestion work takes:

```
                  ┌──────────────────────────────────────────────┐
                  │  TopStoriesWorkflow  (deterministic only)    │
                  │                                              │
   stage 1        │   await fetch_top_ids(limit)  ──────────────► │  [id, id, id, ...]
   discover       │                                              │
                  │   chunk into batches of N                    │
                  │                                              │
   stage 2        │   asyncio.gather(                            │
   fan out        │     fetch_batch(run_id, batch_1),            │
                  │     fetch_batch(run_id, batch_2),   ...      │
                  │     return_exceptions=True                   │
                  │   )                                          │
                  │                                              │
                  │   await count_landed(run_id)                 │
                  └──────────────────────────────────────────────┘
                                      │
                        activities (the only I/O)
                                      │
                                      ▼
                    HN API  ──►  _data/run=<id>/<story_id>.json
```

**The work-list is discovered, never configured.** No story id appears anywhere in this repo. A
story that starts trending after stage 1 is simply picked up by the next run — which is also
why the same code covers a normal run and a catch-up with no second mode.

**Every write is idempotent.** The destination path is a pure function of `(run_id, story_id)`,
and it is checked for existence before any HTTP call. So a retried activity *resumes* rather
than repeats, and a re-run of a completed run costs one `stat()` per story instead of a network
request. That is what makes Temporal's retries free rather than merely survivable.

### Why batches at all

The activity is the unit of retry. If one activity fetched all 30 stories, a single failure on
story 29 would retry all 29 successes with it — and if it exhausted its attempts, the whole run
would be a failure. Batching at 10 makes the blast radius of one failure one tenth of the run,
and `return_exceptions=True` in the fan-out means a failed batch is *counted* rather than
allowed to cancel its siblings.

Pick the batch size by asking: how much work am I willing to have re-attempted together?

---

## Install

**Requirements**

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) — on Windows,
  `winget install astral-sh.uv`; on macOS, `brew install uv`
- The [Temporal CLI](https://docs.temporal.io/cli) — `winget install Temporal.TemporalCLI` or
  `brew install temporal`. Needed for the dev server; **not** needed for `local` or the tests.

```bash
git clone https://github.com/patelpranav2003/temporal-durable-ingest-playground.git
cd temporal-durable-ingest-playground
uv sync
```

The only vendor dependency is `requests`. `temporalio` brings the SDK; `pytest` and
`pytest-asyncio` are dev-only.

---

## Quickstart

### 1. Prove Temporal is not doing the work

```bash
uv run durable-ingest local --limit 10
```

No server, no worker, no Temporal import on the path. The whole pipeline runs in one process and
prints a summary. Read `pipeline.py` alongside this — Temporal is orchestration *on top of*
these functions, never inside them.

### 2. Start the dev server

In its own terminal, and leave it running:

```bash
temporal server start-dev --db-filename _temporal/dev.db
```

- gRPC on **7233** (what the worker dials)
- Web UI on **8233** — <http://localhost:8233>

`--db-filename` makes state survive a restart. Without it the server is purely in-memory and
your schedules and histories vanish when you Ctrl+C it.

### 3. Start a worker

Another terminal, also left running:

```bash
uv run durable-ingest worker
```

The worker **dials out** to the server and long-polls. Nothing ever dials in — which is why a
deployed worker needs no inbound firewall rule, and also why a worker connected to the wrong
task queue looks perfectly healthy while doing nothing at all.

### 4. Run a workflow

```bash
uv run durable-ingest start --limit 30 --batch-size 10 --wait
```

Watch it in the UI while it runs. Then look at what landed:

```bash
uv run durable-ingest landed <run_id>     # run_id is in the summary
```

---

## Command reference

Everything below is `uv run durable-ingest <command>`.

| Command | What it does |
|---|---|
| `local` | The whole pipeline with **no Temporal at all**. Run this first. |
| `worker` | Start the long-poll worker. Blocks until Ctrl+C. |
| `start` | Start one workflow. `--wait` blocks for the result. |
| `query <id>` | Read a workflow's live progress counters. Works on running **and** finished workflows. |
| `signal <id> --ids 1,2,3` | Inject extra story ids into a **running** workflow. |
| `stop <id>` | Ask a workflow to finish gracefully after current work. |
| `describe <id>` | Status, run id, task queue, start/close time. |
| `schedule <action>` | `create`, `update`, `trigger`, `pause`, `unpause`, `describe`, `delete`. |
| `landed <run_id>` | What is on disk for a run. |
| `reset [--run-id X]` | Delete landed data so a demo can be repeated. |

**Flags on `local` and `start`**

| Flag | Default | Purpose |
|---|---|---|
| `--limit N` | `30` | How many of the ~500 top-story ids to take. |
| `--batch-size N` | `10` | Stories per stage-2 activity — the blast radius of one failure. |
| `--delay-ms N` | `0` | Artificial pause per story, so a run lasts long enough to interrupt. |
| `--fail-first-n N` | `0` | *(`start` only)* Make each batch fail its first N attempts, on purpose. |
| `--id X` | generated | Set the workflow id explicitly. |
| `--task-queue X` | from config | *(`start` only)* Override the queue — point it somewhere wrong on purpose. |
| `--wait` | off | Block until the workflow finishes. |
| `-v` / `--verbose` | off | Debug logging, including `temporalio`'s own. |

Progress goes to **stderr** and results to **stdout**, so `| jq` works:

```bash
uv run durable-ingest start --wait 2>/dev/null | jq .written
```

**Exit codes:** `0` clean · `1` a failure (including any failed batch) · `2` bad arguments ·
`130` interrupted.

---

## The experiments

Run these in order. Each one is a specific Temporal behaviour you cannot really understand from
prose.

### 1 — The pipeline without Temporal

```bash
uv run durable-ingest local --limit 10
```

Establishes the baseline: the fetching code is ordinary Python, unit-testable in milliseconds,
runnable with no infrastructure.

### 2 — Idempotence, i.e. why retries are free

```bash
uv run durable-ingest local --limit 10        # writes 10
uv run durable-ingest local --limit 10        # skips 10, writes 0
```

The second run makes zero HTTP calls. Retrying is cheap because it is a no-op on work already
done — that property is what everything below depends on.

### 3 — Durability: kill the worker mid-run

Start a slow run, then kill the worker while it is working:

```bash
uv run durable-ingest start --limit 30 --delay-ms 800 --id crash-test
# ...in the worker's terminal, Ctrl+C it while batches are in flight
```

The workflow does **not** fail. It sits in the UI as Running, because a workflow's state lives
on the server, not in your process. Bring the worker back:

```bash
uv run durable-ingest worker
uv run durable-ingest query crash-test
```

It picks up where it left off, and already-landed stories are skipped rather than re-fetched.
This is the single most important idea in the repo.

### 4 — Retry with backoff

```bash
uv run durable-ingest start --limit 20 --fail-first-n 2 --wait
```

Every batch fails its first two attempts and succeeds on the third, driven by Temporal's real
`activity.info().attempt` counter. Open the workflow's event history in the UI: you will see the
failures, the 1s → 2s backoff, and the eventual success as distinct recorded events. The
workflow code never learns any of it happened.

### 5 — A failure retries cannot fix

The retry policy allows 4 attempts, so make every attempt fail:

```bash
uv run durable-ingest start --limit 20 --fail-first-n 9 --wait ; echo "exit=$?"
```

The batches exhaust their retries and are counted in `failed_batches`. The workflow still
**completes** and still returns a summary — `return_exceptions=True` in the fan-out means a
partial run is banked instead of thrown away. The CLI exits `1` so a scheduler notices.

Whatever landed is correct and complete, and the next run costs almost nothing for the stories
that already succeeded.

### 6 — Signals and queries

Start something slow, then talk to it while it runs:

```bash
uv run durable-ingest start --limit 30 --delay-ms 500 --id chatty
uv run durable-ingest query chatty                       # read live counters
uv run durable-ingest signal chatty --ids 8863,1,2       # inject work mid-run
uv run durable-ingest stop chatty                        # graceful finish
```

A **signal** is input to a running workflow (fire-and-forget, may change what it does next). A
**query** is a side-effect-free read of its state. Then try the query again *after* it finishes —
it still answers, because the server replays the closed history to serve it.

Note the difference between `stop` (a signal; finishes the current work) and
`temporal workflow terminate` (kills it mid-flight, runs no cleanup).

### 7 — Break the task queue on purpose

This is the most confusing failure in Temporal, and it is worth meeting once here rather than
for the first time in production.

```bash
uv run durable-ingest start --limit 10 --task-queue nobody-is-listening --id lost
```

The workflow starts fine. It appears in the UI as **Running**. It stays that way forever. There
is no error anywhere — not in the worker, not in the server, not in the UI — because a task
queue nobody polls is indistinguishable from a queue whose worker is merely busy.

```bash
uv run durable-ingest describe lost      # note the task_queue field
```

The lesson: when a workflow is stuck at Running with no events progressing, **check that the
task queue in the start call matches the one the worker polls.** Both sides read
`config.TASK_QUEUE`, which is why they normally agree.

### 8 — Break the workflow sandbox on purpose

In `workflows.py`, change the guarded import to pull the activities in directly:

```python
with workflow.unsafe.imports_passed_through():
    from . import config, activities        # <-- add activities
```

Then start the worker. It fails **at startup**, before any workflow runs, with a message about
`Failed validating workflow TopStoriesWorkflow` and a restricted import.

Why: importing `activities` transitively pulls in `requests`, `http.client`, sockets and `ssl`.
Workflow code must replay to identical decisions, so the sandbox refuses to let network
machinery into the module. That is why the workflow dispatches activities **by string name** and
never imports them — the same property that lets the tests swap in doubles.

`config.py` carries a matching scar: an early version computed a default path with
`Path(__file__).resolve()` and every workflow test failed at worker startup, because a path that
resolves differently on another machine is exactly what makes a replay diverge. The fix is the
general one — keep the plain value in `config.py`, do the filesystem work in `store.py`, which
the workflow never imports.

Revert the change when you are done.

### 9 — Server-side schedules

```bash
uv run durable-ingest schedule create
uv run durable-ingest schedule describe     # recent runs, next run times
uv run durable-ingest schedule trigger      # fire now, without waiting
uv run durable-ingest schedule pause
uv run durable-ingest schedule unpause
uv run durable-ingest schedule delete
```

A schedule is an object **on the Temporal server**. Nothing in this repo is a cron and the
worker has no idea what time it is. Which means:

- the schedule keeps firing while every worker is down — runs queue and drain when one returns,
  rather than being silently missed
- pausing is server-side state, so it survives a deploy
- "did it fire on the 12th?" has a real answer

Try it: `schedule create`, then stop the worker for fifteen minutes, then start it again and
watch the queued runs drain.

**Every-N-minutes vs. once a day.** The default is an interval, because a demo wants several
runs while you watch. A real ingest usually wants a wall-clock time:

```bash
# once a day at 15:00 India time
HN_SCHEDULE_MODE=daily HN_SCHEDULE_AT=15:00 HN_SCHEDULE_TIMEZONE=Asia/Kolkata \
  uv run durable-ingest schedule create
```

A `ScheduleSpec`'s times are the **union** of its calendars, intervals and cron expressions, so
`_spec()` populates exactly one of them. The zone is the part people get wrong: a calendar spec
with no `time_zone_name` is evaluated by the server **in UTC**, so `hour=15` fires at 20:30 in
India. Naming the zone also means the schedule follows daylight saving, which a fixed offset
cannot.

Remember `create` never overwrites. To move an existing schedule to a new time, either
`schedule update` with the new env vars set, or `schedule delete` then `schedule create`.

Note `create` is **create-only** by design. Updating a schedule replaces its state as well as
its spec, and state includes `paused` — so a process that reconciled on every boot would
silently un-pause a schedule someone had deliberately switched off. `schedule update` exists as
a separate, deliberate act.

---

## Configuration

Every knob lives in `config.py` and is read from the environment. Defaults are chosen so a demo
run finishes in under a minute.

| Variable | Default | Meaning |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Where the dev server listens (the UI is 8233, not this). |
| `TEMPORAL_NAMESPACE` | `default` | The dev server ships with `default` already created. |
| `HN_TASK_QUEUE` | `durable-ingest` | The channel the worker polls and the workflow dispatches to. **Both sides must agree** — see experiment 7. |
| `HN_WORKFLOW_PREFIX` | `hn-top-stories` | Prefix for generated workflow ids. |
| `HN_SCHEDULE_ID` | `hn-top-stories-every-5m` | The schedule's id. |
| `HN_SCHEDULE_MODE` | `interval` | `interval` (every N minutes) or `daily` (one wall-clock time). An unknown value fails loudly. |
| `HN_SCHEDULE_INTERVAL_MINUTES` | `5` | How often it fires in `interval` mode. |
| `HN_SCHEDULE_AT` | `15:00` | `HH:MM`, used in `daily` mode. |
| `HN_SCHEDULE_TIMEZONE` | `Asia/Kolkata` | IANA zone the server evaluates `daily` in. **Omit it and Temporal uses UTC.** |
| `HN_BASE` | `https://hacker-news.firebaseio.com/v0` | The source API root. |
| `HN_HTTP_TIMEOUT` | `10` | Per-request timeout, seconds. |
| `HN_LIMIT` | `30` | Default story count. |
| `HN_BATCH_SIZE` | `10` | Default stories per activity. |
| `HN_MAX_CONCURRENT_ACTIVITIES` | `4` | Batches one worker runs at once. |
| `HN_DEST_ROOT` | `<repo>/_data` | Where landed files go. |

The `HN_*` prefix names the *source*, which is Hacker News; the project name is independent of it.

### Timeouts and the retry policy

Set in `workflows.py`, and worth understanding as a group:

| Setting | Value (batch) | Question it answers |
|---|---|---|
| `start_to_close` | 5 min | How long may **one attempt** take? |
| `schedule_to_close` | 20 min | How long may **all attempts together** take? |
| `heartbeat_timeout` | 30 s | How long may it go **without reporting progress**? |
| `maximum_attempts` | 4 | Bounded, not unlimited. |
| backoff | 1s, ×2, cap 10s | Retry spacing. |

The heartbeat is the one people skip, and it is the one that matters. It is what lets Temporal
tell a *slow* attempt from a *wedged* one: a batch crawling through a throttled API keeps
heartbeating and is left alone to finish, while a batch that has reported nothing for 30 seconds
is genuinely stuck and gets retried. A wall-clock deadline cannot distinguish those two, and
killing the slow-but-working case is how a healthy run gets failed.

`fetch_batch` heartbeats after **every story**, carrying the index reached as its payload.

---

## Tests

```bash
uv run pytest -q          # 34 passed in ~5s
```

No server, no network, no waiting — and that is a property of the design, not a convenience.
CI runs the same command on 3.11 and 3.13 (`.github/workflows/test.yml`).

### `tests/test_workflows.py` — the orchestration

Two things make a serverless workflow test possible:

1. **`WorkflowEnvironment.start_time_skipping()`** runs a real Temporal server in-process with a
   *fake clock*. Whenever every workflow is blocked on a timer, the environment fast-forwards —
   so the retry test exercises a genuine 1s → 2s → 4s backoff in milliseconds.
2. **Activities are replaced with test doubles**, registered under the same string names the
   workflow dispatches to. The workflow under test never touches the network.

What is asserted:

| Test | Contract |
|---|---|
| `test_fans_out_and_sums_the_batches` | 10 stories at batch size 4 → 3 batches, every story accounted for. |
| `test_a_transient_failure_is_retried_not_fatal` | Two failures then success; the workflow never notices. |
| `test_a_permanently_failing_batch_degrades_the_run_instead_of_sinking_it` | The `return_exceptions=True` contract: a partial run completes and returns a summary. |
| `test_progress_query_is_answerable_after_completion` | A query works against a **closed** workflow. |
| `test_batch_arithmetic` | Chunking edge cases, parametrised. |

### `tests/test_hn.py` — the error taxonomy

`requests.get` is stubbed, because what is under test is not "can we reach Hacker News" but
"given this response, does the caller learn the right thing about whether to retry". Get that
classification wrong in either direction and you either hammer a dead id four times or give up
on a blip.

| Asserted | Why it matters |
|---|---|
| 200 + `null` body → `NotFoundError` | The vendor quirk, normalised in one place. A dead id must not burn the retry budget. |
| `Timeout`, `ConnectionError`, 5xx → `TransientError` | Nothing about the request was wrong; retry. |
| 429 → `TransientError` with `retry_after` parsed | When a server says how long to wait, believe it. |
| 429 with no header → `retry_after is None` | Absent, not zero — zero reads as "retry immediately". |
| 400/401/403/404 → `SourceError`, **not** transient | Our request was wrong and will be wrong next time. |
| `NotFoundError` catchable as `SourceError` | The class hierarchy is part of the contract. |
| Non-list work-list / non-object item → `SourceError` | Shape guards fail at the boundary instead of five steps later. |
| The configured timeout reaches `requests.get` | A request with no timeout can hang forever, and a hung activity is worse than a failed one. |

### `tests/test_schedules.py` — when it fires

`_spec()` is a pure function of config, so no client and no server are needed. A wrong spec is
the kind of bug you would otherwise discover by nobody noticing the ingest never ran.

| Asserted | Why it matters |
|---|---|
| `daily` at `15:00` → one calendar, hour 15, minute 0, second 0 | The basic shape, and that no stray interval rides along. |
| The IANA zone reaches `time_zone_name` | Without it the server evaluates the calendar **in UTC** — 15:00 becomes 20:30 IST. |
| `"09:30"` parses both halves; `"15"` means on the hour | The `HH:MM` contract, including the lazy form. |
| `day_of_month` 1-31, `month` 1-12, `day_of_week` 0-6 | These defaults are what make it *daily* rather than one single date. |
| `interval` mode carries no calendar and no zone | An interval is a duration, not a time of day. |
| An unknown mode raises | A typo like `dialy` must not silently fall back to firing every 5 minutes. |

---

## Where data lands

```
_data/
  run=local/                 # from `durable-ingest local`
    8863.json
    ...
  run=20260825T142530/       # from a workflow; id is workflow.now(), not the wall clock
    8863.json
    ...
```

One JSON file per story per run, written atomically (temp file + `os.replace`). The atomicity is
not decoration: `exists()` is trusted as proof that the work is done, so a half-written file
that already answers "yes" would be skipped by the very retry meant to repair it.

In production this is an S3 key instead of a file path, and nothing else changes — which is what
lets a full pull be proven on a laptop before any bucket exists. (`PutObject` is atomic by
nature, so the production writer gets for free what the local one has to earn.)

Clear it with `uv run durable-ingest reset`.

---

## Design notes

### Read the code in this order

Each module depends only on the ones above it:

| File | Role | Temporal? |
|---|---|---|
| `config.py` | Every knob, read from the environment | imported into the sandbox — must stay inert |
| `hn.py` | The API client | **no** |
| `store.py` | Landing + the resume contract | **no** |
| `pipeline.py` | The actual work | **no** |
| `activities.py` | The I/O boundary | yes — starts here |
| `workflows.py` | The recipe; deterministic only | yes |
| `worker.py` | The long-poll process | yes |
| `schedules.py` | Server-side schedules | yes |
| `cli.py` | The operator surface | — |

That the first four have no Temporal import is enforced by convention here and by a CI job in
the production repos. It buys three things: you can run a full pull with no infrastructure, the
fetching layer is unit-testable in milliseconds, and the Temporal layer stays thin enough to
read in one sitting.

### How a failure is classified

`hn.py` divides errors into three kinds, because how a caller *classifies* a failure is the part
of an integration that actually matters:

| Class | Means | Caller should |
|---|---|---|
| `TransientError` | Timeout, connection reset, 5xx, 429 | Retry. Carries `retry_after` when the server sent one — guessing shorter than asked is how a rate limit becomes a ban. |
| `NotFoundError` | Nothing at that id | Skip and carry on. Retrying cannot help, so it must not burn the retry budget. |
| `SourceError` | A 4xx that is not 429 | Fail. Our request was wrong and will be wrong next time. |

A vendor quirk worth knowing: HN returns HTTP **200 with a literal `null` body** for a dead id,
not a 404. That is normalised in exactly one place (`hn.py`), which is where vendor weirdness
belongs.

### Why activities are `def`, not `async def`

Everything underneath blocks — HTTP calls, file writes, sleeps. An `async def` activity that
blocks holds the event loop for its whole duration, which stalls every other activity on the
worker *and* the heartbeats that are supposed to prove this one is alive. Sync activities run in
the worker's thread pool instead (created in `worker.py`, sized `concurrency + 1` so a short
activity can start while the long ones saturate the pool).

### Determinism rules the workflow follows

- `workflow.now()`, never `datetime.now()` — on replay the wall clock has moved on, and a real
  clock would produce a different value than the original run
- activities by string name, never imported
- no I/O, not even a "quick" filesystem check — that is what `count_landed` exists to demonstrate
- `dict.fromkeys()` for order-stable dedupe of signalled ids, not a `set`

### How this maps to real ingestion pipelines

| Here | Production |
|---|---|
| `hn.py` in-repo | A module in a shared connector library, imported |
| `_data/run=<id>/<id>.json` | An S3 key |
| `requests` as a direct dependency | Behind the connector library; CI fails on a direct import |
| Signals and queries | Not used by the ingest pipelines — but core Temporal, and this is the cheapest place to meet them |
| One dev server, one worker | A namespace per environment |

---

## Troubleshooting

**The workflow sits at Running forever and nothing happens.**
Task queue mismatch, nine times out of ten. Run `durable-ingest describe <id>` and compare
`task_queue` against what the worker logged at startup. See experiment 7.

**The worker fails at startup with "Failed validating workflow".**
Something non-deterministic reached the workflow sandbox — usually a new import in
`workflows.py`, or a filesystem/clock call in `config.py`. See experiment 8.

**`Connection refused` on 7233.**
The dev server is not running, or `TEMPORAL_ADDRESS` points elsewhere. Start it with
`temporal server start-dev --db-filename _temporal/dev.db`.

**Schedules or histories disappeared after restarting the server.**
You started it without `--db-filename`, so it was purely in-memory.

**The schedule fires but no runs execute.**
A schedule stores the task queue *at creation time*. If you changed `HN_TASK_QUEUE` since, the
schedule still targets the old one — `schedule delete`, then `schedule create`.

**`schedule create` says "already exists — left as it is".**
Working as intended; `create` never overwrites. Use `schedule update`, or delete first.

**A run reports `not_found` for some stories.**
Normal. Ids get deleted on HN between stage 1 and stage 2; the pipeline skips them without
burning retries.

---

## Project layout

```
.
├── pyproject.toml              # dist name, CLI entry point, pytest + ruff config
├── src/durable_ingest/
│   ├── __init__.py             # the reading order, restated
│   ├── config.py               # every knob (inert — imported into the sandbox)
│   ├── hn.py                   # the API client, and the error taxonomy
│   ├── store.py                # atomic writes, the resume contract
│   ├── pipeline.py             # the actual work — no Temporal
│   ├── activities.py           # the I/O boundary, heartbeats, injected failure
│   ├── workflows.py            # the recipe, timeouts, signals, queries
│   ├── worker.py               # long-poll process, thread pool
│   ├── schedules.py            # server-side schedules
│   └── cli.py                  # the operator surface
├── tests/
│   ├── test_workflows.py       # time-skipping env, activity doubles
│   ├── test_hn.py              # the error taxonomy, requests.get stubbed
│   └── test_schedules.py       # the schedule spec: interval vs daily, and the time zone
├── .github/workflows/test.yml  # pytest on 3.11 + 3.13, ruff check
├── _data/                      # landed JSON (gitignored)
└── _temporal/                  # dev server db + log (gitignored)
```

---

## License

MIT — see [LICENSE](LICENSE).
