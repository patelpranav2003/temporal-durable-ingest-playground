"""durable-ingest — a hands-on Temporal playground over the public Hacker News API.

Read the modules in this order; each one only depends on the ones above it:

    config.py      every knob
    hn.py          the API client        — no Temporal
    store.py       landing + resume      — no Temporal
    pipeline.py    the actual work       — no Temporal
    activities.py  the I/O boundary      — Temporal starts here
    workflows.py   the recipe            — deterministic only
    worker.py      the long-poll process
    schedules.py   server-side schedules
    cli.py         the operator surface
"""

__version__ = "0.1.0"
