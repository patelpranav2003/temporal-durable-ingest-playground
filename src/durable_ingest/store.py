"""Landing files on disk — and the resume contract that makes retries free.

THE WHOLE IDEA IN ONE SENTENCE: the destination path is a pure function of (run id, story id),
and we check whether it exists before doing any work. So a retried activity RESUMES instead of
repeating, a re-run costs one stat() per story instead of an HTTP call, and there is no
"have we done this already?" state to keep anywhere else.

In production this is an S3 key instead of a file path. Nothing else changes — which is why the
production repos can prove a full pull on a laptop before any bucket exists.
"""

import json
import logging
import os
import tempfile
from functools import lru_cache
from pathlib import Path

from . import config

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def dest_root() -> Path:
    """Where landed files live. Resolved LAZILY, and deliberately not in config.py.

    `Path.resolve()` is blocked inside the workflow sandbox — a path that resolves differently
    on another machine is precisely the kind of thing that makes a replay diverge. config.py is
    imported into the sandbox; this module is not, so the filesystem work belongs here.
    """
    if config.DEST_ROOT_ENV:
        return Path(config.DEST_ROOT_ENV).expanduser().resolve()
    return Path(__file__).resolve().parents[2] / "_data"


def run_dir(run_id: str) -> Path:
    return dest_root() / f"run={run_id}"


def key_for(run_id: str, item_id: int) -> Path:
    """The one and only destination for a story in a run.

    Derived purely from its inputs — no clock, no counter, no randomness. That is what makes it
    safe to recompute on a retry and get the same answer.
    """
    return run_dir(run_id) / f"{item_id}.json"


def exists(run_id: str, item_id: int) -> bool:
    return key_for(run_id, item_id).exists()


def write(run_id: str, item_id: int, payload: dict) -> Path:
    """Write one story, ATOMICALLY.

    The atomicity is not decoration. ``exists()`` is trusted as proof that the work is done, so a
    half-written file that already answers "yes" would be skipped by the very retry meant to
    repair it. Write to a temporary file in the same directory, then rename — rename is atomic
    within a filesystem, so the path either does not exist or holds a complete document.

    (S3's PutObject is atomic by nature, so the production writer gets this for free and the
    local one has to earn it. Same guarantee either way.)
    """
    path = key_for(run_id, item_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)  # atomic
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path


def landed(run_id: str) -> list[int]:
    """Which stories this run already holds. Used by the CLI to report, not by the activities."""
    d = run_dir(run_id)
    if not d.exists():
        return []
    return sorted(int(p.stem) for p in d.glob("*.json"))


def clear(run_id: str | None = None) -> int:
    """Delete landed data so a demo can be repeated. Returns the number of files removed."""
    target = run_dir(run_id) if run_id else dest_root()
    if not target.exists():
        return 0
    removed = 0
    for p in sorted(target.rglob("*.json")):
        p.unlink()
        removed += 1
    for d in sorted(target.rglob("*"), reverse=True):
        if d.is_dir():
            d.rmdir()
    if run_id is None and target.exists():
        target.rmdir()
    return removed
