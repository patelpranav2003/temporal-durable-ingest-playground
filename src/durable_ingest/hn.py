"""The Hacker News client — plain Python, no Temporal anywhere in this file.

That absence is the point, and it is the same rule the production repos enforce with a CI job.
Keeping the fetching layer free of Temporal imports means:

  * you can run a full pull from a terminal with no server, no worker, nothing (``durable-ingest local``);
  * these functions are unit-testable in milliseconds instead of through a workflow environment;
  * the Temporal layer on top stays a thin wrapper you can read in one sitting.

In a real source repo this file would not exist at all — it would be a module in the shared
connector library, imported here. The error classes below deliberately mirror that library's
contract, because HOW A CALLER CLASSIFIES A FAILURE is the part of an integration that matters.
"""

import logging

import requests

from . import config

logger = logging.getLogger(__name__)


class SourceError(Exception):
    """Base class, so a caller can catch everything this module raises."""


class TransientError(SourceError):
    """Worth trying again: a timeout, a connection reset, a 5xx.

    The retry-able half of the contract. Nothing about the request was wrong.
    """

    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        # When a server tells you how long to wait, believe it. Guessing shorter than
        # asked is how a rate limit becomes a ban.
        self.retry_after = retry_after


class NotFoundError(SourceError):
    """The answer is settled: there is nothing at that id.

    Deterministic, so retrying cannot help. A caller must skip it and carry on rather than
    burn its retry budget. HN returns a literal ``null`` body for a dead id — a 200, not a 404,
    which is exactly the kind of vendor quirk that belongs in one place.
    """


def _get(path: str) -> object:
    url = f"{config.HN_BASE}/{path}"
    try:
        response = requests.get(url, timeout=config.HTTP_TIMEOUT_SECONDS)
    except requests.Timeout as e:
        raise TransientError(f"timeout on {path}") from e
    except requests.RequestException as e:
        raise TransientError(f"connection failure on {path}: {e}") from e

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        raise TransientError(
            "rate limited", retry_after=int(retry_after) if retry_after else None
        )
    if response.status_code >= 500:
        raise TransientError(f"HTTP {response.status_code} on {path}")
    if response.status_code >= 400:
        # A 4xx that is not 429 is a request problem — our fault, and identical next time.
        raise SourceError(f"HTTP {response.status_code} on {path}: {response.text[:200]}")

    return response.json()


def top_story_ids(limit: int) -> list[int]:
    """Stage 1: the work-list.

    Returns the current top-story ids, newest ranking first. HN returns ~500; we take the first
    ``limit`` so a demo run stays short.

    THIS IS DISCOVERY, NOT CONFIGURATION. No story id is written down anywhere in this project.
    A story that trends after this call is simply picked up by the next run.
    """
    ids = _get("topstories.json")
    if not isinstance(ids, list):
        # A guard on the shape of someone else's response. Cheap, and it turns a confusing
        # failure five steps later into an obvious one here.
        raise SourceError(f"expected a list of ids, got {type(ids).__name__}")
    return [int(i) for i in ids[:limit]]


def fetch_item(item_id: int) -> dict:
    """Stage 2: one story.

    Raises NotFoundError for a dead id, so the caller can skip it without retrying.
    """
    item = _get(f"item/{item_id}.json")
    if item is None:
        raise NotFoundError(f"item {item_id} does not resolve")
    if not isinstance(item, dict):
        raise SourceError(f"item {item_id}: expected an object, got {type(item).__name__}")
    return item
