"""Tests for the source client's ERROR TAXONOMY — the part of an integration that matters.

Nothing here touches the network. `requests.get` is replaced with a stub, because what is under
test is not "can we reach Hacker News" but "given this response, does the caller learn the right
thing about whether to retry".

The three answers, and why each is a separate class:

    TransientError   nothing about the request was wrong — retry
    NotFoundError    settled; retrying cannot change it — skip, and do not spend the budget
    SourceError      our request was wrong and will be wrong next time — fail

Get that classification wrong in either direction and you either hammer a dead id four times or
give up on a blip.
"""

import pytest
import requests

from durable_ingest import hn


class FakeResponse:
    """The narrow slice of `requests.Response` that `hn._get` actually touches."""

    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


@pytest.fixture
def fake_get(monkeypatch):
    """Install a stub for `requests.get` and hand back the recorded call arguments."""
    calls = []

    def install(response=None, raises=None):
        def _get(url, timeout=None):
            calls.append({"url": url, "timeout": timeout})
            if raises is not None:
                raise raises
            return response

        monkeypatch.setattr(hn.requests, "get", _get)
        return calls

    return install


# --- the happy paths ------------------------------------------------------------


def test_top_story_ids_truncates_to_the_limit(fake_get):
    fake_get(FakeResponse(payload=list(range(500))))
    assert hn.top_story_ids(5) == [0, 1, 2, 3, 4]


def test_top_story_ids_coerces_to_int(fake_get):
    """HN returns numbers, but a proxy or a cache could hand back strings."""
    fake_get(FakeResponse(payload=["8863", "8864"]))
    assert hn.top_story_ids(2) == [8863, 8864]


def test_fetch_item_returns_the_object(fake_get):
    fake_get(FakeResponse(payload={"id": 8863, "title": "My YC app: Dropbox"}))
    assert hn.fetch_item(8863)["title"] == "My YC app: Dropbox"


def test_the_request_carries_the_configured_timeout(fake_get):
    """A request with no timeout can hang forever, and a hung activity is worse than a failed one."""
    calls = fake_get(FakeResponse(payload={"id": 1}))
    hn.fetch_item(1)
    assert calls[0]["timeout"] == hn.config.HTTP_TIMEOUT_SECONDS
    assert calls[0]["url"].endswith("/item/1.json")


# --- settled: retrying cannot help ----------------------------------------------


def test_a_null_body_is_not_found_not_a_crash(fake_get):
    """THE VENDOR QUIRK: HN answers 200 with a literal `null` for a dead id, never a 404.

    Normalised here so no caller has to know it.
    """
    fake_get(FakeResponse(status_code=200, payload=None))
    with pytest.raises(hn.NotFoundError):
        hn.fetch_item(999_999_999)


def test_not_found_is_catchable_as_a_source_error(fake_get):
    """The class hierarchy is part of the contract: one `except` can still catch everything."""
    fake_get(FakeResponse(payload=None))
    with pytest.raises(hn.SourceError):
        hn.fetch_item(1)


# --- transient: worth retrying ---------------------------------------------------


def test_a_timeout_is_transient(fake_get):
    fake_get(raises=requests.Timeout("too slow"))
    with pytest.raises(hn.TransientError):
        hn.fetch_item(1)


def test_a_connection_failure_is_transient(fake_get):
    fake_get(raises=requests.ConnectionError("reset by peer"))
    with pytest.raises(hn.TransientError):
        hn.fetch_item(1)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_5xx_is_transient(fake_get, status):
    fake_get(FakeResponse(status_code=status))
    with pytest.raises(hn.TransientError):
        hn.fetch_item(1)


def test_a_429_carries_the_servers_own_retry_after(fake_get):
    """When a server says how long to wait, believe it — guessing shorter is how a limit becomes a ban."""
    fake_get(FakeResponse(status_code=429, headers={"Retry-After": "42"}))
    with pytest.raises(hn.TransientError) as excinfo:
        hn.fetch_item(1)
    assert excinfo.value.retry_after == 42


def test_a_429_without_the_header_leaves_retry_after_unset(fake_get):
    """Absent, not zero. Zero would read as "retry immediately", which is the opposite of the ask."""
    fake_get(FakeResponse(status_code=429))
    with pytest.raises(hn.TransientError) as excinfo:
        hn.fetch_item(1)
    assert excinfo.value.retry_after is None


# --- our fault: identical next time ---------------------------------------------


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_a_4xx_that_is_not_429_is_permanent(fake_get, status):
    """Not a TransientError — retrying a malformed request just spends the budget to fail again."""
    fake_get(FakeResponse(status_code=status, text="nope"))
    with pytest.raises(hn.SourceError) as excinfo:
        hn.fetch_item(1)
    assert not isinstance(excinfo.value, hn.TransientError)


# --- shape guards ---------------------------------------------------------------
#
# Cheap assertions on someone else's response shape. They turn a confusing failure five steps
# later into an obvious one at the boundary.


def test_a_work_list_that_is_not_a_list_fails_at_the_boundary(fake_get):
    fake_get(FakeResponse(payload={"error": "unexpected"}))
    with pytest.raises(hn.SourceError, match="expected a list"):
        hn.top_story_ids(10)


def test_an_item_that_is_not_an_object_fails_at_the_boundary(fake_get):
    fake_get(FakeResponse(payload=[1, 2, 3]))
    with pytest.raises(hn.SourceError, match="expected an object"):
        hn.fetch_item(1)
