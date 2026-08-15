"""Published-link liveness: every ``morzecrew.github.io`` URL the corpus cites is alive.

Split from the offline checks because it fails for reasons a pull request does not
control — a page renamed in ``pages/``, a mike alias moved — and because it is the one
check here that touches the network. It runs on a schedule and fails that job.

The naive form of this check is actively harmful and was measured to be so: sweeping
every URL without pacing returns a uniform wall of 502s, including the site root, which
is a 100% false-positive rate and trains everyone to ignore the check inside a week.
Three things keep it honest, and they are not the same thing:

- **Pacing** — a delay between requests, so the sweep does not look like an attack.
- **A timeout on every request** — pacing bounds the rate, never the duration. A request
  with no timeout can hang on a stalled socket until the CI job's own timeout kills it,
  which spends hours of a runner to learn nothing.
- **A bounded retry budget** — this check's whole rationale is that its transient
  failure rate is high. Retrying is what separates "the page is gone" from "the CDN
  hiccuped"; without it the pacing only slows down the false positives.

A 404 is not retried. The page being absent is the answer, and spending the budget on it
delays the report without changing it.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from .checks import PUBLISHED_URL_PATTERN
from .corpus import Corpus

Fetcher = Callable[[str, float], "tuple[int | None, str]"]
"""A single GET: returns the HTTP status, or ``None`` when no response was produced."""

# ----------------------- #

DEFAULT_PACING_SECONDS = 0.4
DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_BUDGET_SECONDS = 900.0
"""Wall-clock bound on a whole sweep. A green run takes well under a minute; this only
bites when nearly every request is timing out, which is when a report is worth most."""

_USER_AGENT = "forze-skills-check (+https://github.com/morzecrew/forze)"
_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
_PERMITTED_SCHEMES = frozenset({"http", "https"})


@dataclass(frozen=True)
class LinkOutcome:
    """What happened to one URL after its retry budget was spent."""

    url: str
    status: int | None
    detail: str
    attempts: int

    @property
    def ok(self) -> bool:
        return self.status == 200

    @property
    def checked(self) -> bool:
        """Whether the sweep got to this URL at all.

        An unchecked URL is not a dead one and not a live one, and collapsing it into
        either loses the only fact that matters about it.
        """
        return self.attempts > 0


@dataclass(frozen=True)
class LinkPolicy:
    """Pacing, per-request duration bound, retry budget, and a bound on the whole sweep.

    The first three bound one request; none of them bounds the run. Multiply them out and
    the worst case is real: 64 URLs x (3 attempts x 15s + 2s + 4s backoff) plus pacing is
    about 55 minutes, which a total outage reaches exactly. A sweep that only prints after
    finishing is then killed by the CI job's own limit having reported nothing at all —
    the run costs an hour and produces no information.

    ``budget_seconds`` is what makes the sweep's duration a property of this policy rather
    than of whichever runner limit happens to kill it. When it runs out the remaining URLs
    are reported **unchecked** and the sweep fails: unchecked is not passed, exactly as an
    unresolved import is not a resolved one.
    """

    pacing_seconds: float = DEFAULT_PACING_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    attempts: int = DEFAULT_ATTEMPTS
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS
    budget_seconds: float = DEFAULT_BUDGET_SECONDS


def collect_published_urls(corpus: Corpus) -> tuple[str, ...]:
    """Every distinct published-docs URL in the corpus, trailing slash normalized.

    Read from the raw text rather than from parsed links, because a URL written bare in
    prose is as much a claim about a live page as one inside a Markdown link.
    """
    found: set[str] = set()

    for doc in corpus.documents:
        for match in PUBLISHED_URL_PATTERN.finditer(doc.text):
            found.add(_normalize(match.group(0)))

    return tuple(sorted(found))


def check_liveness(
    urls: tuple[str, ...],
    policy: LinkPolicy | None = None,
    fetcher: Fetcher | None = None,
) -> list[LinkOutcome]:
    """Fetch every URL, pacing between them and retrying transient failures.

    ``fetcher`` is the seam the tests drive: pacing, retry classification and the
    404-is-final rule are policy this repository owns, and proving them against the live
    site would be both slow and a network flake away from a red build.
    """
    settings = policy or LinkPolicy()
    fetch = fetcher or _fetch
    outcomes: list[LinkOutcome] = []
    started = time.monotonic()

    for index, url in enumerate(urls):
        if time.monotonic() - started >= settings.budget_seconds:
            # Everything from here on is reported, never dropped. A sweep that stopped
            # early and said so is a partial answer; one that stopped early and returned
            # the outcomes it happened to collect is a wrong answer.
            outcomes.extend(
                LinkOutcome(url=remaining, status=None, detail="sweep budget exhausted", attempts=0)
                for remaining in urls[index:]
            )

            return outcomes

        if index and settings.pacing_seconds > 0:
            time.sleep(settings.pacing_seconds)

        outcomes.append(_fetch_with_retries(url, settings, fetch))

    return outcomes


# ----------------------- #


def _fetch_with_retries(url: str, policy: LinkPolicy, fetch: Fetcher) -> LinkOutcome:
    status: int | None = None
    detail = "not attempted"
    attempt = 0

    while attempt < policy.attempts:
        attempt += 1
        status, detail = fetch(url, policy.timeout_seconds)

        if status == 200:
            return LinkOutcome(url=url, status=status, detail=detail, attempts=attempt)

        if status is not None and status not in _RETRYABLE_STATUS:
            return LinkOutcome(url=url, status=status, detail=detail, attempts=attempt)

        if attempt < policy.attempts and policy.backoff_seconds > 0:
            time.sleep(policy.backoff_seconds * attempt)

    return LinkOutcome(url=url, status=status, detail=detail, attempts=attempt)


def _fetch(url: str, timeout: float) -> tuple[int | None, str]:
    """One GET. ``None`` status means the request never produced an HTTP response.

    The standard library expresses the connect and read bounds as a single socket
    timeout, which applies to establishing the connection and to every subsequent read.
    Both are therefore bounded; they simply share one number.
    """
    # `urlopen` honours `file:`, `ftp:` and any scheme a handler is registered for, so a
    # URL that reached here from somewhere other than the corpus could read the local
    # filesystem and report it as a live link. Today's only caller matches against a
    # pattern anchored to `https://<host>/`, which makes that unreachable — enforcing it
    # here rather than relying on that keeps the guarantee with the function that needs
    # it, instead of with whoever calls it next.
    if urlsplit(url).scheme not in _PERMITTED_SCHEMES:
        return None, f"refusing to fetch a non-HTTP(S) URL: {url}"

    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT}, method="GET")

    try:
        # The scheme allowlist above is the audit B310 asks for: it runs on every call and
        # returns before reaching here, which a syntactic rule cannot see.
        with urllib.request.urlopen(request, timeout=timeout) as response:  # nosec B310
            response.read()

            return int(response.status), "ok"
    except urllib.error.HTTPError as error:
        return int(error.code), error.reason or "http error"
    # A transport failure produced no HTTP response at all, so it has no status.
    except Exception as error:
        return None, f"{type(error).__name__}: {error}"


def _normalize(url: str) -> str:
    """Strip a trailing ``.``/``,`` picked up from prose and any fragment."""
    split = urlsplit(url.rstrip(".,;:"))

    return urlunsplit((split.scheme, split.netloc, split.path, split.query, ""))
