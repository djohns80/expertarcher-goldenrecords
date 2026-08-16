"""Prepare ExpertArcher scores for submission to the Golden Records Scores API.

Fetches the list of scores from the ExpertArcher API and maps each one onto
the fields the Golden Records API expects, using reference data for rounds,
bow classes, age groups and members. Records that cannot be mapped are skipped
and explained in a summary report so they can be fixed at source.

Pipeline (see main() at the bottom):
  1. Fetch reference data from Golden Records (fetch_resource) into the
     golden-records/ files, unless already present. These give us the ids.
  2. Fetch the scores to process from ExpertArcher (fetch_scores), optionally
     filtered by archer name (--include-name / --exclude-name, filter_by_name).
  3. Map each score to a Golden Records record (transform_score), resolving
     round / bow-type / member / age-group names to ids (resolve).
  4. Report what mapped and what was skipped, and why (print_report).
  5. Submit the mapped records to Golden Records (submit_records), unless
     --dry-run was given.
  6. Report the submission outcome (report_submission): accepted / duplicate /
     error counts, errors grouped by type, with full per-record detail written
     to the error log (--error-log) for review.

Scores come from the ExpertArcher API (the [expertarcher] section of
config.toml); the reporting window is set with --from / --to. The Golden
Records reference files (rounds.json, bowtypes.json, age-groups.json,
members.json) are downloaded from the API when missing (or with --refresh),
using the [api] section. API keys are read from the environment, never a file
(see the ENV_* constants). See build_arg_parser for all options.

After reporting, the mapped records are POSTed to the Golden Records Scores API
(POST /scores, one record per request); skipped records are never submitted.
Rejections continue past (one bad record does not block the rest) and are split
into duplicates (already submitted -- benign) and genuine errors. This writes
live data, so runs submit by default -- pass --dry-run to fetch, map and report
without POSTing anything.

For a large historical import, --csv PATH instead writes the mapped records to
a Golden Records CSV bulk-import file (and submits nothing): one upload avoids
the API's 200-requests/hour throttle. The same records feed either output --
api_record() renders the JSON body, csv_row() the CSV row.
README.md has the full integration overview, field mapping and setup.
"""

import argparse
import csv
import json
import os
import time
import tomllib
from collections import Counter, defaultdict, namedtuple
from datetime import datetime

# Default input locations, relative to the working directory. All are
# overridable on the command line (see build_arg_parser). The reference data
# downloaded from Golden Records lives in its own directory, separate from the
# ExpertArcher input and local config.
GR_DIR = "golden-records"
DEFAULT_MEMBERS = os.path.join(GR_DIR, "members.json")
DEFAULT_AGE_GROUPS = os.path.join(GR_DIR, "age-groups.json")
DEFAULT_ROUNDS = os.path.join(GR_DIR, "rounds.json")
DEFAULT_BOWTYPES = os.path.join(GR_DIR, "bowtypes.json")
DEFAULT_MAPPINGS = "mappings.toml"
DEFAULT_CONFIG = "config.toml"
# Full detail of any rejected submissions is appended here for separate review;
# the report itself only shows counts. Overridable with --error-log.
DEFAULT_ERROR_LOG = "submission-errors.log"

# Numeric score fields that must be present and integer-parseable.
NUMERIC_FIELDS = ("score", "hits", "golds")

# Constant tag written to every submitted record's `user_1` field, identifying
# the ExpertArcher integration as the source of the score.
USER_1_TAG = "Expert Archer"

# Golden Records ScoreStatusOptions enum. The `status` field is sent as one of
# these integer codes (the string names are rejected).
STATUS_PRACTICE = 1
STATUS_CLUB_EVENT = 2
STATUS_CLUB_COMPETITION = 3
STATUS_OPEN_COMPETITION = 4

# The JSON API sends `status` as the enum integer above; the CSV bulk-import
# format uses the equivalent text label instead. Same mapping, two renderings.
STATUS_LABELS = {
    STATUS_PRACTICE: "Practice",
    STATUS_CLUB_EVENT: "Club Event",
    STATUS_CLUB_COMPETITION: "Club Competition",
    STATUS_OPEN_COMPETITION: "Open Competition",
}

# Column order of the Golden Records CSV bulk-import file (from their sample
# score-records.csv). The file ends with a single `END` sentinel row. Columns
# we have no ExpertArcher source for are written blank (Golden Records fills
# Handicap/Classification on import; Affiliation/Verified are optional).
CSV_HEADER = [
    "Date", "Name", "Score", "Hits", "Golds", "Xs", "Handicap", "Classification",
    "Location", "Class", "Age Group", "Round", "Type", "Record Status",
    "Qualifying", "Status", "Affiliation", "Affiliation Expiry",
    "Affiliation ID", "Verified",
]

# The Scores API returns this exact message (in the response body's `errors`
# list) when a matching score is already present. It is not a real failure --
# it just means the score was submitted before -- so it is counted and reported
# separately from genuine errors (see submit_records / report_submission).
DUPLICATE_ERROR = "This score already exists in the database."

# API keys are read from the environment, never from a file, so they stay out
# of source control. For Golden Records set the API key (preferred) or the
# username/password pair for Basic Auth; for ExpertArcher set the API key.
ENV_API_KEY = "GOLDEN_RECORDS_API_KEY"
ENV_USERNAME = "GOLDEN_RECORDS_USERNAME"
ENV_PASSWORD = "GOLDEN_RECORDS_PASSWORD"
ENV_EA_API_KEY = "EXPERTARCHER_API_KEY"

# All the reference lookups needed to map one score, resolved once at startup.
#   name_map:  ExpertArcher name -> Golden Records name (from mappings.toml).
#              A single generic map; the field being resolved (round vs bow
#              type) decides which id table the result is looked up in.
#   round_ids/class_ids: lower-cased Golden Records name -> a details dict
#              carrying the canonical name and id (and, for rounds, the
#              Indoor/Outdoor type). Keyed lower-case for case-insensitive
#              lookup (see resolve / load_rounds / load_bowtypes).
#   classification_map: ExpertArcher classification -> Golden Records
#              classification name, for the CSV Classification column
#              (from mappings.toml); unlisted classifications map to blank.
Lookups = namedtuple(
    "Lookups",
    "members age_groups name_map round_ids class_ids classification_map",
)

# One fully-resolved score, independent of the output format. transform_score
# produces these; api_record() renders one as the JSON body for POST /scores,
# and csv_row() renders one as a Golden Records CSV import row. Sharing this
# intermediate keeps both outputs (and the skip/validation logic) in step.
#   *_id / *_name: the resolved Golden Records id and its canonical name (the
#                  API uses the ids, the CSV uses the names).
#   date:          a datetime, formatted per target (ISO for the API,
#                  DD/MM/YYYY for the CSV).
#   status:        the ScoreStatusOptions enum int (STATUS_LABELS maps it to
#                  the CSV text label).
#   handicap/classification: optional CSV-only columns copied from ExpertArcher
#                  when present (blank otherwise); the API has no such fields.
Mapped = namedtuple(
    "Mapped",
    "member_id member_name round_id round_name round_type class_id class_name "
    "age_group_id age_group_name date score hits golds xs location status "
    "record_status qualifying record_qualifying handicap classification",
)

# One rejected submission: its position in the submitted list, the API's
# `errors` messages (used to categorise it), the full error message (for the
# log) and the record we sent (for context in the log).
Failure = namedtuple("Failure", "index errors message record")

# Outcome of a submission run: how many were accepted, which were duplicates
# (benign) and which were genuine errors. duplicates/errors are lists of
# Failure (see submit_records).
SubmitResult = namedtuple("SubmitResult", "submitted duplicates errors")


def load_json(path):
    """Load and return the JSON document at path (UTF-8)."""
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def load_age_groups(path):
    """Return a mapping of age-group name -> age_group_id."""
    return {row["age_group"]: row["age_group_id"] for row in load_json(path)}


def load_members(path):
    """Return a mapping of member name -> member_id from the members JSON."""
    return {row["name"]: row["member_id"] for row in load_json(path)}


def load_rounds(path):
    """Return a mapping of lower-cased round name -> its details.

    Each value is {"name", "id", "type"}: the canonical Golden Records name
    (preserving its casing), the round_id, and the Indoor/Outdoor type (used
    for the CSV `Type` column). Keyed by lower-cased name so lookups are
    case-insensitive (see `resolve`).
    """
    return {row["round"].lower(): {"name": row["round"],
                                   "id": row["round_id"],
                                   "type": row.get("type", "")}
            for row in load_json(path)}


def load_bowtypes(path):
    """Return a mapping of lower-cased bow class name -> its details.

    Each value is {"name", "id"}: the canonical Golden Records name (preserving
    its casing) and the class_id. Keyed by lower-cased name so lookups are
    case-insensitive (see `resolve`).
    """
    return {row["bow_class"].lower(): {"name": row["bow_class"],
                                       "id": row["class_id"]}
            for row in load_json(path)}


def load_mappings(path):
    """Load the ExpertArcher -> Golden Records mappings (TOML).

    Returns (name_map, classification_map):
      - name_map: round & bow-type name overrides (the [names] table). The code
        resolves each mapped name against the appropriate id table by context.
        A legacy flat file (entries at the top level, no [names] header) is
        still accepted.
      - classification_map: ExpertArcher classification -> Golden Records
        classification name (the [classifications] table), for the CSV import's
        Classification column; an unlisted classification is left blank.
    """
    with open(path, "rb") as file:  # tomllib requires a binary stream
        data = tomllib.load(file)
    if "names" in data:
        name_map = data["names"]
    else:  # legacy flat file: top-level string entries are the name map
        name_map = {key: value for key, value in data.items()
                    if isinstance(value, str)}
    return name_map, data.get("classifications", {})


def load_config(path):
    """Load the API config (TOML) for both APIs, or return {} if absent."""
    if not os.path.exists(path):
        return {}
    with open(path, "rb") as file:
        return tomllib.load(file)


class RateLimiter:
    """Enforces a minimum interval between successive requests.

    Golden Records throttles to 1 request/second (and 20/minute, 200/hour).
    Spacing requests at least `min_interval` apart satisfies the per-second
    limit; a single run makes only a handful of requests, so the minute/hour
    limits are not a concern (the retry logic in `_request_with_retry` handles them
    if they ever are). One shared instance spaces every request in a run --
    across both the Golden Records downloads and the ExpertArcher scores fetch.
    """

    def __init__(self, min_interval):
        self.min_interval = min_interval
        self._last = None

    def wait(self):
        if self.min_interval > 0 and self._last is not None:
            remaining = self.min_interval - (time.monotonic() - self._last)
            if remaining > 0:
                time.sleep(remaining)
        self._last = time.monotonic()


def fetch_resource(config, endpoint_key, limiter, verbose=False):
    """Download a full reference resource from the Golden Records API.

    Pages through the endpoint (authenticated with the configured API key or
    Basic Auth credentials) accumulating the JSON list from each page until the
    last page is reached, then returns the combined list.
    Requests are spaced by `limiter` and retried with backoff on 429/5xx
    responses. `requests` is imported lazily so the tool still runs offline
    when the local files are already present.
    """
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The 'requests' package is required to download reference data. "
            "Install dependencies with `uv sync`."
        ) from exc

    api = config.get("api", {})
    base_url = api.get("base_url", "").rstrip("/")
    endpoint = api.get("endpoints", {}).get(endpoint_key, "")
    auth_kwargs = _build_auth()
    if not (base_url and endpoint and auth_kwargs):
        raise RuntimeError(
            f"Golden Records API is not configured for '{endpoint_key}'. "
            f"Set base_url and endpoints.{endpoint_key} in {DEFAULT_CONFIG}, and "
            f"credentials in the environment (${ENV_API_KEY}, or "
            f"${ENV_USERNAME}/${ENV_PASSWORD})."
        )

    pg = api.get("pagination", {})
    page_param = pg.get("page_param", "page")
    size_param = pg.get("size_param", "pageSize")
    page_size = pg.get("page_size", 100)
    page = pg.get("start_page", 1)

    th = api.get("throttle", {})
    max_retries = th.get("max_retries", 5)
    backoff = th.get("backoff_seconds", 2.0)

    url = base_url + endpoint
    records = []
    while True:
        response = _request_with_retry(
            requests, "GET", url, auth_kwargs, limiter, endpoint_key,
            max_retries, backoff,
            params={page_param: page, size_param: page_size}, verbose=verbose,
        )
        batch = response.json()
        records.extend(batch)
        if _is_last_page(response, page, batch, page_size):
            break
        page += 1
    return records


def fetch_scores(config, limiter, date_from=None, date_to=None, verbose=False):
    """Fetch the scores to process from the ExpertArcher API.

    The API key is sent as a query-string parameter (not a header, unlike
    Golden Records), alongside the fixed parameters from config (e.g. club,
    select) and an optional from/to date range. Returns the JSON list of score
    records, matching the shape the rest of the pipeline expects. `requests` is
    imported lazily so the "not installed" message is friendly.
    """
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The 'requests' package is required to fetch scores. "
            "Install dependencies with `uv sync`."
        ) from exc

    ea = config.get("expertarcher", {})
    base_url = ea.get("base_url", "").rstrip("/")
    endpoint = ea.get("endpoint", "")
    key_param = ea.get("key_param", "apikey")
    api_key = os.environ.get(ENV_EA_API_KEY, "")
    if not (base_url and endpoint and api_key):
        raise RuntimeError(
            f"ExpertArcher API is not configured. Set expertarcher.base_url and "
            f"expertarcher.endpoint in {DEFAULT_CONFIG}, and ${ENV_EA_API_KEY} in "
            f"the environment."
        )

    # Key first, then the fixed params from config, then the optional window.
    params = {key_param: api_key}
    params.update(ea.get("params", {}))
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to

    # Reuse the shared retry policy; the endpoint is not paged.
    th = config.get("api", {}).get("throttle", {})
    response = _request_with_retry(
        requests, "GET", base_url + endpoint, {}, limiter, "scores",
        th.get("max_retries", 5), th.get("backoff_seconds", 2.0), params=params,
        verbose=verbose,
    )
    return response.json()


def submit_records(config, records, limiter, verbose=False):
    """POST each mapped record to the Golden Records Scores API.

    One record per request (the API's contract), authenticated like the
    reference fetches, spaced by the shared limiter and retried on 429/5xx.
    Submission continues past individual rejections so one bad record does not
    block the rest. Rejections are split into duplicates (the score was already
    submitted -- benign) and genuine errors, so the report can treat them
    differently (see report_submission). Returns a SubmitResult.

    This WRITES LIVE DATA to Golden Records. It runs on every invocation except
    --dry-run (see main), so the script should only be run deliberately.
    """
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The 'requests' package is required to submit scores. "
            "Install dependencies with `uv sync`."
        ) from exc

    api = config.get("api", {})
    base_url = api.get("base_url", "").rstrip("/")
    endpoint = api.get("endpoints", {}).get("scores", "")
    auth_kwargs = _build_auth()
    if not (base_url and endpoint and auth_kwargs):
        raise RuntimeError(
            f"Golden Records API is not configured for submission. Set base_url "
            f"and endpoints.scores in {DEFAULT_CONFIG}, and credentials in the "
            f"environment (${ENV_API_KEY}, or ${ENV_USERNAME}/${ENV_PASSWORD})."
        )

    th = api.get("throttle", {})
    max_retries = th.get("max_retries", 5)
    backoff = th.get("backoff_seconds", 2.0)

    url = base_url + endpoint
    submitted = 0
    duplicates = []
    errors = []
    for index, record in enumerate(records):
        try:
            _request_with_retry(requests, "POST", url, auth_kwargs, limiter,
                                "scores", max_retries, backoff, json=record,
                                verbose=verbose)
            submitted += 1
        except ApiError as exc:
            failure = Failure(index, exc.errors, str(exc), record)
            if DUPLICATE_ERROR in exc.errors:
                duplicates.append(failure)
            else:
                errors.append(failure)
        except RuntimeError as exc:
            # Transport-level failure with no response body to categorise.
            errors.append(Failure(index, [], str(exc), record))
    return SubmitResult(submitted, duplicates, errors)


def _build_auth():
    """Build the requests keyword arguments carrying the API credentials.

    Credentials come from the environment (see the ENV_* constants), never
    from a file. Two schemes are supported, API key taking precedence when
    set:
      - API key: sent as `Authorization: Basic <api_key>` -- the key takes the
        place of the usual base64 username:password value. Unlike Basic Auth,
        a club-level key returns the whole membership rather than only the
        caller's own record.
      - HTTP Basic Auth: a username/password pair.
    Returns a dict to splat into requests.get (e.g. {"headers": {...}} or
    {"auth": (user, pass)}), or {} if no credentials are set.
    """
    api_key = os.environ.get(ENV_API_KEY, "")
    if api_key:
        return {"headers": {"Authorization": f"Basic {api_key}"}}
    username = os.environ.get(ENV_USERNAME, "")
    if username:
        return {"auth": (username, os.environ.get(ENV_PASSWORD, ""))}
    return {}


class ApiError(RuntimeError):
    """An API request that failed with an HTTP error response.

    Subclasses RuntimeError so existing `except RuntimeError` handlers keep
    working, but also carries the parsed detail (status code and the response
    body's `errors` list) so callers can categorise failures -- e.g. tell a
    duplicate submission apart from a validation error -- rather than only
    string-matching the message.
    """

    def __init__(self, message, status_code=None, errors=None):
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors or []


def _extract_errors(response):
    """Pull the `errors` list out of a Golden Records error response body.

    The API returns validation/failure detail as {"errors": ["...", ...]}.
    Returns a list of message strings (empty if the body has none or is not
    JSON), used to categorise submission failures.
    """
    try:
        body = response.json()
    except ValueError:
        return []
    errors = body.get("errors") if isinstance(body, dict) else None
    if isinstance(errors, list):
        return [str(item) for item in errors]
    if isinstance(errors, str):
        return [errors]
    return []


def _request_with_retry(requests, method, url, auth_kwargs, limiter, label,
                        max_retries, backoff, params=None, json=None,
                        verbose=False):
    """Make one HTTP request, waiting on the limiter and retrying 429/5xx.

    Shared by the GET fetches and the POST submissions. `auth_kwargs` carries
    the credentials (from `_build_auth`) and is splatted into the request;
    `params` sets the query string and `json` the request body. Retries honour
    a `Retry-After` header (seconds) when the server sends one, otherwise use
    exponential backoff. An HTTP error response (e.g. 4xx other than 429, or a
    5xx that outlasts the retries) is raised as an ApiError carrying the status
    code and the response body's `errors` list; its message also includes the
    body, which is where Golden Records puts its detail and which
    raise_for_status() would otherwise drop. A transport-level failure with no
    response (e.g. connection error) is raised as a plain RuntimeError. With
    `verbose`, every request's method/URL/status/body is logged.
    """
    for attempt in range(max_retries + 1):
        limiter.wait()
        try:
            response = requests.request(method, url, params=params, json=json,
                                        timeout=30, **auth_kwargs)
        except requests.RequestException as exc:
            if attempt < max_retries:
                time.sleep(backoff * (2 ** attempt))
                continue
            raise RuntimeError(f"{method} {url} ({label}) failed: {exc}") from exc

        if verbose:
            print(f"  [http] {method} {response.url} -> {response.status_code} "
                  f"{_body_snippet(response)}")

        if response.status_code == 429 or response.status_code >= 500:
            if attempt < max_retries:
                time.sleep(_retry_delay(response, attempt, backoff))
                continue

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ApiError(
                f"{method} {url} ({label}) failed: {exc} -- "
                f"response body: {_body_snippet(response)}",
                status_code=response.status_code,
                errors=_extract_errors(response),
            ) from exc
        return response


def _body_snippet(response, limit=1000):
    """Return a short, single-line snippet of a response body for logging.

    Golden Records returns error detail (validation messages) in the body of a
    4xx, which raise_for_status() drops. Prefer a compact JSON rendering, fall
    back to raw text, collapse whitespace to keep it on one line, and truncate
    so log/error lines stay readable.
    """
    try:
        text = json.dumps(response.json(), ensure_ascii=False,
                          separators=(",", ":"))
    except ValueError:
        text = response.text or ""
    text = " ".join(text.split())
    if not text:
        return "(empty body)"
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def _retry_delay(response, attempt, backoff):
    """Seconds to wait before a retry: the Retry-After header if present and
    numeric, otherwise exponential backoff."""
    retry_after = response.headers.get("Retry-After", "")
    if retry_after.strip().isdigit():
        return int(retry_after)
    return backoff * (2 ** attempt)


def _is_last_page(response, page, batch, page_size):
    """Decide whether the current page is the final page of a paged resource.

    The Golden Records API returns a `paging-headers` header, e.g.
    {"totalCount":373,"currentPage":1,"totalPages":1,"nextPage":"No", ...};
    when present, currentPage >= totalPages is authoritative. If the header is
    missing or unparseable, fall back to "a page shorter than page_size is the
    last one".
    """
    header = response.headers.get("paging-headers")
    if header:
        try:
            info = json.loads(header)
            current = info.get("currentPage", page)
            total = info.get("totalPages")
            if total is not None:
                return current >= total
        except (ValueError, TypeError):
            pass  # fall back to the length heuristic below
    return len(batch) < page_size


def ensure_reference_file(path, endpoint_key, config, refresh, limiter,
                          verbose=False):
    """Ensure `path` exists, downloading it from the API when missing/refreshing.

    Returns True if the file was (re)downloaded, False if the existing file
    was used as-is.
    """
    if os.path.exists(path) and not refresh:
        return False
    records = fetch_resource(config, endpoint_key, limiter, verbose=verbose)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)
        file.write("\n")
    return True


def parse_ea_date(value):
    """Parse an ExpertArcher datetime string, or return None if unparseable.

    ExpertArcher supplies a full datetime; both output formats need only the
    date part, formatted differently (the API's `date_shot` wants ISO
    YYYY-MM-DD -- other forms are rejected as "No valid entry in Date Shot
    field." -- while the CSV import wants DD/MM/YYYY). Returning the parsed
    datetime lets each renderer format it, and None lets the caller skip a
    score whose date cannot be parsed.
    """
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def get_age_group(gender, age_class):
    """Derive the Golden Records age-group name from gender and age class.

    e.g. ("female", "u14") -> "Women U14"; ("male", "senior") -> "Men".
    Returns "" for genders we do not recognise.
    """
    prefix = {"male": "Men", "female": "Women"}.get(gender)
    if prefix is None:
        return ""
    suffix = "" if age_class == "senior" else age_class.upper()
    return f"{prefix} {suffix}".strip()


def resolve(ea_name, name_map, name_to_entry, kind):
    """Resolve an ExpertArcher name to its Golden Records details.

    The name is first translated via the mappings; names not listed there are
    used as-is (many match Golden Records exactly, so only the differences are
    kept in the mappings file). The resulting name is then looked up in the
    official reference table, case-insensitively (the tables are keyed by
    lower-cased name), so names differing only by capitalisation need no
    mapping.

    Returns (entry, None) on success -- where entry is the details dict from
    load_rounds / load_bowtypes ({"name", "id", ...}) -- or (None, reason)
    where reason is a (category, detail) tuple for the report:
      - "unmatched <kind>": the name is unknown to both the mappings and
        Golden Records, so a mapping needs adding.
      - "unknown Golden Records <kind>": a mapping exists but points at a name
        Golden Records does not recognise, so the mapping is wrong.
    """
    gr_name = name_map.get(ea_name, ea_name)
    found = name_to_entry.get(gr_name.lower()) if isinstance(gr_name, str) else None
    if found is not None:
        return found, None
    if ea_name in name_map:  # explicitly mapped, but to a non-existent name
        return None, (f"unknown Golden Records {kind}", f"{ea_name!r} -> {gr_name!r}")
    return None, (f"unmatched {kind}", ea_name)


def transform_score(score, lookups):
    """Map one ExpertArcher score onto a resolved `Mapped` record.

    Rounds and bow types are resolved via `resolve`: the ExpertArcher name is
    translated to a Golden Records name via the mappings (or used as-is if not
    listed), then that name is resolved to its id (and, for rounds, its type)
    via the official reference files. The result is format-independent --
    api_record() renders it for POST /scores and csv_row() for the CSV import.

    Returns (mapped, None) on success, or (None, reason) if the score cannot
    be mapped. `reason` is a (category, detail) tuple used by the report.
    """
    ea_round = score.get("round")
    round_entry, reason = resolve(ea_round, lookups.name_map, lookups.round_ids, "round")
    if reason is not None:
        return None, reason

    ea_bowtype = score.get("bowtype")
    class_entry, reason = resolve(ea_bowtype, lookups.name_map, lookups.class_ids, "bow type")
    if reason is not None:
        return None, reason

    ea_name = score.get("name")
    if ea_name not in lookups.members:
        return None, ("unmatched member name", ea_name)

    age_group_name = get_age_group(score.get("gender"), score.get("class"))
    if age_group_name not in lookups.age_groups:
        detail = f"{score.get('gender')}/{score.get('class')} -> {age_group_name!r}"
        return None, ("unmatched age group", detail)

    try:
        numbers = {field: int(score[field]) for field in NUMERIC_FIELDS}
        # Xs may arrive under either casing; missing means zero.
        xs = max(int(score.get("xs", 0) or 0), int(score.get("Xs", 0) or 0))
    except (KeyError, TypeError, ValueError) as exc:
        return None, ("invalid number", str(exc))

    raw_date = score.get("date")
    date = parse_ea_date(raw_date)
    if date is None:
        return None, ("invalid date", repr(raw_date))

    ea_tournament = bool(score.get("tournament", False))
    ea_competition = bool(score.get("competition", False))
    if ea_tournament:
        status = STATUS_OPEN_COMPETITION
    elif ea_competition:
        status = STATUS_CLUB_COMPETITION
    else:
        status = STATUS_CLUB_EVENT

    place = score.get("place")
    location = place.strip() if isinstance(place, str) else ""

    mapped = Mapped(
        member_id=lookups.members[ea_name],
        member_name=ea_name,
        round_id=round_entry["id"],
        round_name=round_entry["name"],
        round_type=round_entry.get("type", ""),
        class_id=class_entry["id"],
        class_name=class_entry["name"],
        age_group_id=lookups.age_groups[age_group_name],
        age_group_name=age_group_name,
        date=date,
        score=numbers["score"],
        hits=numbers["hits"],
        golds=numbers["golds"],
        xs=xs,
        location=location,
        status=status,
        record_status=ea_tournament,               # True only for open tournaments
        qualifying="252" not in ea_round,           # 252 award rounds are not qualifying
        record_qualifying=True,                     # always eligible for club records
        # CSV-only columns (the API has no such fields). Handicap copies the
        # ExpertArcher value when present. Classification is translated to the
        # Golden Records name via the mappings; an unlisted value is left blank.
        handicap=_opt_str(score.get("handicap")),
        classification=lookups.classification_map.get(
            score.get("classification") or "", ""),
    )
    return mapped, None


def _opt_str(value):
    """Render an optional value as a string, or "" when it is absent/None."""
    return "" if value is None else str(value)


def api_record(mapped):
    """Render a Mapped score as the JSON body for POST /scores.

    These keys are exactly the fields the Golden Records Scores API expects:
    the *_id fields are the resolved reference ids, `status` is the
    ScoreStatusOptions enum int, and `date_shot` is ISO YYYY-MM-DD.
    (Golden Records devs: this dict is the integration contract.)
    """
    return {
        "age_group_id": mapped.age_group_id,             # GR id: Men/Women + age band
        "class_id": mapped.class_id,                     # GR id: bow type
        "date_shot": mapped.date.strftime("%Y-%m-%d"),   # ISO YYYY-MM-DD (date only)
        "member_id": mapped.member_id,                   # GR id: the archer
        "golds": mapped.golds,
        "hits": mapped.hits,
        "location": mapped.location,                     # free text, from EA "place"
        "qualifying": mapped.qualifying,
        "record_qualifying": mapped.record_qualifying,
        "record_status": mapped.record_status,
        "round_id": mapped.round_id,                     # GR id: round
        "score": mapped.score,
        "status": mapped.status,                         # enum: 2=Club Event, 3=Club Competition, 4=Open Competition
        "user_1": USER_1_TAG,                            # constant tag marking the ExpertArcher integration as source
        "Xs": mapped.xs,
    }


def csv_row(mapped):
    """Render a Mapped score as one Golden Records CSV import row (see CSV_HEADER).

    Uses the reference *names* (not ids), a DD/MM/YYYY date, uppercase
    TRUE/FALSE booleans and the `status` text label. Columns with no
    ExpertArcher source (Affiliation/Verified) are left blank; Handicap and
    Classification carry through only when ExpertArcher supplied them.
    """
    return [
        mapped.date.strftime("%d/%m/%Y"),
        mapped.member_name,
        mapped.score,
        mapped.hits,
        mapped.golds,
        mapped.xs,
        mapped.handicap,
        mapped.classification,
        mapped.location,
        mapped.class_name,                  # CSV "Class" is the bow type
        mapped.age_group_name,
        mapped.round_name,
        mapped.round_type,                  # Indoor/Outdoor, from rounds.json
        _csv_bool(mapped.record_status),
        _csv_bool(mapped.qualifying),
        STATUS_LABELS[mapped.status],
        "",                                 # Affiliation
        "",                                 # Affiliation Expiry
        "",                                 # Affiliation ID
        "",                                 # Verified
    ]


def _csv_bool(value):
    """Render a boolean as the uppercase TRUE/FALSE the CSV import expects."""
    return "TRUE" if value else "FALSE"


def write_csv(path, mapped_records):
    """Write mapped scores to a Golden Records CSV import file.

    Writes the header row (CSV_HEADER), one row per mapped score, then the
    trailing `END` sentinel row the importer expects. Overwrites any existing
    file at `path`.
    """
    with open(path, "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_HEADER)
        for mapped in mapped_records:
            writer.writerow(csv_row(mapped))
        writer.writerow(["END"] + [""] * (len(CSV_HEADER) - 1))


def process(scores, lookups):
    """Transform every score, returning (mapped, skips).

    `mapped` is a list of Mapped records (see transform_score); `skips` is a
    list of (category, detail, archer) tuples, one per skipped score, ready
    for the report.
    """
    mapped = []
    skips = []
    for score in scores:
        record, reason = transform_score(score, lookups)
        if reason is None:
            mapped.append(record)
        else:
            category, detail = reason
            skips.append((category, detail, score.get("name", "?")))
    return mapped, skips


def filter_by_name(scores, include_name, exclude_name):
    """Filter fetched scores by ExpertArcher name (exact, case-sensitive match).

    - include_name: keep only scores whose `name` equals it.
    - exclude_name: drop scores whose `name` equals it.
    At most one is set (the CLI makes them mutually exclusive). With neither,
    the scores are returned unchanged.
    """
    if include_name is not None:
        return [s for s in scores if s.get("name") == include_name]
    if exclude_name is not None:
        return [s for s in scores if s.get("name") != exclude_name]
    return scores


def print_report(total_read, mapped, skips):
    """Print a human-readable summary of what was parsed and what was skipped."""
    print("=" * 60)
    print("ExpertArcher -> Golden Records: processing report")
    print("=" * 60)
    print(f"Records read:    {total_read}")
    print(f"Records parsed:  {len(mapped)}")
    print(f"Records skipped: {len(skips)}")

    if not skips:
        print("\nAll records mapped successfully.")
        return

    # Group skips by reason category, then collapse to unique entries so the
    # report shows each problem once rather than one line per affected record.
    #   - unmatched member name: one entry per unique name.
    #   - everything else:        one entry per detail + archer combination.
    by_category = defaultdict(list)
    for category, detail, archer in skips:
        by_category[category].append((detail, archer))

    # Collapse each category to its unique entries up front, so the header can
    # report the unique count and sections order by it.
    collapsed = {category: _collapse(category, entries)
                 for category, entries in by_category.items()}

    print("\nSKIPPED RECORDS BY REASON")
    print("=" * 60)
    for category in sorted(collapsed, key=lambda c: (-len(collapsed[c][0]), c)):
        counts, labels = collapsed[category]
        header = f"{category.capitalize()}  ({len(counts)} unique)"
        print(f"\n{header}")
        print("=" * len(header))

        for key, count in counts.most_common():
            print(f"    - {labels[key]}  ({count} record(s))")


def _collapse(category, entries):
    """Reduce a category's skips to unique entries, returning (counts, labels).

    Unmatched member names collapse per unique name; every other category
    collapses per unique detail + archer combination.
    """
    if category == "unmatched member name":
        keys = [detail for detail, _ in entries]
        labels = {detail: _fmt(detail) for detail, _ in entries}
    else:
        keys = [(detail, archer) for detail, archer in entries]
        labels = {(detail, archer): f"{_fmt(detail)} | {archer}"
                  for detail, archer in entries}
    return Counter(keys), labels


def report_submission(result, total, error_log_path):
    """Summarise a submission run, and write full failure detail to a log.

    Prints the accepted / duplicate / error counts and, for genuine errors, a
    breakdown by error type (the API's own messages). Duplicates are shown as a
    single benign count -- re-running a submission is expected to report them.
    The full per-record detail (the response bodies) is appended to
    `error_log_path` for separate review rather than flooding the report.
    """
    print(f"Submitted {result.submitted} of {total} record(s).")
    if result.duplicates:
        print(f"Duplicates skipped (already in Golden Records): "
              f"{len(result.duplicates)}")
    if result.errors:
        print(f"Rejected with errors: {len(result.errors)}")

        # Count by error type -- the API's own messages. A record may carry
        # more than one message; count each so the totals explain every error.
        counts = Counter()
        for failure in result.errors:
            for message in (failure.errors or ["(no error detail -- see log)"]):
                counts[message] += 1

        print("\nSUBMISSION ERRORS BY TYPE")
        print("=" * 60)
        for message, count in counts.most_common():
            print(f"    - {message}  ({count})")

    failures = result.duplicates + result.errors
    if failures:
        write_error_log(error_log_path, failures)
        print(f"\nFull detail of the {len(failures)} rejected record(s) "
              f"written to {error_log_path}")


def write_error_log(path, failures):
    """Append full detail of rejected submissions to a log file for review.

    Each run adds a timestamped block: one entry per rejected record with the
    full error message (status + response body) and the record we sent, so a
    reviewer can see exactly what was rejected and why. Appends (does not
    overwrite) so a history builds up across runs.
    """
    stamp = datetime.now().isoformat(timespec="seconds")
    with open(path, "a", encoding="utf-8") as file:
        file.write(f"# {stamp} -- {len(failures)} rejected submission(s)\n")
        for failure in failures:
            file.write(f"record {failure.index}: {failure.message}\n")
            file.write("    submitted: "
                       f"{json.dumps(failure.record, ensure_ascii=False)}\n")
        file.write("\n")


def _fmt(detail):
    """Render an offending value for the report, flagging missing/empty ones."""
    return repr(detail) if detail not in (None, "") else "(missing / empty)"


def build_arg_parser():
    """Build the command-line parser. See each argument's help for details."""
    parser = argparse.ArgumentParser(
        description="Prepare ExpertArcher scores for the Golden Records Scores API."
    )
    parser.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD",
                        help="Only fetch scores shot on or after this date")
    parser.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD",
                        help="Only fetch scores shot on or before this date")
    name_filter = parser.add_mutually_exclusive_group()
    name_filter.add_argument("--include-name", metavar="NAME",
                             help="Only process scores whose ExpertArcher name "
                                  "exactly matches NAME")
    name_filter.add_argument("--exclude-name", metavar="NAME",
                             help="Process all scores except those whose "
                                  "ExpertArcher name exactly matches NAME")
    parser.add_argument("--members", default=DEFAULT_MEMBERS,
                        help=f"Members JSON (default: {DEFAULT_MEMBERS})")
    parser.add_argument("--age-groups", default=DEFAULT_AGE_GROUPS,
                        help=f"Age groups JSON (default: {DEFAULT_AGE_GROUPS})")
    parser.add_argument("--rounds", default=DEFAULT_ROUNDS,
                        help=f"Golden Records rounds JSON (default: {DEFAULT_ROUNDS})")
    parser.add_argument("--bowtypes", default=DEFAULT_BOWTYPES,
                        help=f"Golden Records bow types JSON (default: {DEFAULT_BOWTYPES})")
    parser.add_argument("--mappings", default=DEFAULT_MAPPINGS,
                        help=f"ExpertArcher -> Golden Records name mappings TOML "
                             f"(default: {DEFAULT_MAPPINGS})")
    parser.add_argument("--config", default=DEFAULT_CONFIG,
                        help=f"API config TOML for ExpertArcher (scores) and "
                             f"Golden Records (reference files) "
                             f"(default: {DEFAULT_CONFIG})")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-download the reference files from the API even "
                             "if they already exist locally")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch, map and report, but do NOT submit anything "
                             "to Golden Records (no POST is made)")
    parser.add_argument("--csv", metavar="PATH",
                        help="Write the mapped records to a Golden Records CSV "
                             "bulk-import file at PATH instead of submitting them "
                             "(no POST is made). Best for large historical "
                             "imports, which the per-record API path throttles.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Log every HTTP request's method, URL, status and "
                             "response body (useful for debugging API errors)")
    parser.add_argument("--error-log", default=DEFAULT_ERROR_LOG,
                        help=f"File to append full detail of any rejected "
                             f"submissions to (default: {DEFAULT_ERROR_LOG})")
    return parser


def load_env():
    """Load credentials from a local .env file into the environment, if present.

    Uses python-dotenv when available; a missing package or missing file is
    fine (the credentials can still be set directly in the environment).
    Existing environment variables are not overridden.
    """
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return
    load_dotenv()


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    load_env()

    # Download any missing (or, with --refresh, all) reference files first.
    # One shared limiter spaces every request across all the downloads.
    config = load_config(args.config)
    min_interval = config.get("api", {}).get("throttle", {}).get("min_interval_seconds", 1.0)
    limiter = RateLimiter(min_interval)
    try:
        for path, endpoint_key in [(args.rounds, "rounds"),
                                   (args.bowtypes, "bowtypes"),
                                   (args.age_groups, "age_groups"),
                                   (args.members, "members")]:
            if ensure_reference_file(path, endpoint_key, config, args.refresh,
                                     limiter, verbose=args.verbose):
                print(f"Downloaded {path} from the Golden Records API")
    except RuntimeError as exc:
        raise SystemExit(f"Error: {exc}")

    name_map, classification_map = load_mappings(args.mappings)
    lookups = Lookups(
        members=load_members(args.members),
        age_groups=load_age_groups(args.age_groups),
        name_map=name_map,
        round_ids=load_rounds(args.rounds),
        class_ids=load_bowtypes(args.bowtypes),
        classification_map=classification_map,
    )

    try:
        scores = fetch_scores(config, limiter, args.date_from, args.date_to,
                              verbose=args.verbose)
    except RuntimeError as exc:
        raise SystemExit(f"Error: {exc}")
    print(f"Fetched {len(scores)} scores from the ExpertArcher API")

    # Optionally filter by archer name (exact match) before mapping, so the
    # filter applies to whichever output follows (submission or --csv).
    if args.include_name is not None or args.exclude_name is not None:
        before = len(scores)
        scores = filter_by_name(scores, args.include_name, args.exclude_name)
        if args.include_name is not None:
            print(f"Filtered to {len(scores)} score(s) for name "
                  f"'{args.include_name}' (from {before}).")
        else:
            print(f"Excluded name '{args.exclude_name}': {len(scores)} of "
                  f"{before} score(s) remain.")

    mapped, skips = process(scores, lookups)
    print_report(len(scores), mapped, skips)

    # --csv writes a Golden Records bulk-import file and submits nothing. This
    # is the route for large historical imports, where the per-record API path
    # would be throttled (Golden Records allows only 200 requests/hour).
    if args.csv:
        if not mapped:
            print("\nNothing mapped -- no CSV written.")
        else:
            write_csv(args.csv, mapped)
            print(f"\nWrote {len(mapped)} record(s) to {args.csv} for Golden "
                  f"Records CSV import (nothing was submitted).")
        return

    # Otherwise submit the mapped records to the live Golden Records API, unless
    # this is a --dry-run (fetch/map/report only, no POST).
    if not mapped:
        pass  # nothing mapped, nothing to submit
    elif args.dry_run:
        print(f"\nDry run: {len(mapped)} record(s) would be submitted to "
              f"Golden Records (nothing was sent).")
    else:
        print(f"\nSubmitting {len(mapped)} record(s) to Golden Records...")
        records = [api_record(m) for m in mapped]
        try:
            result = submit_records(config, records, limiter,
                                    verbose=args.verbose)
        except RuntimeError as exc:
            raise SystemExit(f"Error: {exc}")
        report_submission(result, len(records), args.error_log)


if __name__ == "__main__":
    main()
