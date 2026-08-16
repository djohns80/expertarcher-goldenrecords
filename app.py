"""Prepare ExpertArcher scores for submission to the Golden Records Scores API.

Fetches the list of scores from the ExpertArcher API and maps each one onto
the fields the Golden Records API expects, using reference data for rounds,
bow classes, age groups and members. Records that cannot be mapped are skipped
and explained in a summary report so they can be fixed at source.

Pipeline (see main() at the bottom):
  1. Fetch reference data from Golden Records (fetch_resource) into the
     golden-records/ files, unless already present. These give us the ids.
  2. Fetch the scores to process from ExpertArcher (fetch_scores).
  3. Map each score to a Golden Records record (transform_score), resolving
     round / bow-type / member / age-group names to ids (resolve).
  4. Report what mapped and what was skipped, and why (print_report).

Scores come from the ExpertArcher API (the [expertarcher] section of
config.toml); the reporting window is set with --from / --to. The Golden
Records reference files (rounds.json, bowtypes.json, age-groups.json,
members.json) are downloaded from the API when missing (or with --refresh),
using the [api] section. API keys are read from the environment, never a file
(see the ENV_* constants). See build_arg_parser for all options.

This script reports only; it does not submit anything or write an output file.
README.md has the full integration overview, field mapping and setup.
"""

import argparse
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

# Numeric score fields that must be present and integer-parseable.
NUMERIC_FIELDS = ("score", "hits", "golds")

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
#   *_ids:     Golden Records name -> Golden Records id (from the official files)
Lookups = namedtuple(
    "Lookups",
    "members age_groups name_map round_ids class_ids",
)


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
    """Return a mapping of Golden Records round name -> round_id.

    Keyed by lower-cased name so lookups are case-insensitive (see `resolve`).
    """
    return {row["round"].lower(): row["round_id"] for row in load_json(path)}


def load_bowtypes(path):
    """Return a mapping of Golden Records bow class name -> class_id.

    Keyed by lower-cased name so lookups are case-insensitive (see `resolve`).
    """
    return {row["bow_class"].lower(): row["class_id"] for row in load_json(path)}


def load_mappings(path):
    """Return the ExpertArcher name -> Golden Records name map (TOML).

    A single flat map covering both rounds and bow types; the code resolves
    each mapped name against the appropriate id table by context. TOML is used
    so the file can carry comments grouping the entries by type.
    """
    with open(path, "rb") as file:  # tomllib requires a binary stream
        return tomllib.load(file)


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
    limits are not a concern (the retry logic in `_get_with_retry` handles them
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


def fetch_resource(config, endpoint_key, limiter):
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
        response = _get_with_retry(
            requests, url, auth_kwargs,
            {page_param: page, size_param: page_size},
            limiter, endpoint_key, max_retries, backoff,
        )
        batch = response.json()
        records.extend(batch)
        if _is_last_page(response, page, batch, page_size):
            break
        page += 1
    return records


def fetch_scores(config, limiter, date_from=None, date_to=None):
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
    response = _get_with_retry(
        requests, base_url + endpoint, {}, params, limiter,
        "scores", th.get("max_retries", 5), th.get("backoff_seconds", 2.0),
    )
    return response.json()


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


def _get_with_retry(requests, url, auth_kwargs, params, limiter, endpoint_key, max_retries, backoff):
    """GET a page, waiting on the limiter and retrying 429/5xx with backoff.

    `auth_kwargs` carries the credentials (from `_build_auth`) and is splatted
    into the request. Retries honour a `Retry-After` header (seconds) when the
    server sends one, otherwise use exponential backoff. Non-retryable errors
    (e.g. 4xx other than 429) and exhausted retries are raised as a RuntimeError.
    """
    for attempt in range(max_retries + 1):
        limiter.wait()
        try:
            response = requests.get(url, params=params, timeout=30, **auth_kwargs)
        except requests.RequestException as exc:
            if attempt < max_retries:
                time.sleep(backoff * (2 ** attempt))
                continue
            raise RuntimeError(f"Failed to download '{endpoint_key}' from {url}: {exc}") from exc

        if response.status_code == 429 or response.status_code >= 500:
            if attempt < max_retries:
                time.sleep(_retry_delay(response, attempt, backoff))
                continue

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to download '{endpoint_key}' from {url}: {exc}") from exc
        return response


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


def ensure_reference_file(path, endpoint_key, config, refresh, limiter):
    """Ensure `path` exists, downloading it from the API when missing/refreshing.

    Returns True if the file was (re)downloaded, False if the existing file
    was used as-is.
    """
    if os.path.exists(path) and not refresh:
        return False
    records = fetch_resource(config, endpoint_key, limiter)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)
        file.write("\n")
    return True


def format_date(value):
    """Format an ExpertArcher ISO datetime as a date-only DD/MM/YYYY string.

    ExpertArcher supplies a full datetime, but Golden Records and the report
    only use the date. Returns the original value unchanged if it cannot be
    parsed, so the report can still show something useful.
    """
    try:
        return datetime.fromisoformat(value).strftime("%d/%m/%Y")
    except (TypeError, ValueError):
        return value


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


def resolve(ea_name, name_map, name_to_id, kind):
    """Resolve an ExpertArcher name to a Golden Records id.

    The name is first translated via the mappings; names not listed there are
    used as-is (many match Golden Records exactly, so only the differences are
    kept in the mappings file). The resulting name is then looked up in the
    official id table, case-insensitively (the id tables are keyed by
    lower-cased name), so names differing only by capitalisation need no
    mapping.

    Returns (id, None) on success, or (None, reason) where reason is a
    (category, detail) tuple for the report:
      - "unmatched <kind>": the name is unknown to both the mappings and
        Golden Records, so a mapping needs adding.
      - "unknown Golden Records <kind>": a mapping exists but points at a name
        Golden Records does not recognise, so the mapping is wrong.
    """
    gr_name = name_map.get(ea_name, ea_name)
    found = name_to_id.get(gr_name.lower()) if isinstance(gr_name, str) else None
    if found is not None:
        return found, None
    if ea_name in name_map:  # explicitly mapped, but to a non-existent name
        return None, (f"unknown Golden Records {kind}", f"{ea_name!r} -> {gr_name!r}")
    return None, (f"unmatched {kind}", ea_name)


def transform_score(score, lookups):
    """Map one ExpertArcher score onto a Golden Records API record.

    Rounds and bow types are resolved via `resolve`: the ExpertArcher name is
    translated to a Golden Records name via the mappings (or used as-is if not
    listed), then that name is resolved to an id via the official reference
    files.

    Returns (record, None) on success, or (None, reason) if the score cannot
    be mapped. `reason` is a (category, detail) tuple used by the report.
    """
    ea_round = score.get("round")
    round_id, reason = resolve(ea_round, lookups.name_map, lookups.round_ids, "round")
    if reason is not None:
        return None, reason

    ea_bowtype = score.get("bowtype")
    class_id, reason = resolve(ea_bowtype, lookups.name_map, lookups.class_ids, "bow type")
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
    date_shot = format_date(raw_date)
    if date_shot == raw_date:  # unchanged means it could not be parsed
        return None, ("invalid date", repr(raw_date))

    ea_tournament = bool(score.get("tournament", False))
    ea_competition = bool(score.get("competition", False))
    if ea_tournament:
        status = "Open Competition"
    elif ea_competition:
        status = "Club Competition"
    else:
        status = "Club Event"

    place = score.get("place")
    location = place.strip() if isinstance(place, str) else ""

    # The Golden Records score record. These keys are exactly the fields the
    # Golden Records Scores API expects: the *_id fields are resolved from the
    # reference files, the rest are copied or derived from the ExpertArcher
    # score. (Golden Records devs: this dict is the integration contract.)
    record = {
        "age_group_id": lookups.age_groups[age_group_name],  # GR id: Men/Women + age band
        "class_id": class_id,                                # GR id: bow type
        "date_shot": date_shot,                              # DD/MM/YYYY (date only)
        "member_id": lookups.members[ea_name],               # GR id: the archer
        "golds": numbers["golds"],
        "hits": numbers["hits"],
        "location": location,                                # free text, from EA "place"
        "qualifying": "252" not in ea_round,                 # 252 award rounds are not qualifying
        "record_qualifying": True,                           # always eligible for club records
        "record_status": ea_tournament,                      # True only for open tournaments
        "round_id": round_id,                                # GR id: round
        "score": numbers["score"],
        "status": status,                                    # Open Competition / Club Competition / Club Event
        "Xs": xs,
    }
    return record, None


def process(scores, lookups):
    """Transform every score, returning (records, skips).

    `skips` is a list of (category, detail, archer) tuples, one per skipped
    score, ready for the report.
    """
    records = []
    skips = []
    for score in scores:
        record, reason = transform_score(score, lookups)
        if reason is None:
            records.append(record)
        else:
            category, detail = reason
            skips.append((category, detail, score.get("name", "?")))
    return records, skips


def print_report(total_read, records, skips):
    """Print a human-readable summary of what was parsed and what was skipped."""
    print("=" * 60)
    print("ExpertArcher -> Golden Records: processing report")
    print("=" * 60)
    print(f"Records read:    {total_read}")
    print(f"Records parsed:  {len(records)}")
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
            if ensure_reference_file(path, endpoint_key, config, args.refresh, limiter):
                print(f"Downloaded {path} from the Golden Records API")
    except RuntimeError as exc:
        raise SystemExit(f"Error: {exc}")

    lookups = Lookups(
        members=load_members(args.members),
        age_groups=load_age_groups(args.age_groups),
        name_map=load_mappings(args.mappings),
        round_ids=load_rounds(args.rounds),
        class_ids=load_bowtypes(args.bowtypes),
    )

    try:
        scores = fetch_scores(config, limiter, args.date_from, args.date_to)
    except RuntimeError as exc:
        raise SystemExit(f"Error: {exc}")
    print(f"Fetched {len(scores)} scores from the ExpertArcher API")

    records, skips = process(scores, lookups)
    print_report(len(scores), records, skips)


if __name__ == "__main__":
    main()
