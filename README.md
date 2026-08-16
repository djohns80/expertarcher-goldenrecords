# ExpertArcher → Golden Records

A small Python tool that reads a club's scores from the **ExpertArcher** API and
maps each one onto the record shape the **Golden Records** Scores API expects.
Scores that can't be mapped are skipped and explained in a readable report, so
the underlying data can be fixed at source.

It is intentionally a single, readable script ([app.py](app.py)) rather than a
framework — the goal is to make the integration easy to follow.

## Who this is for

- **ExpertArcher developers** — to see exactly what data Golden Records needs and
  how ExpertArcher scores translate into it. If ExpertArcher ever wants to push
  scores to Golden Records natively, [app.py](app.py) is a working reference for
  the field mapping, name resolution and the API calls involved. See
  [A more native integration](#a-more-native-integration).
- **Golden Records developers** — to see how a third party integrates with your
  APIs: how we authenticate, page, respect throttling, and what the outgoing
  score record looks like. The record built in `transform_score` is the
  integration contract.

## How it works

```
ExpertArcher API                     Golden Records API
   (scores)                          (reference data: ids)
      │                                     │
      │  GET /club?apikey=…&from=…&to=…     │  GET /rounds, /classes,
      │                                     │      /age-groups, /members
      ▼                                     ▼
   [ scores ]                        golden-records/*.json  (cached locally)
      │                                     │
      └──────────────┬──────────────────────┘
                     ▼
            transform_score()   ── map names → ids, derive fields
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
  Golden Records            skipped records
  score records             (grouped report)
```

The pipeline lives in `main()`:

1. **Fetch reference data from Golden Records** (`fetch_resource`) into the
   [golden-records/](golden-records/) directory, unless the files are already
   present. These provide the ids we map names onto. Re-download with `--refresh`.
2. **Fetch the scores from ExpertArcher** (`fetch_scores`) for the `--from` /
   `--to` window.
3. **Map each score** to a Golden Records record (`transform_score`), resolving
   round / bow-type / member / age-group names to ids (`resolve`).
4. **Report** what mapped and what was skipped, and why (`print_report`).
5. **Submit** the mapped records to Golden Records (`submit_records`), unless
   `--dry-run` was given.

**Runs submit by default.** After reporting, the mapped records are POSTed to the
Golden Records Scores API, so run the tool deliberately. Pass `--dry-run` to
fetch, map and report without sending anything. See
[Submitting scores](#submitting-scores).

## The two APIs

| | ExpertArcher (source) | Golden Records (reference) |
|---|---|---|
| Used for | The scores to process | Ids for rounds, bow classes, age groups, members |
| Endpoint | `GET {base_url}/club` | `GET {base_url}/rounds`, `/classes`, `/age-groups`, `/members` |
| Auth | API key as a **query-string** param (`apikey=…`) | API key as `Authorization: Basic <key>` header, **or** HTTP Basic Auth |
| Paging | Single request (date range bounds the result) | Paged; last page detected via the `paging-headers` response header |
| Config | `[expertarcher]` in [config.toml](config.toml) | `[api]` in [config.toml](config.toml) |

### Authentication notes for Golden Records

Golden Records supports two schemes (`_build_auth`), API key first:

- **API key (recommended)** — sent as `Authorization: Basic <key>`, i.e. the key
  replaces the usual base64 `username:password` value. A **club-level key returns
  the whole membership**.
- **HTTP Basic Auth (fallback)** — `username` / `password`. Note: under Basic
  Auth the `/members` endpoint returns **only the authenticating user's own
  record**, not the club roster. This is why the API key is strongly preferred.

Both APIs are rate-limited by a single shared `RateLimiter` (Golden Records
throttles to 1/second, 20/minute, 200/hour) and retried with exponential
backoff, honouring any `Retry-After` header (`_get_with_retry`).

## Field mapping

Each ExpertArcher score becomes one Golden Records record in `transform_score`.
The keys below are exactly what the Golden Records Scores API expects.

| Golden Records field | Source | Notes |
|---|---|---|
| `member_id` | member name → id | Name resolved against `members.json` |
| `round_id` | `round` name → id | Via mappings + `rounds.json` (see [Name resolution](#name-resolution)) |
| `class_id` | `bowtype` name → id | Via mappings + `bowtypes.json` |
| `age_group_id` | `gender` + `class` → id | e.g. `female` + `u14` → "Women U14" → id (`get_age_group`) |
| `date_shot` | `date` | ExpertArcher datetime reduced to ISO `YYYY-MM-DD` |
| `score`, `hits`, `golds` | same | Parsed as integers |
| `Xs` | `xs` / `Xs` | Either casing accepted; missing = 0 |
| `location` | `place` | Free text |
| `status` | `tournament` / `competition` flags | `ScoreStatusOptions` enum int: `4` Open Competition / `3` Club Competition / `2` Club Event |
| `record_status` | `tournament` | `True` only for open tournaments |
| `qualifying` | `round` name | `False` for "252" award rounds |
| `record_qualifying` | — | Always `True` (eligible for club records) |

### Name resolution

ExpertArcher and Golden Records mostly use the same names for rounds and bow
types, but not always. `resolve` handles this in two steps:

1. **Translate** the ExpertArcher name via [mappings.toml](mappings.toml) — a flat
   `"ExpertArcher name" = "Golden Records name"` table. Only names that differ by
   more than capitalisation are listed; anything not in the file is used as-is.
2. **Look up** the resulting name in the Golden Records id table
   (`rounds.json` / `bowtypes.json`), **case-insensitively**.

So `"afb"` needs a mapping (→ "American Flatbow"), but `"recurve"` does not
(matches "Recurve" by case-insensitive lookup). This keeps the hand-maintained
mappings file as small as possible. Unmatched names are reported, not guessed.

## Setup

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                 # install dependencies (requests, python-dotenv)
cp .env.example .env     # then edit .env with your API keys
```

Edit [config.toml](config.toml) to set your ExpertArcher `base_url` and `club`
(the Golden Records `[api]` section is pre-filled). **`config.toml` holds no
secrets** — all API keys live in `.env`, which is gitignored.

`.env` keys (see [.env.example](.env.example)):

| Variable | For | Notes |
|---|---|---|
| `EXPERTARCHER_API_KEY` | ExpertArcher | Required |
| `GOLDEN_RECORDS_API_KEY` | Golden Records | Recommended |
| `GOLDEN_RECORDS_USERNAME` / `GOLDEN_RECORDS_PASSWORD` | Golden Records | Basic Auth fallback |

## Usage

```bash
# Process AND submit scores shot in July 2026 (see the warning below)
uv run python app.py --from 2026-07-01 --to 2026-08-01

# Same, but fetch/map/report only -- nothing is submitted
uv run python app.py --dry-run --from 2026-07-01 --to 2026-08-01

# Re-download the Golden Records reference files first
uv run python app.py --refresh --from 2026-07-01 --to 2026-08-01

uv run python app.py --help    # all options
```

> Runs submit to Golden Records by default; use `--dry-run` to skip it — see
> [Submitting scores](#submitting-scores).

### The report

```
============================================================
ExpertArcher -> Golden Records: processing report
============================================================
Records read:    409
Records parsed:  358
Records skipped: 51

SKIPPED RECORDS BY REASON
============================================================

Unmatched round  (8 unique)
===========================
    - '(252 40yrds barebow)' | Hugh Simpson  (6 record(s))
    ...
```

Skips are grouped by reason and collapsed to unique entries (member-name issues
by name; everything else by detail + archer), so each problem shows once with a
record count. Skip categories:

- **Unmatched round / bow type** — name unknown to both the mappings and Golden
  Records → add a mapping in [mappings.toml](mappings.toml) or fix it in
  ExpertArcher.
- **Unknown Golden Records round / bow type** — a mapping exists but points at a
  name Golden Records doesn't recognise → the mapping is wrong.
- **Unmatched member name** — the archer name isn't in the Golden Records roster
  (typo, missing surname, etc.).
- **Unmatched age group** — the gender/class combination didn't resolve.
- **Invalid number / date** — a required field was missing or unparseable.

## Submitting scores

> ⚠️ **Runs write live data to Golden Records by default and this cannot be
> undone from the tool.** Run it only when you mean to submit. To fetch, map and
> report without sending anything, pass `--dry-run`.

After printing the report, the tool POSTs each mapped record to the Golden
Records Scores API (`POST {base_url}/scores`, one record per request):

```bash
uv run python app.py --from 2026-07-01 --to 2026-08-01

# Dry run: everything except the POST
uv run python app.py --dry-run --from 2026-07-01 --to 2026-08-01
```

With `--dry-run` the tool reports how many records *would* be submitted and
sends nothing. Otherwise each mapped record is POSTed individually, spaced by the
same rate limiter and retried on 429/5xx like the reference fetches. Submission
continues past any records the API rejects, so one bad record does not block the
rest. Skipped records (from the report) are never submitted.

### Submission results

After submitting, the tool prints a summary rather than one line per record:

```
Submitted 12 of 15 record(s).
Duplicates skipped (already in Golden Records): 2

SUBMISSION ERRORS BY TYPE
============================================================
    - No valid entry in Date Shot field.  (1)

Full detail of the 3 rejected record(s) written to submission-errors.log
```

- **Duplicates** — the API rejects a score it already holds with *"This score
  already exists in the database."* This is benign (expected when you re-run a
  submission), so it is counted on its own line, separate from real errors.
- **Errors by type** — every other rejection is counted by the API's own error
  message, so systematic problems (e.g. a bad field) stand out at a glance.
- **`submission-errors.log`** — the full detail of every rejected record (the
  response body and the record that was sent) is appended here for separate
  review, instead of flooding the report. The file is gitignored; change its
  path with `--error-log`. `--verbose` still logs every request live.

## A more native integration

This tool works from the outside in — pulling scores and reference data over
HTTP and mapping between them. If ExpertArcher wanted to integrate with Golden
Records directly, [app.py](app.py) still documents what that requires:

- The **target record shape** and field semantics (`transform_score`) — the data
  Golden Records needs for each score.
- The **name → id resolution** that any integration must perform, and the fact
  that only a handful of round/bow-type names differ between the systems
  (`resolve`, [mappings.toml](mappings.toml)).
- The **derived fields** that aren't a straight copy (`status`, `qualifying`,
  `record_status`, age-group derivation).

A native integration could skip the ExpertArcher fetch and mapping entirely by
emitting Golden Records records at the point scores are recorded — but the
mapping table above is the same either way.

## Project layout

```
app.py                 The whole tool (fetch → map → report → submit)
config.toml            API config for both services (no secrets; committed)
.env.example           Template for the API keys → copy to .env (gitignored)
mappings.toml          ExpertArcher → Golden Records name overrides
golden-records/        Cached reference data downloaded from Golden Records
  rounds.json, bowtypes.json, age-groups.json   (committed)
  members.json                                  (gitignored — personal data)
submission-errors.log  Full detail of rejected submissions (gitignored; per run)
pyproject.toml         Dependencies / uv project
```

## Notes on data & privacy

- `golden-records/members.json` contains personal data and is **gitignored**; it
  is downloaded fresh from the API and never committed.
- Scores are fetched live from ExpertArcher and not stored locally.
- API keys are read from the environment only, never committed.
