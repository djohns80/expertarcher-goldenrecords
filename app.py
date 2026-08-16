"""Prepare ExpertArcher scores for submission to the Golden Records Scores API.

Reads a list of scores exported from ExpertArcher and maps each one onto the
fields the Golden Records API expects, using reference data for rounds, bow
classes, age groups and members. Records that cannot be mapped are skipped and
explained in a summary report so they can be fixed at source.

This script reports only; it does not submit anything or write an output file.
"""

import argparse
import csv
import json
import tomllib
from collections import Counter, defaultdict, namedtuple
from datetime import datetime

# Default input locations, relative to the working directory. All are
# overridable on the command line (see build_arg_parser).
DEFAULT_SCORES = "scores.json"
DEFAULT_MEMBERS = "Members.csv"
DEFAULT_AGE_GROUPS = "age-groups.json"
DEFAULT_ROUNDS = "rounds.json"
DEFAULT_BOWTYPES = "bowtypes.json"
DEFAULT_MAPPINGS = "mappings.toml"

# Numeric score fields that must be present and integer-parseable.
NUMERIC_FIELDS = ("score", "hits", "golds")

# All the reference lookups needed to map one score, resolved once at startup.
#   name_map:  ExpertArcher name -> Golden Records name (from mappings.json).
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
    """Return a mapping of member name -> member_id from the members CSV."""
    with open(path, "r", encoding="utf-8", newline="") as file:
        return {row["name"]: row["member_id"] for row in csv.DictReader(file)}


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

    record = {
        "age_group_id": lookups.age_groups[age_group_name],
        "class_id": class_id,
        "date_shot": date_shot,
        "member_id": lookups.members[ea_name],
        "golds": numbers["golds"],
        "hits": numbers["hits"],
        "location": location,
        "qualifying": "252" not in ea_round,
        "record_qualifying": True,
        "record_status": ea_tournament,
        "round_id": round_id,
        "score": numbers["score"],
        "status": status,
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
    parser = argparse.ArgumentParser(
        description="Prepare ExpertArcher scores for the Golden Records Scores API."
    )
    parser.add_argument("--scores", default=DEFAULT_SCORES,
                        help=f"ExpertArcher scores JSON (default: {DEFAULT_SCORES})")
    parser.add_argument("--members", default=DEFAULT_MEMBERS,
                        help=f"Members CSV export (default: {DEFAULT_MEMBERS})")
    parser.add_argument("--age-groups", default=DEFAULT_AGE_GROUPS,
                        help=f"Age groups JSON (default: {DEFAULT_AGE_GROUPS})")
    parser.add_argument("--rounds", default=DEFAULT_ROUNDS,
                        help=f"Golden Records rounds JSON (default: {DEFAULT_ROUNDS})")
    parser.add_argument("--bowtypes", default=DEFAULT_BOWTYPES,
                        help=f"Golden Records bow types JSON (default: {DEFAULT_BOWTYPES})")
    parser.add_argument("--mappings", default=DEFAULT_MAPPINGS,
                        help=f"ExpertArcher -> Golden Records name mappings TOML "
                             f"(default: {DEFAULT_MAPPINGS})")
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)

    lookups = Lookups(
        members=load_members(args.members),
        age_groups=load_age_groups(args.age_groups),
        name_map=load_mappings(args.mappings),
        round_ids=load_rounds(args.rounds),
        class_ids=load_bowtypes(args.bowtypes),
    )
    scores = load_json(args.scores)

    records, skips = process(scores, lookups)
    print_report(len(scores), records, skips)


if __name__ == "__main__":
    main()
