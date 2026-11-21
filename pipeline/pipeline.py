#!/usr/bin/env python3
"""Shopping Sessions with optional compiled parsing and compact dedup records."""
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
import json
import os
import re
import sys
import time
from event_store import EventStore, STORE_NAME
from click_store import load_clicks, CLICK_NAME
from session_store import prepare_sessions
from event_loader import load_events, LOADER_NAME
from identity import People, load_identity, IDENTITY_NAME
from aggregation import try_aggregate

os.environ.setdefault("POLARS_MAX_THREADS", os.environ.get("NCPU", "8"))
import polars as pl

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
EPOCH = date(1970, 1, 1).toordinal()
ISO = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})")
USER_SCHEMA = {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64,
               "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String}
CAMPAIGN_SCHEMA = {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
                   "sessions": pl.Int64, "purchasing_sessions": pl.Int64,
                   "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64}


def digits(value):
    return bool(value) and value.isascii() and value.isdigit()


def timestamp(value):
    if digits(value):
        value = value.lstrip("0") or "0"
        return int(value) if len(value) <= 18 else None
    match = ISO.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    try:
        parsed = datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None
    zone = match[7]
    offset = 0
    if zone != "Z":
        hours, minutes = int(zone[1:3]), int(zone[4:6])
        if hours > 23 or minutes > 59:
            return None
        offset = (hours * 60 + minutes) * (1 if zone[0] == "+" else -1)
    return ((parsed.toordinal() - EPOCH) * DAY +
            (hour * 3600 + minute * 60 + second - offset * 60) * 1000)


def rows(paths, columns):
    """Only LF terminates a record; quotes must wrap a whole field."""
    for path in paths:
        with Path(path).open(encoding="utf-8", newline="\n") as stream:
            next(stream, None)  # Only the first record is a header.
            for line in stream:
                if line.endswith("\n"):
                    line = line[:-1]
                fields = line.split(",")
                if len(fields) != columns:
                    continue
                valid = True
                for index, field in enumerate(fields):
                    if '"' in field:
                        if len(field) >= 2 and field[0] == field[-1] == '"' and '"' not in field[1:-1]:
                            fields[index] = field[1:-1]
                        else:
                            valid = False
                            break
                if valid:
                    yield fields


# Preserve the Python path for differential checks and source-only use.
PARSER_NAME = "python"
if os.environ.get("SS_PYTHON_PARSER") != "1":
    try:
        from _fastparse import rows, timestamp
        PARSER_NAME = "compiled"
    except ImportError:
        pass


def solve(data, output):
    started = time.monotonic()
    manifest = json.loads((data / "manifest.json").read_text())
    start, end = timestamp(manifest["window_start"]), timestamp(manifest["window_end"])
    products = dict(rows([data / "products.csv"], 2))
    campaigns = dict(rows([data / "campaigns.csv"], 2))
    resolve_person = load_identity(sorted((data / "links").glob("*.csv")), rows)

    if IDENTITY_NAME == "compiled":
        # Native loaders cache per phase; the flat link map makes a persistent
        # duplicate user cache unnecessary, including IDs with no final events.
        def person_for(user):
            person = resolve_person(user)
            return user if person is None else person
    else:
        person_cache = {}
        def person_for(user):
            person = person_cache.get(user)
            if person is None:
                person = resolve_person(user)
                if person is None:
                    person = user
                person_cache[user] = person
            return person

    deduplicated = load_events(sorted((data / "events").glob("*.csv")), start, end,
                               products, person_for, rows, timestamp)
    session_stream = prepare_sessions(deduplicated)
    session_engine = type(session_stream).__name__
    del deduplicated
    person_codes = session_stream.people
    clicks = load_clicks(sorted((data / "clicks").glob("*.csv")), start, end,
                         campaigns, person_codes, person_for, rows, timestamp)
    # No identity lookups remain after both loaders finish. Payloads and session
    # names own their strings; release link/alias maps before daily aggregation.
    del person_for, resolve_person
    if IDENTITY_NAME != "compiled":
        del person_cache

    native_rows = try_aggregate(session_stream, clicks, campaigns)
    if native_rows is None:
        aggregate_engine = "python"
        users = {}
        campaign_totals = defaultdict(lambda: [0, 0, 0, 0])
        for person, session_start, count, revenue, session_categories in session_stream:
            day = date.fromordinal(EPOCH + session_start // DAY)
            key = (person, day)
            user_row = users.get(key)
            if user_row is None:
                user_row = users[key] = [0, 0, 0, None]
            user_row[0] += 1
            user_row[1] += count
            user_row[2] += revenue
            if session_categories:
                categories = user_row[3]
                if categories is None:
                    categories = user_row[3] = {}
                for category, amount in session_categories.items():
                    categories[category] = categories.get(category, 0) + amount
            credited, assists = clicks.credit(person_codes[person], session_start)
            row = campaign_totals[(credited, day)]
            row[0] += 1
            row[1] += int(revenue > 0)
            row[2] += revenue
            for campaign in assists:
                campaign_totals[(campaign, day)][3] += 1

        # Daily totals own the strings they use. These completed phase buffers must
        # not overlap the row lists and columnar output allocations.
        del clicks, session_stream, person_codes
        user_rows = []
        for (person, day), (sessions, count, revenue, categories) in users.items():
            category = min(categories, key=lambda name: (-categories[name], name)) if categories else None
            user_rows.append((person, day, sessions, count, revenue, category))
        campaign_rows = [(campaign, campaigns.get(campaign, "organic"), day, *totals)
                         for (campaign, day), totals in campaign_totals.items()]
    else:
        aggregate_engine = "compiled"
        user_rows, campaign_rows = native_rows
        del native_rows, clicks, session_stream, person_codes
    for name, result, schema in (("user_daily", user_rows, USER_SCHEMA),
                                 ("campaign_daily", campaign_rows, CAMPAIGN_SCHEMA)):
        folder = output / name
        folder.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(result, schema=schema, orient="row").write_parquet(folder / "part-0.parquet")
    print(f"parser={PARSER_NAME} store={STORE_NAME} events={LOADER_NAME} links={IDENTITY_NAME} clicks={CLICK_NAME} sessions={session_engine} aggregate={aggregate_engine}: {len(user_rows)} user rows, {len(campaign_rows)} campaign rows; {time.monotonic() - started:.3f}s", file=sys.stderr)


if __name__ == "__main__":
    data = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATA_DIR", "/data"))
    output = Path(sys.argv[2] if len(sys.argv) > 2 else os.environ.get("OUTPUT_DIR", "/output"))
    solve(data, output)
