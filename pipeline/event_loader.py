"""Native event loading with the previous Python validation loop as fallback."""
import os
from event_store import EventStore

DAY = 86400000


def digits(value):
    return bool(value) and value.isascii() and value.isdigit()


def python_load_events(paths, start, end, products, person_for, rows, timestamp, store_type=EventStore):
    deduplicated = store_type()
    for fields in rows(paths, 7):
        identifier, user, raw_ts, raw_ingested, kind, product, raw_price = fields
        if not identifier:
            continue
        ingested = timestamp(raw_ingested)
        if ingested is None:
            raise ValueError("ingest_ts must be valid F1 per the input contract")
        token = deduplicated.consider(identifier, ingested)
        if token is None:
            continue
        # Keep an earliest-ingest tombstone even if this copy fails validation.
        # A later valid duplicate must never rescue an invalid earliest copy.
        ts = timestamp(raw_ts)
        if not user or ts is None or not start <= ts < end or kind not in {"view", "cart", "purchase"} or not product:
            continue
        if ingested - ts > DAY:
            continue
        price = 0
        category = None
        if kind == "purchase":
            if not digits(raw_price):
                continue
            significant = raw_price.lstrip("0") or "0"
            if len(significant) > 8:
                continue
            price = int(significant)
            if not 1 <= price <= 10_000_000:
                continue
            category = products.get(product, "unknown")
        deduplicated.accept(token, person_for(user), ts, price, category)
    return deduplicated


native_load = None
LOADER_NAME = 'python'
if all(os.environ.get(name) != '1' for name in ('SS_PYTHON_EVENTS', 'SS_PYTHON_PARSER', 'SS_PYTHON_STORE')):
    try:
        if os.environ.get('SS_PYTHON_EVENT_INDEX') == '1':
            from _fastparse import load_events_dict as native_load
            LOADER_NAME = 'compiled-dict'
        else:
            from _fastparse import load_events as native_load
            LOADER_NAME = 'compiled-bytes'
    except ImportError:
        pass


def load_events(paths, start, end, products, person_for, rows, timestamp):
    if native_load is not None:
        return native_load(paths, start, end, products, person_for)
    return python_load_events(paths, start, end, products, person_for, rows, timestamp)
