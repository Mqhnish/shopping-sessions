"""Select native aggregation only when both session and click stores support it."""
import os

native_aggregate = None
if os.environ.get('SS_PYTHON_AGGREGATE') != '1':
    try:
        from _fastparse import aggregate_daily as native_aggregate
    except ImportError:
        pass


def try_aggregate(sessions, clicks, campaigns):
    if native_aggregate is not None:
        return native_aggregate(sessions, clicks, campaigns)
    return None
