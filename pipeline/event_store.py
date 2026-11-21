"""Earliest-ingest storage; Python implementation remains a differential fallback."""
import os


class PythonEventStore:
    def __init__(self):
        self.records = {}

    def consider(self, identifier, ingested):
        previous = self.records.get(identifier)
        if previous is not None and ingested >= previous[0]:
            return None
        self.records[identifier] = (ingested, None)
        return identifier

    def accept(self, token, person, ts, price, category):
        self.records[token] = (self.records[token][0], (person, ts, price, category))

    def __iter__(self):
        while self.records:
            _, (_, payload) = self.records.popitem()
            if payload is not None:
                yield payload


EventStore = PythonEventStore
STORE_NAME = "python"
if os.environ.get("SS_PYTHON_STORE") != "1":
    try:
        from _fastparse import EventStore
        STORE_NAME = "compiled"
    except ImportError:
        pass
