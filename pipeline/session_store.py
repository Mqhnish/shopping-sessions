"""Compact session summaries with a readable fallback for differential checks."""
from collections import defaultdict
import os


class PythonSessions:
    def __init__(self, store):
        self.events = defaultdict(list)
        for person, ts, price, category in store:
            self.events[person].append((ts, price, category))
        self.people = {person: code for code, person in enumerate(self.events)}

    def __iter__(self):
        for person, events in self.events.items():
            events.sort(key=lambda event: event[0])
            position = 0
            while position < len(events):
                limit = position + 1
                while limit < len(events) and events[limit][0] - events[limit - 1][0] <= 1800000:
                    limit += 1
                revenue, categories = 0, None
                for _, price, category in events[position:limit]:
                    if price:
                        revenue += price
                        if categories is None:
                            categories = {}
                        categories[category] = categories.get(category, 0) + price
                yield person, events[position][0], limit - position, revenue, categories
                position = limit


def prepare_sessions(store):
    if os.environ.get('SS_PYTHON_SESSIONS') != '1' and hasattr(store, 'sessions'):
        return store.sessions()
    return PythonSessions(store)
