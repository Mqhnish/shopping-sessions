"""Session-start attribution with a selectable, readable Python fallback."""
from collections import Counter, defaultdict, deque
import os

WEEK = 604800000


class PythonClickIndex:
    def __init__(self, paths, start, end, campaigns, people, person_for, rows, timestamp):
        self.clicks = defaultdict(list)
        seen = set()
        for identifier, user, campaign, raw_ts in rows(paths, 4):
            if not identifier or identifier in seen:
                continue
            seen.add(identifier)
            ts = timestamp(raw_ts)
            if user and campaign in campaigns and ts is not None and start - WEEK <= ts < end:
                code = people.get(person_for(user))
                if code is not None:
                    self.clicks[code].append((ts, identifier, campaign))
        for clicks in self.clicks.values():
            clicks.sort(key=lambda click: click[1], reverse=True)
            clicks.sort(key=lambda click: click[0])
        self.person = None

    def credit(self, person, ts):
        if person != self.person:
            self.person = person
            self.pending = iter(self.clicks.pop(person, []))
            self.following = next(self.pending, None)
            self.active = deque()
            self.assists = Counter()
        while self.following is not None and self.following[0] <= ts:
            self.active.append(self.following)
            self.assists[self.following[2]] += 1
            self.following = next(self.pending, None)
        while self.active and self.active[0][0] < ts - WEEK:
            campaign = self.active.popleft()[2]
            self.assists[campaign] -= 1
            if not self.assists[campaign]:
                del self.assists[campaign]
        return (self.active[-1][2] if self.active else 'organic', tuple(self.assists))


CLICK_NAME = 'python'
native_load = None
if os.environ.get('SS_PYTHON_CLICKS') != '1' and os.environ.get('SS_PYTHON_PARSER') != '1':
    try:
        from _fastparse import load_clicks as native_load
        CLICK_NAME = 'compiled'
    except ImportError:
        pass


def load_clicks(paths, start, end, campaigns, people, person_for, rows, timestamp):
    if native_load is not None:
        return native_load(paths, start - WEEK, end, campaigns, people, person_for)
    return PythonClickIndex(paths, start, end, campaigns, people, person_for, rows, timestamp)
