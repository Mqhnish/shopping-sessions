"""Linked-ID resolution with an unchanged Python union/find fallback."""
import os


class People:
    def __init__(self):
        self.parent, self.size, self.minimum = {}, {}, {}

    def find(self, name):
        parent = self.parent
        root = name
        while parent[root] != root:
            root = parent[root]
        while name != root:
            parent[name], name = root, parent[name]
        return root

    def union(self, a, b):
        for name in (a, b):
            if name not in self.parent:
                self.parent[name] = name
                self.size[name] = 1
                self.minimum[name] = name
        a, b = self.find(a), self.find(b)
        if a == b:
            return
        if self.size[a] < self.size[b]:
            a, b = b, a
        self.parent[b] = a
        self.size[a] += self.size.pop(b)
        self.minimum[a] = min(self.minimum[a], self.minimum.pop(b))

    def person(self, name):
        return self.minimum[self.find(name)] if name in self.parent else name


native_load = None
IDENTITY_NAME = 'python'
if all(os.environ.get(name) != '1' for name in ('SS_PYTHON_LINKS', 'SS_PYTHON_PARSER')):
    try:
        from _fastparse import load_links as native_load
        IDENTITY_NAME = 'compiled'
    except ImportError:
        pass


def load_identity(paths, rows):
    if native_load is not None:
        return native_load(paths).get
    people = People()
    for a, b in rows(paths, 2):
        if a and b:
            people.union(a, b)
    return people.person
