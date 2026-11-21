/* Post-dedup compact records: no per-event Python timestamp or event tuple. */
typedef struct {
    int64_t ts;
    PyObject *category;
    uint32_t person;
    int price;
} SessionEvent;

typedef struct {
    PyObject_HEAD
    SessionEvent *records;
    Py_ssize_t length, position;
    PyObject *people, *names;
} Sessions;

/* Dense codes are assigned once per canonical person, not once per event. */
static PyObject *session_person_code(Sessions *self, PyObject *person) {
    PyObject *code = PyDict_GetItemWithError(self->people, person);
    if (code || PyErr_Occurred()) return code;
    Py_ssize_t index = PyList_GET_SIZE(self->names);
    if (index >= UINT32_MAX) { PyErr_SetString(PyExc_OverflowError, "too many people"); return NULL; }
    code = PyLong_FromSsize_t(index);
    if (!code) return NULL;
    int rc = PyDict_SetItem(self->people, person, code);
    Py_DECREF(code);
    if (rc < 0 || PyList_Append(self->names, person) < 0) return NULL;
    return PyDict_GetItemWithError(self->people, person);
}

/* Preserve the event-store iterator API even when no expanded EventRecord
 * allocation exists. It consumes categories just like the old iterator. */
static PyObject *prepared_event_next(PyObject *prepared) {
    Sessions *self = (Sessions *)prepared;
    if (self->position == self->length) return NULL;
    SessionEvent *record = &self->records[self->position];
    PyObject *row = Py_BuildValue("OLiO", PyList_GET_ITEM(self->names, record->person),
                                  (long long)record->ts, record->price, record->category);
    if (row) { Py_CLEAR(record->category); ++self->position; }
    return row;
}

static void sessions_dealloc(Sessions *self) {
    for (Py_ssize_t i = 0; i < self->length; ++i) Py_XDECREF(self->records[i].category);
    free(self->records);
    Py_XDECREF(self->people); Py_XDECREF(self->names);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int session_compare(const void *av, const void *bv) {
    const SessionEvent *a = av, *b = bv;
    if (a->person != b->person) return a->person < b->person ? -1 : 1;
    return (a->ts > b->ts) - (a->ts < b->ts);
}

#include "parallel_sort.h"

static PyObject *sessions_next(Sessions *self) {
    if (self->position == self->length) {
        free(self->records); self->records = NULL;
        self->length = self->position = 0;
        return NULL;
    }
    Py_ssize_t first = self->position, limit = first + 1;
    SessionEvent *record = &self->records[first];
    while (limit < self->length && self->records[limit].person == record->person &&
           (uint64_t)self->records[limit].ts - (uint64_t)self->records[limit - 1].ts <= 1800000)
        ++limit;
    int64_t revenue = 0;
    PyObject *categories = NULL;
    for (Py_ssize_t i = first; i < limit; ++i) {
        SessionEvent *event = &self->records[i];
        if (!event->price) continue;
        if (event->price < 0 || revenue > INT64_MAX - event->price) {
            PyErr_SetString(PyExc_OverflowError, "session revenue outside int64"); goto fail;
        }
        revenue += event->price;
        if (!categories) { categories = PyDict_New(); if (!categories) goto fail; }
        PyObject *previous = PyDict_GetItemWithError(categories, event->category);
        if (!previous && PyErr_Occurred()) goto fail;
        long long amount = previous ? PyLong_AsLongLong(previous) : 0;
        if (PyErr_Occurred()) goto fail;
        PyObject *total = PyLong_FromLongLong(amount + event->price);
        if (!total) goto fail;
        int rc = PyDict_SetItem(categories, event->category, total);
        Py_DECREF(total);
        if (rc < 0) goto fail;
    }
    PyObject *result = Py_BuildValue("OLnLO", PyList_GET_ITEM(self->names, record->person),
                                     (long long)record->ts, limit - first, (long long)revenue,
                                     categories ? categories : Py_None);
    if (!result) goto fail;
    for (Py_ssize_t i = first; i < limit; ++i) Py_CLEAR(self->records[i].category);
    self->position = limit;
    Py_XDECREF(categories);
    return result;
fail:
    Py_XDECREF(categories);
    return NULL;
}

static PyObject *sessions_people(Sessions *self, void *closure) {
    return Py_NewRef(self->people);
}
static PyGetSetDef sessions_getset[] = {
    {"people", (getter)sessions_people, NULL, "Canonical person to dense code mapping.", NULL},
    {NULL}
};
static PyTypeObject SessionsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_fastparse.Sessions",
    .tp_basicsize = sizeof(Sessions),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)sessions_dealloc,
    .tp_iter = PyObject_SelfIter,
    .tp_iternext = (iternextfunc)sessions_next,
    .tp_getset = sessions_getset,
};

static PyObject *store_sessions(EventStore *store, PyObject *unused) {
    if (store->sealed) return PyErr_Format(PyExc_RuntimeError, "store already consumed");
    if (store->prepared) {
        Sessions *prepared = (Sessions *)store->prepared;
        store->prepared = NULL;
        store->sealed = 1;
        sort_session_records(&prepared->records, (size_t)prepared->length);
        return (PyObject *)prepared;
    }
    Sessions *self = (Sessions *)SessionsType.tp_alloc(&SessionsType, 0);
    if (!self) return NULL;
    self->people = PyDict_New(); self->names = PyList_New(0);
    if (!self->people || !self->names) goto fail;
    Py_ssize_t count = 0;
    for (Py_ssize_t i = 0; i < store->length; ++i) count += store_record(store, i)->person != NULL;
    if ((size_t)count > SIZE_MAX / sizeof(SessionEvent)) { PyErr_NoMemory(); goto fail; }
    if (count) {
        self->records = malloc((size_t)count * sizeof(SessionEvent));
        if (!self->records) { PyErr_NoMemory(); goto fail; }
    }
    store->sealed = 1;
    Py_CLEAR(store->index);
    for (Py_ssize_t i = 0; i < store->length; ++i) {
        EventRecord *record = store_record(store, i);
        if (record->person) {
            PyObject *code = PyDict_GetItemWithError(self->people, record->person);
            if (!code) {
                if (PyErr_Occurred()) goto fail;
                Py_ssize_t index = PyList_GET_SIZE(self->names);
                if (index >= UINT32_MAX) { PyErr_SetString(PyExc_OverflowError, "too many people"); goto fail; }
                code = PyLong_FromSsize_t(index);
                if (!code) goto fail;
                int rc = PyDict_SetItem(self->people, record->person, code);
                Py_DECREF(code);
                if (rc < 0) goto fail;
                if (PyList_Append(self->names, record->person) < 0) goto fail;
                code = PyDict_GetItemWithError(self->people, record->person);
            }
            Py_ssize_t person = PyLong_AsSsize_t(code);
            if (PyErr_Occurred()) goto fail;
            self->records[self->length++] = (SessionEvent){record->ts, Py_NewRef(record->category),
                                                          (uint32_t)person, record->price};
        }
        Py_CLEAR(record->person); Py_CLEAR(record->category);
        if ((i + 1) % STORE_BLOCK == 0 || i + 1 == store->length) {
            free(store->blocks[i / STORE_BLOCK]);
            store->blocks[i / STORE_BLOCK] = NULL;
        }
    }
    free(store->blocks); store->blocks = NULL;
    store->length = store->capacity = store->position = 0;
    sort_session_records(&self->records, (size_t)self->length);
    return (PyObject *)self;
fail:
    /* A failure after sealing may leave some blocks transferred/freed. Make
     * the consumed store safely empty even if a caller catches the exception. */
    if (store->sealed) store_release_records(store);
    Py_DECREF(self);
    return NULL;
}
