/* Packed numeric records; only shared person/category strings retain references.
 * The mutable API uses exact Python keys; bulk loading can use exact bytes. */
typedef struct {
    int64_t ingested, ts;
    PyObject *person, *category;
    int price;
} EventRecord;

#define STORE_BLOCK 4096
typedef struct {
    PyObject_HEAD
    PyObject *index;
    PyObject *prepared;
    EventRecord **blocks;
    Py_ssize_t length, capacity, position;
    int sealed;
} EventStore;

static EventRecord *store_record(EventStore *self, Py_ssize_t index) {
    return &self->blocks[index / STORE_BLOCK][index % STORE_BLOCK];
}

static PyObject *store_new(PyTypeObject *type, PyObject *args, PyObject *kw) {
    EventStore *self = (EventStore *)type->tp_alloc(type, 0);
    if (!self) return NULL;
    self->index = PyDict_New();
    if (!self->index) { Py_DECREF(self); return NULL; }
    return (PyObject *)self;
}

static void store_release_records(EventStore *self) {
    for (Py_ssize_t block = 0; block < self->capacity; ++block) {
        if (!self->blocks[block]) continue;
        Py_ssize_t count = self->length - block * STORE_BLOCK;
        if (count > STORE_BLOCK) count = STORE_BLOCK;
        for (Py_ssize_t i = 0; i < count; ++i) {
            Py_XDECREF(self->blocks[block][i].person);
            Py_XDECREF(self->blocks[block][i].category);
        }
        free(self->blocks[block]);
    }
    free(self->blocks);
    self->blocks = NULL;
    self->length = self->capacity = self->position = 0;
}

static void store_dealloc(EventStore *self) {
    store_release_records(self);
    Py_XDECREF(self->prepared);
    Py_XDECREF(self->index);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int store_allocate_record(EventStore *self) {
    if (self->length == PY_SSIZE_T_MAX) { PyErr_NoMemory(); return -1; }
    Py_ssize_t block = self->length / STORE_BLOCK;
    if (block == self->capacity) {
        size_t maximum = SIZE_MAX / sizeof(EventRecord *);
        size_t next = self->capacity ? (size_t)self->capacity * 2 : 4;
        if (next > maximum || next > (size_t)PY_SSIZE_T_MAX) { PyErr_NoMemory(); return -1; }
        EventRecord **grown = realloc(self->blocks, next * sizeof(EventRecord *));
        if (!grown) { PyErr_NoMemory(); return -1; }
        memset(grown + self->capacity, 0, (next - self->capacity) * sizeof(EventRecord *));
        self->blocks = grown;
        self->capacity = (Py_ssize_t)next;
    }
    if (!self->blocks[block]) {
        self->blocks[block] = calloc(STORE_BLOCK, sizeof(EventRecord));
        if (!self->blocks[block]) { PyErr_NoMemory(); return -1; }
    }
    return 0;
}

static PyObject *store_reserve(EventStore *self, PyObject *identifier, long long ingested) {
    if (self->sealed || !self->index) return PyErr_Format(PyExc_RuntimeError, "store no longer accepts records");
    PyObject *token = PyDict_GetItemWithError(self->index, identifier);
    Py_ssize_t offset;
    if (token) {
        offset = PyLong_AsSsize_t(token);
        if (offset == -1 && PyErr_Occurred()) return NULL;
        if (ingested >= store_record(self, offset)->ingested) Py_RETURN_NONE;
        Py_INCREF(token);
    } else {
        if (PyErr_Occurred()) return NULL;
        if (store_allocate_record(self) < 0) return NULL;
        offset = self->length;
        token = PyLong_FromSsize_t(offset);
        if (!token) return NULL;
        if (PyDict_SetItem(self->index, identifier, token) < 0) { Py_DECREF(token); return NULL; }
        ++self->length;
    }
    EventRecord *record = store_record(self, offset);
    record->ingested = ingested;
    /* Clear a previous valid copy BEFORE validation of this earlier copy. */
    Py_CLEAR(record->person);
    Py_CLEAR(record->category);
    return token;
}

static PyObject *store_consider(EventStore *self, PyObject *args) {
    PyObject *identifier;
    long long ingested;
    if (!PyArg_ParseTuple(args, "UL", &identifier, &ingested)) return NULL;
    return store_reserve(self, identifier, ingested);
}

static PyObject *store_accept(EventStore *self, PyObject *args) {
    Py_ssize_t offset;
    PyObject *person, *category;
    long long ts;
    int price;
    if (!PyArg_ParseTuple(args, "nULiO", &offset, &person, &ts, &price, &category)) return NULL;
    if (self->sealed || !self->index) return PyErr_Format(PyExc_RuntimeError, "store no longer accepts records");
    if (offset < 0 || offset >= self->length) return PyErr_Format(PyExc_IndexError, "invalid record token");
    if (category != Py_None && !PyUnicode_Check(category))
        return PyErr_Format(PyExc_TypeError, "category must be str or None");
    EventRecord *record = store_record(self, offset);
    Py_XSETREF(record->person, Py_NewRef(person));
    Py_XSETREF(record->category, Py_NewRef(category));
    record->ts = ts;
    record->price = price;
    Py_RETURN_NONE;
}

static PyObject *store_iter(EventStore *self) {
    /* Once parsing ends, IDs and the hash index are no longer needed. */
    self->sealed = 1;
    Py_CLEAR(self->index);
    return Py_NewRef((PyObject *)self);
}

static PyObject *prepared_event_next(PyObject *prepared);

static PyObject *store_next(EventStore *self) {
    if (!self->sealed) return PyErr_Format(PyExc_RuntimeError, "call iter(store) first");
    if (self->prepared) {
        PyObject *row = prepared_event_next(self->prepared);
        if (!row && !PyErr_Occurred()) Py_CLEAR(self->prepared);
        return row;
    }
    while (self->position < self->length) {
        EventRecord *record = store_record(self, self->position);
        PyObject *payload = NULL;
        if (record->person) {
            payload = Py_BuildValue("OLiO", record->person, (long long)record->ts,
                                   record->price, record->category);
            if (!payload) return NULL;
        }
        Py_CLEAR(record->person);
        Py_CLEAR(record->category);
        ++self->position;
        if (self->position % STORE_BLOCK == 0 || self->position == self->length) {
            Py_ssize_t block = (self->position - 1) / STORE_BLOCK;
            free(self->blocks[block]);
            self->blocks[block] = NULL;
        }
        if (payload) return payload;
    }
    free(self->blocks);
    self->blocks = NULL;
    self->length = self->capacity = self->position = 0;
    return NULL;
}

#include "session_store.h"

static PyMethodDef store_methods[] = {
    {"consider", (PyCFunction)store_consider, METH_VARARGS, "Record earliest ingest and invalidate replaced payload."},
    {"accept", (PyCFunction)store_accept, METH_VARARGS, "Attach a validated payload to a record token."},
    {"sessions", (PyCFunction)store_sessions, METH_NOARGS, "Seal records and produce sorted session summaries."},
    {NULL, NULL, 0, NULL}
};
static PyTypeObject EventStoreType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_fastparse.EventStore",
    .tp_basicsize = sizeof(EventStore),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = store_new,
    .tp_dealloc = (destructor)store_dealloc,
    .tp_iter = (getiterfunc)store_iter,
    .tp_iternext = (iternextfunc)store_next,
    .tp_methods = store_methods,
};
