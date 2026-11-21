#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* Strict, LF-only dialect. No normalization or CSV-library CR handling. */
typedef struct { const char *data; Py_ssize_t length; } RawField;

typedef struct {
    PyObject_HEAD
    PyObject *paths;
    FILE *file;
    char *line;
    size_t capacity;
    Py_ssize_t columns;
    RawField *fields;
} Rows;

static int open_next(Rows *self) {
    PyObject *path = PyIter_Next(self->paths);
    if (!path) return PyErr_Occurred() ? -1 : 0;
    PyObject *bytes = NULL;
    if (!PyUnicode_FSConverter(path, &bytes)) { Py_DECREF(path); return -1; }
    self->file = fopen(PyBytes_AS_STRING(bytes), "rb");
    if (!self->file) PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path);
    Py_DECREF(bytes); Py_DECREF(path);
    if (!self->file) return -1;
    /* Only the first LF-delimited record is a header. */
    if (getline(&self->line, &self->capacity, self->file) < 0 && ferror(self->file)) {
        PyErr_SetFromErrno(PyExc_OSError); return -1;
    }
    return 1;
}

static int csv_fields(const char *line, Py_ssize_t length, Py_ssize_t columns, RawField *fields) {
    Py_ssize_t count = 1;
    for (ssize_t i = 0; i < length; ++i) count += line[i] == ',';
    if (count != columns) return 0;
    ssize_t begin = 0;
    int valid = 1;
    for (Py_ssize_t column = 0; column < count; ++column) {
        ssize_t end = begin;
        while (end < length && line[end] != ',') ++end;
        ssize_t first = begin, last = end;
        if (first < last && line[first] == '"') {
            if (last - first < 2 || line[last - 1] != '"') { valid = 0; break; }
            ++first; --last;
        }
        if (memchr(line + first, '"', (size_t)(last - first))) { valid = 0; break; }
        fields[column] = (RawField){line + first, last - first};
        begin = end + 1;
    }
    return valid;
}

/* Populate borrowed spans only after validating the entire LF record. */
static int rows_raw(Rows *self) {
    for (;;) {
        if (!self->file) {
            int result = open_next(self);
            if (result <= 0) return result;
        }
        ssize_t length = getline(&self->line, &self->capacity, self->file);
        if (length < 0) {
            int failed = ferror(self->file);
            if (failed) PyErr_SetFromErrno(PyExc_OSError);
            fclose(self->file); self->file = NULL;
            if (failed) return -1;
            continue;
        }
        if (length && self->line[length - 1] == '\n') --length;
        if (csv_fields(self->line, length, self->columns, self->fields)) return 1;
    }
}

static PyObject *rows_next(Rows *self) {
    if (rows_raw(self) <= 0) return NULL;
    PyObject *fields = PyList_New(self->columns);
    if (!fields) return NULL;
    for (Py_ssize_t column = 0; column < self->columns; ++column) {
        RawField field = self->fields[column];
        PyObject *value = PyUnicode_DecodeUTF8(field.data, field.length, "strict");
        if (!value) { Py_DECREF(fields); return NULL; }
        PyList_SET_ITEM(fields, column, value);
    }
    return fields;
}

static void rows_dealloc(Rows *self) {
    if (self->file) fclose(self->file);
    free(self->line);
    free(self->fields);
    Py_XDECREF(self->paths);
    Py_TYPE(self)->tp_free((PyObject *)self);
}
static PyTypeObject RowsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_fastparse.Rows",
    .tp_basicsize = sizeof(Rows),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)rows_dealloc,
    .tp_iter = PyObject_SelfIter,
    .tp_iternext = (iternextfunc)rows_next,
};
static PyObject *make_rows(PyObject *module, PyObject *args) {
    PyObject *paths;
    Py_ssize_t columns;
    if (!PyArg_ParseTuple(args, "On", &paths, &columns)) return NULL;
    if (columns < 1) return PyErr_Format(PyExc_ValueError, "columns must be positive");
    PyObject *iterator = PyObject_GetIter(paths);
    if (!iterator) return NULL;
    Rows *self = PyObject_New(Rows, &RowsType);
    if (!self) { Py_DECREF(iterator); return NULL; }
    self->paths = iterator; self->file = NULL; self->line = NULL;
    self->capacity = 0; self->columns = columns;
    self->fields = NULL;
    if ((size_t)columns > SIZE_MAX / sizeof(RawField)) { Py_DECREF(self); return PyErr_NoMemory(); }
    self->fields = malloc((size_t)columns * sizeof(RawField));
    if (!self->fields) { Py_DECREF(self); return PyErr_NoMemory(); }
    return (PyObject *)self;
}

static int number(const char *p, int n) {
    int result = 0;
    for (int i = 0; i < n; ++i) {
        if (p[i] < '0' || p[i] > '9') return -1;
        result = result * 10 + p[i] - '0';
    }
    return result;
}
/* Gregorian civil date to days since 1970-01-01, including years 1..9999. */
static int64_t civil_days(int y, int m, int d) {
    y -= m <= 2;
    int era = y / 400;
    unsigned yo = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    return era * 146097LL + yo * 365 + yo / 4 - yo / 100 + doy - 719468;
}
typedef struct { uint32_t keys[64]; int64_t days[64]; } TimestampCache;
static int timestamp_with_cache(const char *s, Py_ssize_t n, int64_t *output, TimestampCache *cache) {
    if (!n) return 0;
    int all_digits = 1;
    for (Py_ssize_t i = 0; i < n; ++i) {
        if (s[i] < '0' || s[i] > '9') { all_digits = 0; break; }
    }
    if (all_digits) {
        Py_ssize_t first = 0;
        while (first < n && s[first] == '0') ++first;
        /* Larger values cannot be in any supported calendar window. */
        if (n - first > 18) return 0;
        int64_t value = 0;
        for (; first < n; ++first) value = value * 10 + s[first] - '0';
        *output = value; return 1;
    }
    if ((n != 20 && n != 25) || s[4] != '-' || s[7] != '-' ||
        s[10] != 'T' || s[13] != ':' || s[16] != ':') return 0;
    int y = number(s, 4), m = number(s + 5, 2), d = number(s + 8, 2);
    int h = number(s + 11, 2), minute = number(s + 14, 2), sec = number(s + 17, 2);
    if (y < 1 || m < 1 || m > 12 || d < 1 || d > 31 || h < 0 || h > 23 ||
        minute < 0 || minute > 59 || sec < 0 || sec > 59) return 0;
    uint32_t key = (uint32_t)y * 512 + (uint32_t)m * 32 + (uint32_t)d;
    unsigned slot = key & 63;
    int64_t days;
    if (cache && cache->keys[slot] == key) days = cache->days[slot];
    else {
        static const int month_days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
        int max_day = month_days[m - 1] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
        if (d > max_day) return 0;
        days = civil_days(y, m, d);
        if (cache) { cache->keys[slot] = key; cache->days[slot] = days; }
    }
    int offset = 0;
    if (n == 20) {
        if (s[19] != 'Z') return 0;
    } else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return 0;
        int oh = number(s + 20, 2), om = number(s + 23, 2);
        if (oh < 0 || oh > 23 || om < 0 || om > 59) return 0;
        offset = (oh * 60 + om) * (s[19] == '+' ? 1 : -1);
    }
    *output = days * 86400000LL +
              (h * 3600LL + minute * 60 + sec - offset * 60) * 1000;
    return 1;
}
static int timestamp_bytes(const char *s, Py_ssize_t n, int64_t *output) {
    return timestamp_with_cache(s, n, output, NULL);
}
static PyObject *timestamp(PyObject *module, PyObject *arg) {
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(arg, &n);
    if (!s) return NULL;
    int64_t value;
    if (!timestamp_bytes(s, n, &value)) Py_RETURN_NONE;
    return PyLong_FromLongLong(value);
}

#include "event_store.h"
#include "byte_index.h"
#include "parallel_parse.h"
#include "click_store.h"
#include "event_loader.h"
#include "links.h"
#include "aggregate.h"

static PyMethodDef methods[] = {
    {"rows", make_rows, METH_VARARGS, "Read strict LF-delimited CSV records."},
    {"timestamp", timestamp, METH_O, "Parse strict ASCII timestamps to milliseconds."},
    {"load_clicks", load_clicks, METH_VARARGS, "Load packed click records for session-start attribution."},
    {"load_events", load_events, METH_VARARGS, "Parse, deduplicate and validate events into native records."},
    {"load_events_dict", load_events_dict, METH_VARARGS, "Load events with the differential Python dictionary index."},
    {"load_links", load_links, METH_O, "Resolve all linked IDs with packed union/find and lexical minima."},
    {"aggregate_daily", aggregate_daily, METH_VARARGS, "Aggregate sorted native sessions and click windows into daily rows."},
    {NULL, NULL, 0, NULL}
};
static struct PyModuleDef module = {PyModuleDef_HEAD_INIT, "_fastparse", NULL, -1, methods};
PyMODINIT_FUNC PyInit__fastparse(void) {
    if (PyType_Ready(&RowsType) < 0) return NULL;
    if (PyType_Ready(&EventStoreType) < 0) return NULL;
    if (PyType_Ready(&SessionsType) < 0) return NULL;
    if (PyType_Ready(&ClickIndexType) < 0) return NULL;
    PyObject *result = PyModule_Create(&module);
    if (!result) return NULL;
    if (PyModule_AddObjectRef(result, "EventStore", (PyObject *)&EventStoreType) < 0) {
        Py_DECREF(result); return NULL;
    }
    return result;
}
