/* Exact click IDs are retained through sorting, then discarded. */
typedef struct {
    int64_t ts;
    PyObject *identifier;
    uint32_t person, campaign;
} ClickRecord;

typedef struct {
    PyObject_HEAD
    ClickRecord *records;
    Py_ssize_t length, capacity, npeople, ncampaigns;
    Py_ssize_t *offsets, left, right, limit;
    PyObject *campaigns;
    int64_t *counts, last_ts;
    uint32_t *active, *positions;
    Py_ssize_t nactive, current;
} ClickIndex;

static void clicks_dealloc(ClickIndex *self) {
    for (Py_ssize_t i = 0; i < self->length; ++i) Py_XDECREF(self->records[i].identifier);
    free(self->records); free(self->offsets); free(self->counts);
    free(self->active); free(self->positions);
    Py_XDECREF(self->campaigns);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int click_compare(const void *av, const void *bv) {
    const ClickRecord *a = av, *b = bv;
    if (a->person != b->person) return a->person < b->person ? -1 : 1;
    if (a->ts != b->ts) return a->ts < b->ts ? -1 : 1;
    /* At equal timestamps the rightmost record must have the smallest ID.
     * Unicode scalar ordering equals UTF-8 byte ordering for valid UTF-8. */
    return -PyUnicode_Compare(a->identifier, b->identifier);
}

static int clicks_advance(ClickIndex *self, Py_ssize_t person, int64_t ts) {
    if (person < 0 || person >= self->npeople) {
        PyErr_SetString(PyExc_IndexError, "invalid person code"); return -1;
    }
    if (person != self->current) {
        /* People must be visited once in increasing code order. */
        if (person < self->current) { PyErr_SetString(PyExc_ValueError, "person codes must increase"); return -1; }
        for (Py_ssize_t i = 0; i < self->nactive; ++i) self->counts[self->active[i]] = 0;
        self->nactive = 0;
        self->current = person;
        self->left = self->right = self->offsets[person];
        self->limit = self->offsets[person + 1];
    } else if (ts < self->last_ts) {
        PyErr_SetString(PyExc_ValueError, "session starts must not decrease"); return -1;
    }
    self->last_ts = ts;
    while (self->right < self->limit && self->records[self->right].ts <= ts) {
        uint32_t c = self->records[self->right++].campaign;
        if (!self->counts[c]++) {
            self->positions[c] = (uint32_t)self->nactive;
            self->active[self->nactive++] = c;
        }
    }
    /* Session timestamps come from valid calendar windows, so subtraction
     * stays within int64 even at the supported years' boundaries. */
    int64_t lower = ts < INT64_MIN + 604800000LL ? INT64_MIN : ts - 604800000LL;
    while (self->left < self->right && self->records[self->left].ts < lower) {
        uint32_t c = self->records[self->left++].campaign;
        if (!--self->counts[c]) {
            uint32_t position = self->positions[c];
            uint32_t moved = self->active[--self->nactive];
            self->active[position] = moved;
            self->positions[moved] = position;
        }
    }
    return 0;
}

static PyObject *clicks_credit(ClickIndex *self, PyObject *args) {
    Py_ssize_t person;
    long long ts;
    if (!PyArg_ParseTuple(args, "nL", &person, &ts)) return NULL;
    if (clicks_advance(self, person, ts) < 0) return NULL;
    PyObject *assists = PyTuple_New(self->nactive);
    if (!assists) return NULL;
    for (Py_ssize_t i = 0; i < self->nactive; ++i)
        PyTuple_SET_ITEM(assists, i, Py_NewRef(PyTuple_GET_ITEM(self->campaigns, self->active[i])));
    Py_ssize_t credited = self->left < self->right ? self->records[self->right - 1].campaign : self->ncampaigns;
    PyObject *result = PyTuple_Pack(2, PyTuple_GET_ITEM(self->campaigns, credited), assists);
    Py_DECREF(assists);
    return result;
}

static PyMethodDef clicks_methods[] = {
    {"credit", (PyCFunction)clicks_credit, METH_VARARGS, "Credit a nondecreasing session start and return assists."},
    {NULL, NULL, 0, NULL}
};
static PyTypeObject ClickIndexType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_fastparse.ClickIndex",
    .tp_basicsize = sizeof(ClickIndex),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)clicks_dealloc,
    .tp_methods = clicks_methods,
};

static PyObject *load_clicks_legacy(PyObject *module, PyObject *args) {
    PyObject *paths, *campaigns, *people, *resolve;
    long long start, end;
    if (!PyArg_ParseTuple(args, "OLLO!O!O", &paths, &start, &end,
                         &PyDict_Type, &campaigns, &PyDict_Type, &people, &resolve)) return NULL;
    Py_ssize_t nc = PyDict_Size(campaigns), np = PyDict_Size(people);
    if (nc >= UINT32_MAX || np >= UINT32_MAX)
        return PyErr_Format(PyExc_OverflowError, "too many people or campaigns");
    ClickIndex *self = (ClickIndex *)ClickIndexType.tp_alloc(&ClickIndexType, 0);
    if (!self) return NULL;
    self->current = -1;
    self->npeople = np; self->ncampaigns = nc;
    PyObject *campaign_codes = NULL, *user_codes = NULL, *seen = NULL, *iterator = NULL, *fields = NULL;
    self->campaigns = PyTuple_New(nc + 1);
    if (!self->campaigns) goto fail;
    PyObject *organic = PyUnicode_FromString("organic");
    if (!organic) goto fail;
    PyTuple_SET_ITEM(self->campaigns, nc, organic);
    campaign_codes = PyDict_New(); user_codes = PyDict_New(); seen = PySet_New(NULL);
    if (!campaign_codes || !user_codes || !seen) goto fail;
    Py_ssize_t pos = 0, code = 0;
    PyObject *key, *value;
    while (PyDict_Next(campaigns, &pos, &key, &value)) {
        PyTuple_SET_ITEM(self->campaigns, code, Py_NewRef(key));
        PyObject *number = PyLong_FromSsize_t(code++);
        if (!number) goto fail;
        int rc = PyDict_SetItem(campaign_codes, key, number);
        Py_DECREF(number);
        if (rc < 0) goto fail;
    }
    self->offsets = calloc((size_t)np + 1, sizeof(Py_ssize_t));
    self->counts = calloc((size_t)nc + 1, sizeof(int64_t));
    self->active = calloc((size_t)nc + 1, sizeof(uint32_t));
    self->positions = calloc((size_t)nc + 1, sizeof(uint32_t));
    if (!self->offsets || !self->counts || !self->active || !self->positions) { PyErr_NoMemory(); goto fail; }
    PyObject *row_args = Py_BuildValue("Oi", paths, 4);
    if (!row_args) goto fail;
    iterator = make_rows(NULL, row_args);
    Py_DECREF(row_args);
    if (!iterator) goto fail;
    while ((fields = PyIter_Next(iterator))) {
        PyObject *identifier = PyList_GET_ITEM(fields, 0), *user = PyList_GET_ITEM(fields, 1);
        PyObject *campaign = PyList_GET_ITEM(fields, 2), *raw_ts = PyList_GET_ITEM(fields, 3);
        if (!PyUnicode_GET_LENGTH(identifier)) goto next;
        /* Rules: dedup on click_id (first copy in file order) BEFORE validation, so an
         * invalid first copy (e.g. ts ending in \r) suppresses later valid copies. */
        int duplicate = PySet_Contains(seen, identifier);
        if (duplicate < 0) goto fail;
        if (duplicate) goto next;
        if (PySet_Add(seen, identifier) < 0) goto fail;
        if (!PyUnicode_GET_LENGTH(user)) goto next;
        PyObject *cc = PyDict_GetItemWithError(campaign_codes, campaign);
        if (!cc) { if (PyErr_Occurred()) goto fail; goto next; }
        PyObject *parsed = timestamp(NULL, raw_ts);
        if (!parsed) goto fail;
        if (parsed == Py_None) { Py_DECREF(parsed); goto next; }
        int64_t ts = PyLong_AsLongLong(parsed);
        Py_DECREF(parsed);
        if (PyErr_Occurred()) goto fail;
        if (ts < start || ts >= end) goto next;
        PyObject *pc = PyDict_GetItemWithError(user_codes, user);
        if (!pc) {
            if (PyErr_Occurred()) goto fail;
            PyObject *person = PyObject_CallOneArg(resolve, user);
            if (!person) goto fail;
            pc = PyDict_GetItemWithError(people, person);
            Py_DECREF(person);
            if (!pc && PyErr_Occurred()) goto fail;
            if (!pc) pc = Py_None;
            if (PyDict_SetItem(user_codes, user, pc) < 0) goto fail;
        }
        if (pc == Py_None) goto next;
        Py_ssize_t person = PyLong_AsSsize_t(pc), campaign_code = PyLong_AsSsize_t(cc);
        if (PyErr_Occurred()) goto fail;
        if (person < 0 || person >= np) { PyErr_SetString(PyExc_ValueError, "invalid person code"); goto fail; }
        if (self->length == self->capacity) {
            size_t capacity = self->capacity ? (size_t)self->capacity + (size_t)self->capacity / 2 : 4096;
            if (capacity > SIZE_MAX / sizeof(ClickRecord) || capacity > (size_t)PY_SSIZE_T_MAX) { PyErr_NoMemory(); goto fail; }
            ClickRecord *grown = realloc(self->records, capacity * sizeof(ClickRecord));
            if (!grown) { PyErr_NoMemory(); goto fail; }
            self->records = grown; self->capacity = (Py_ssize_t)capacity;
        }
        self->records[self->length++] = (ClickRecord){ts, Py_NewRef(identifier), (uint32_t)person, (uint32_t)campaign_code};
        ++self->offsets[person + 1];
next:
        Py_CLEAR(fields);
    }
    if (PyErr_Occurred()) goto fail;
    Py_CLEAR(seen); Py_CLEAR(user_codes); Py_CLEAR(campaign_codes); Py_CLEAR(iterator);
    if (self->length > 1) qsort(self->records, self->length, sizeof(ClickRecord), click_compare);
    if (PyErr_Occurred()) goto fail;
    for (Py_ssize_t i = 0; i < self->length; ++i) Py_CLEAR(self->records[i].identifier);
    for (Py_ssize_t i = 1; i <= np; ++i) self->offsets[i] += self->offsets[i - 1];
    return (PyObject *)self;
fail:
    Py_XDECREF(fields); Py_XDECREF(iterator); Py_XDECREF(seen);
    Py_XDECREF(user_codes); Py_XDECREF(campaign_codes); Py_DECREF(self);
    return NULL;
}

#include "byte_clicks.h"

static PyObject *load_clicks(PyObject *module, PyObject *args) {
    const char *legacy = getenv("SS_LEGACY_CLICKS");
    return legacy && !strcmp(legacy, "1") ? load_clicks_legacy(module, args) : load_byte_clicks(args);
}
