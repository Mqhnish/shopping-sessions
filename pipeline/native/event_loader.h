/* Keep all row-level validation in C. The strict CSV/timestamp primitives and
 * earliest-ingest tombstone store are shared with the differential API. */
#include "sharded_events.h"

static PyObject *load_events_impl(PyObject *args, int use_bytes) {
    PyObject *paths, *products, *resolve;
    long long start, end;
    unsigned hash_bits = 64;
    if (!PyArg_ParseTuple(args, "OLLO!O|I", &paths, &start, &end,
                         &PyDict_Type, &products, &resolve, &hash_bits)) return NULL;
    if (hash_bits > 64) return PyErr_Format(PyExc_ValueError, "hash_bits must be 0..64");
    EventStore *store = (EventStore *)store_new(&EventStoreType, NULL, NULL);
    if (!store) return NULL;
    ByteIndex byte_index = {0};
    ParallelParser parser = {0};
    unsigned threads = parser_threads();
    int use_parallel = use_bytes && threads > 1;
    PyObject *people = PyDict_New(), *unknown = PyUnicode_FromString("unknown");
    PyObject *iterator = NULL, *identifier = NULL, *user = NULL, *product = NULL;
    if (!people || !unknown) goto fail;
    if (use_parallel) {
        if (parallel_parser_init(&parser, paths, start, end, threads) < 0) goto fail;
    } else {
        PyObject *row_args = Py_BuildValue("Oi", paths, 7);
        if (!row_args) goto fail;
        iterator = make_rows(NULL, row_args);
        Py_DECREF(row_args);
        if (!iterator) goto fail;
    }
    int next_row;
    for (;;) {
        ParsedEvent *parsed = NULL;
        RawField parsed_fields[7] = {0}, *fields;
        if (use_parallel) {
            next_row = parallel_parser_next(&parser, &parsed);
            if (next_row <= 0) break;
            parsed_fields[0] = parsed->identifier;
            parsed_fields[1] = parsed->user;
            parsed_fields[5] = parsed->product;
            fields = parsed_fields;
        } else {
            next_row = rows_raw((Rows *)iterator);
            if (next_row <= 0) break;
            fields = ((Rows *)iterator)->fields;
        }
        if (!fields[0].length) continue;
        if (!use_bytes) {
            identifier = PyUnicode_DecodeUTF8(fields[0].data, fields[0].length, "strict");
            if (!identifier) goto fail;
        }
        int64_t ingested, ts;
        int valid;
        if (parsed) ingested = parsed->ingested;
        else {
            valid = timestamp_bytes(fields[3].data, fields[3].length, &ingested);
            if (!valid) { PyErr_SetString(PyExc_ValueError, "ingest_ts must be valid F1 per the input contract"); goto fail; }
        }
        /* This records an invalid tombstone and clears any later valid payload
         * before validating the candidate, regardless of which file it is in. */
        Py_ssize_t offset;
        if (use_bytes) {
            int accepted = byte_store_consider(store, &byte_index, fields[0], ingested, hash_bits, &offset);
            if (accepted < 0) goto fail;
            if (!accepted) goto next;
        } else {
            PyObject *token = store_reserve(store, identifier, ingested);
            if (!token) goto fail;
            if (token == Py_None) { Py_DECREF(token); goto next; }
            offset = PyLong_AsSsize_t(token);
            Py_DECREF(token);
            if (PyErr_Occurred()) goto fail;
        }
        int price = 0, purchase;
        PyObject *category = Py_None;
        if (parsed) {
            if (!parsed->valid) goto next;
            ts = parsed->ts; price = parsed->price; purchase = price > 0;
        } else {
            valid = timestamp_bytes(fields[2].data, fields[2].length, &ts);
            if (!valid || !fields[1].length || !fields[5].length || ts < start || ts >= end) goto next;
            purchase = fields[4].length == 8 && !memcmp(fields[4].data, "purchase", 8);
            if (!purchase && !(fields[4].length == 4 && (!memcmp(fields[4].data, "view", 4) ||
                                                         !memcmp(fields[4].data, "cart", 4)))) goto next;
            /* ingest_ts is valid F1, and the event timestamp is window bounded. */
            if (ingested > ts && (uint64_t)ingested - (uint64_t)ts > 86400000) goto next;
            if (purchase) {
                Py_ssize_t length = fields[6].length;
                const char *raw = fields[6].data;
                if (!length) goto next;
                for (Py_ssize_t i = 0; i < length; ++i) {
                    if (raw[i] < '0' || raw[i] > '9') goto next;
                    price = price * 10 + raw[i] - '0';
                    if (price > 10000000) goto next;
                }
                if (!price) goto next;
            }
        }
        user = PyUnicode_DecodeUTF8(fields[1].data, fields[1].length, "strict");
        if (!user) goto fail;
        PyObject *person = PyDict_GetItemWithError(people, user);
        if (!person) {
            if (PyErr_Occurred()) goto fail;
            person = PyObject_CallOneArg(resolve, user);
            if (!person) goto fail;
            if (!PyUnicode_Check(person)) {
                Py_DECREF(person); PyErr_SetString(PyExc_TypeError, "person must be str"); goto fail;
            }
            int rc = PyDict_SetItem(people, user, person);
            Py_DECREF(person);
            if (rc < 0) goto fail;
            person = PyDict_GetItemWithError(people, user);
        }
        if (purchase) {
            product = PyUnicode_DecodeUTF8(fields[5].data, fields[5].length, "strict");
            if (!product) goto fail;
            category = PyDict_GetItemWithError(products, product);
            if (!category) { if (PyErr_Occurred()) goto fail; category = unknown; }
        }
        EventRecord *record = store_record(store, offset);
        record->person = Py_NewRef(person);
        record->category = Py_NewRef(category);
        record->ts = ts; record->price = price;
next:
        Py_CLEAR(identifier); Py_CLEAR(user); Py_CLEAR(product);
    }
    if (next_row < 0) goto fail;
    Py_XDECREF(iterator); Py_DECREF(people); Py_DECREF(unknown);
    parallel_parser_close(&parser);
    byte_index_free(&byte_index);
    /* The temporary byte index is no longer available for mutation. Iteration
     * and session preparation remain valid; dictionary mode stays mutable. */
    if (use_bytes) Py_CLEAR(store->index);
    return (PyObject *)store;
fail:
    parallel_parser_close(&parser);
    Py_XDECREF(identifier); Py_XDECREF(user); Py_XDECREF(product); Py_XDECREF(iterator); Py_XDECREF(people); Py_XDECREF(unknown);
    byte_index_free(&byte_index);
    Py_DECREF(store);
    return NULL;
}

static PyObject *load_events(PyObject *module, PyObject *args) {
    const char *serial = getenv("SS_SERIAL_DEDUP");
    if (parser_threads() > 1 && !(serial && !strcmp(serial, "1"))) return load_sharded_events(args);
    return load_events_impl(args, 1);
}
static PyObject *load_events_dict(PyObject *module, PyObject *args) {
    return load_events_impl(args, 0);
}
