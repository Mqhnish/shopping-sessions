#include <time.h>

static double event_clock(void) {
    struct timespec value = {0};
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (double)value.tv_sec + (double)value.tv_nsec * 1e-9;
}

typedef struct {
    int64_t ingested, ts;
    uint32_t user, category;
    int price;
} ShardEvent;
typedef struct {
    ByteIndex events, users;
    ShardEvent *records;
    size_t length, capacity;
    size_t *user_offsets;
    size_t user_capacity;
    ParsedEvent **input;
    size_t input_length;
    const ByteIndex *products;
    uint32_t unknown;
    unsigned hash_bits;
    int error;
} EventShard;

static void shard_free(EventShard *shard) {
    byte_index_free(&shard->events); byte_index_free(&shard->users);
    free(shard->records); free(shard->user_offsets);
    memset(shard, 0, sizeof(*shard));
}

/* Every shard owns both mutable indices and all records. Workers make no
 * Python API calls or shared writes; borrowed input views are read only until
 * all workers join, before the parser may reuse or unmap them. */
static void *dedup_event_shard(void *opaque) {
    EventShard *shard = opaque;
    for (size_t i = 0; i < shard->input_length; ++i) {
        ParsedEvent *input = shard->input[i];
        Py_ssize_t offset;
        if (shard->length >= (size_t)PY_SSIZE_T_MAX) { shard->error = 3; return NULL; }
        int fresh = byte_index_find(&shard->events, input->identifier.data, input->identifier.length,
                                    shard->hash_bits, (Py_ssize_t)shard->length, &offset);
        if (fresh < 0) { shard->error = 1; return NULL; }
        if (fresh) {
            if (shard->length == shard->capacity) {
                size_t capacity = shard->capacity ? shard->capacity + shard->capacity / 2 : 1024;
                if (capacity > SIZE_MAX / sizeof(ShardEvent)) { shard->error = 1; return NULL; }
                ShardEvent *grown = realloc(shard->records, capacity * sizeof(ShardEvent));
                if (!grown) { shard->error = 1; return NULL; }
                shard->records = grown; shard->capacity = capacity;
            }
            shard->records[shard->length++] = (ShardEvent){.user = UINT32_MAX};
        }
        ShardEvent *record = &shard->records[offset];
        if (!fresh && input->ingested >= record->ingested) continue;
        record->ingested = input->ingested; record->user = UINT32_MAX;
        if (!input->valid) continue;  /* invalid earliest copy remains a tombstone */
        if (shard->users.count >= UINT32_MAX) { shard->error = 3; return NULL; }
        size_t key_offset = shard->users.used;
        Py_ssize_t user;
        int new_user = byte_index_find(&shard->users, input->user.data, input->user.length, 64,
                                       (Py_ssize_t)shard->users.count, &user);
        if (new_user < 0) { shard->error = 1; return NULL; }
        if (new_user) {
            if ((size_t)user == shard->user_capacity) {
                size_t capacity = shard->user_capacity ? shard->user_capacity + shard->user_capacity / 2 : 1024;
                if (capacity > SIZE_MAX / sizeof(size_t)) { shard->error = 1; return NULL; }
                size_t *grown = realloc(shard->user_offsets, capacity * sizeof(size_t));
                if (!grown) { shard->error = 1; return NULL; }
                shard->user_offsets = grown; shard->user_capacity = capacity;
            }
            shard->user_offsets[user] = key_offset;
        }
        Py_ssize_t category = shard->unknown;
        if (input->price) byte_index_lookup(shard->products, input->product.data, input->product.length, &category);
        *record = (ShardEvent){input->ingested, input->ts, (uint32_t)user, (uint32_t)category, input->price};
    }
    return NULL;
}

static int build_product_codes(PyObject *products, ByteIndex *index, PyObject *categories) {
    PyObject *codes = PyDict_New();
    if (!codes) return -1;
    Py_ssize_t position = 0;
    PyObject *product, *category;
    while (PyDict_Next(products, &position, &product, &category)) {
        if (!PyUnicode_Check(product) || !PyUnicode_Check(category)) {
            PyErr_SetString(PyExc_TypeError, "product/category must be strings"); goto fail;
        }
        PyObject *code = PyDict_GetItemWithError(codes, category);
        if (!code) {
            if (PyErr_Occurred()) goto fail;
            if ((uint64_t)PyList_GET_SIZE(categories) >= UINT32_MAX) { PyErr_SetString(PyExc_OverflowError, "too many categories"); goto fail; }
            code = PyLong_FromSsize_t(PyList_GET_SIZE(categories));
            if (!code) goto fail;
            int rc = PyDict_SetItem(codes, category, code);
            Py_DECREF(code);
            if (rc < 0 || PyList_Append(categories, category) < 0) goto fail;
            code = PyDict_GetItemWithError(codes, category);
        }
        Py_ssize_t value = PyLong_AsSsize_t(code), length;
        if (PyErr_Occurred()) goto fail;
        const char *bytes = PyUnicode_AsUTF8AndSize(product, &length);
        if (!bytes) goto fail;
        Py_ssize_t ignored;
        if (byte_index_find(index, bytes, length, 64, value, &ignored) < 0) goto fail;
    }
    PyObject *unknown = PyUnicode_FromString("unknown");
    if (!unknown) goto fail;
    int rc = PyList_Append(categories, unknown);
    Py_DECREF(unknown); Py_DECREF(codes);
    return rc;
fail:
    Py_DECREF(codes); return -1;
}

static PyObject *load_sharded_events(PyObject *args) {
    PyObject *paths, *products, *resolve;
    long long start, end;
    unsigned hash_bits = 64;
    if (!PyArg_ParseTuple(args, "OLLO!O|I", &paths, &start, &end, &PyDict_Type, &products, &resolve, &hash_bits)) return NULL;
    if (hash_bits > 64) return PyErr_Format(PyExc_ValueError, "hash_bits must be 0..64");
    const char *timing = getenv("SS_PROFILE_EVENTS");
    int profile = timing && !strcmp(timing, "1");
    double began = profile ? event_clock() : 0, parsed_time = 0, scatter_time = 0, dedup_time = 0, transfer_time = 0;
    unsigned count = parser_threads();
    EventShard shards[8] = {0};
    ParallelParser parser = {0};
    ByteIndex product_index = {0}, user_index = {0};
    ParsedEvent **refs = NULL;
    size_t ref_capacity = 0;
    PyObject *categories = PyList_New(0), *canonical = PyList_New(0);
    EventStore *store = NULL;
    Sessions *prepared = NULL;
    const char *expanded = getenv("SS_MATERIALIZE_EVENTS");
    int compact = !(expanded && !strcmp(expanded, "1"));
    if (!categories || !canonical) goto fail;
    if (build_product_codes(products, &product_index, categories) < 0) goto fail;
    if (parallel_parser_init(&parser, paths, start, end, count) < 0) goto fail;
    parser.hash_ids = 1;
    for (unsigned s = 0; s < count; ++s) {
        shards[s].events.no_python_errors = shards[s].users.no_python_errors = 1;
        shards[s].products = &product_index;
        shards[s].unknown = (uint32_t)PyList_GET_SIZE(categories) - 1;
        shards[s].hash_bits = hash_bits;
    }
    int batch;
    for (;;) {
        double mark = profile ? event_clock() : 0;
        batch = parallel_parser_batch(&parser);
        double after_parse = profile ? event_clock() : 0;
        parsed_time += after_parse - mark;
        if (batch <= 0) break;
        size_t lengths[8] = {0}, offsets[8], positions[8], total = 0;
        for (unsigned j = 0; j < parser.jobs; ++j) {
            for (size_t r = 0; r < parser.tasks[j].length; ++r) ++lengths[parser.tasks[j].records[r].hash % count];
        }
        for (unsigned s = 0; s < count; ++s) {
            offsets[s] = positions[s] = total;
            if (lengths[s] > SIZE_MAX - total) { PyErr_NoMemory(); goto fail; }
            total += lengths[s];
        }
        if (total > SIZE_MAX / sizeof(ParsedEvent *)) { PyErr_NoMemory(); goto fail; }
        if (total > ref_capacity) {
            ParsedEvent **grown = realloc(refs, total * sizeof(ParsedEvent *));
            if (!grown) { PyErr_NoMemory(); goto fail; }
            refs = grown; ref_capacity = total;
        }
        for (unsigned j = 0; j < parser.jobs; ++j) {
            for (size_t r = 0; r < parser.tasks[j].length; ++r) {
                ParsedEvent *record = &parser.tasks[j].records[r];
                refs[positions[record->hash % count]++] = record;
            }
        }
        pthread_t workers[8]; int started[8] = {0};
        for (unsigned s = 0; s < count; ++s) {
            shards[s].input = total ? refs + offsets[s] : NULL;
            shards[s].input_length = lengths[s];
        }
        double after_scatter = profile ? event_clock() : 0;
        scatter_time += after_scatter - after_parse;
        Py_BEGIN_ALLOW_THREADS
        for (unsigned s = 0; s < count; ++s) {
            if (!shards[s].input_length) continue;
            if (s + 1 < count && !pthread_create(&workers[s], NULL, dedup_event_shard, &shards[s])) started[s] = 1;
            else dedup_event_shard(&shards[s]);
        }
        for (unsigned s = 0; s < count; ++s) if (started[s]) pthread_join(workers[s], NULL);
        Py_END_ALLOW_THREADS
        if (profile) dedup_time += event_clock() - after_scatter;
        for (unsigned s = 0; s < count; ++s) {
            if (shards[s].error == 1) { PyErr_NoMemory(); goto fail; }
            if (shards[s].error == 3) { PyErr_SetString(PyExc_OverflowError, "too many events/users"); goto fail; }
        }
    }
    if (batch < 0) goto fail;
    parallel_parser_close(&parser); free(refs); refs = NULL;
    /* Dedup is complete before any Python identity callback. Free event-ID
     * indices before allocating the final Python-owned record payloads. */
    for (unsigned s = 0; s < count; ++s) byte_index_free(&shards[s].events);
    store = (EventStore *)store_new(&EventStoreType, NULL, NULL);
    if (!store) goto fail;
    if (compact) {
        prepared = (Sessions *)SessionsType.tp_alloc(&SessionsType, 0);
        if (!prepared) goto fail;
        store->prepared = (PyObject *)prepared; /* store owns cleanup immediately */
        prepared->people = PyDict_New(); prepared->names = PyList_New(0);
        if (!prepared->people || !prepared->names) goto fail;
        size_t valid = 0;
        for (unsigned s = 0; s < count; ++s)
            for (size_t r = 0; r < shards[s].length; ++r)
                valid += shards[s].records[r].user != UINT32_MAX;
        if (valid > (size_t)PY_SSIZE_T_MAX || valid > SIZE_MAX / sizeof(SessionEvent)) { PyErr_NoMemory(); goto fail; }
        if (valid) {
            prepared->records = malloc(valid * sizeof(SessionEvent));
            if (!prepared->records) { PyErr_NoMemory(); goto fail; }
        }
    }
    double transfer_start = profile ? event_clock() : 0;
    for (unsigned s = 0; s < count; ++s) {
        EventShard *shard = &shards[s];
        if (shard->users.count > SIZE_MAX / sizeof(PyObject *)) { PyErr_NoMemory(); goto fail; }
        PyObject **names = calloc(shard->users.count ? shard->users.count : 1, sizeof(PyObject *));
        if (!names) { PyErr_NoMemory(); goto fail; }
        uint32_t *codes = NULL;
        if (compact && shard->users.count) {
            codes = malloc(shard->users.count * sizeof(uint32_t));
            if (!codes) { free(names); PyErr_NoMemory(); goto fail; }
        }
        int failed = 0;
        for (size_t r = 0; r < shard->length; ++r) {
            ShardEvent *record = &shard->records[r];
            if (record->user == UINT32_MAX) continue;
            if (!names[record->user]) {
                const char *key = shard->users.arena + shard->user_offsets[record->user];
                uint64_t length; memcpy(&length, key, sizeof(length)); key += sizeof(length);
                Py_ssize_t code;
                int fresh = byte_index_find(&user_index, key, length, 64, PyList_GET_SIZE(canonical), &code);
                if (fresh < 0) { failed = 1; break; }
                if (fresh) {
                    PyObject *user = PyUnicode_DecodeUTF8(key, length, "strict");
                    if (!user) { failed = 1; break; }
                    PyObject *person = PyObject_CallOneArg(resolve, user);
                    Py_DECREF(user);
                    if (!person) { failed = 1; break; }
                    if (!PyUnicode_Check(person)) { Py_DECREF(person); PyErr_SetString(PyExc_TypeError, "person must be str"); failed = 1; break; }
                    PyObject *value = compact ? session_person_code(prepared, person) : person;
                    int rc = value ? PyList_Append(canonical, value) : -1;
                    Py_DECREF(person);
                    if (rc < 0) { failed = 1; break; }
                }
                names[record->user] = Py_NewRef(PyList_GET_ITEM(canonical, code));
                if (compact) {
                    unsigned long dense = PyLong_AsUnsignedLong(names[record->user]);
                    if (PyErr_Occurred()) { failed = 1; break; }
                    codes[record->user] = (uint32_t)dense;
                }
            }
            if (compact) {
                prepared->records[prepared->length++] = (SessionEvent){record->ts,
                    Py_NewRef(record->price ? PyList_GET_ITEM(categories, record->category) : Py_None),
                    codes[record->user], record->price};
                continue;
            }
            if (store_allocate_record(store) < 0) { failed = 1; break; }
            EventRecord *output = store_record(store, store->length++);
            *output = (EventRecord){record->ingested, record->ts, Py_NewRef(names[record->user]),
                                    Py_NewRef(record->price ? PyList_GET_ITEM(categories, record->category) : Py_None), record->price};
        }
        for (size_t u = 0; u < shard->users.count; ++u) Py_XDECREF(names[u]);
        free(names); free(codes);
        if (failed) goto fail;
        shard_free(shard);
    }
    if (profile) transfer_time = event_clock() - transfer_start;
    Py_CLEAR(store->index);
    byte_index_free(&product_index); byte_index_free(&user_index);
    Py_DECREF(categories); Py_DECREF(canonical);
    if (profile) {
        double total = event_clock() - began;
        fprintf(stderr, "PROFILE events parse=%.6f scatter=%.6f dedup=%.6f transfer=%.6f other=%.6f total=%.6f\n",
                parsed_time, scatter_time, dedup_time, transfer_time,
                total - parsed_time - scatter_time - dedup_time - transfer_time, total);
    }
    return (PyObject *)store;
fail:
    parallel_parser_close(&parser); free(refs);
    for (unsigned s = 0; s < 8; ++s) shard_free(&shards[s]);
    byte_index_free(&product_index); byte_index_free(&user_index);
    Py_XDECREF(categories); Py_XDECREF(canonical); Py_XDECREF(store);
    return NULL;
}
