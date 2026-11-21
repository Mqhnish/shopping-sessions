/* Click IDs remain exact bytes until the final tie sort. Workers own every
 * mutable index; only immutable campaign codes are shared. */
typedef struct { int64_t ts; size_t identifier; uint32_t user, campaign; } ShardClick;
typedef struct {
    ByteIndex seen, users;
    size_t *user_offsets;
    size_t user_capacity;
    ShardClick *records;
    size_t length, capacity;
    ParsedEvent **input;
    size_t input_length;
    const ByteIndex *campaigns;
    int error;
} ClickShard;
typedef struct {
    int64_t ts;
    const char *identifier;
    size_t length;
    uint32_t person, campaign;
} SortClick;

static void click_shard_free(ClickShard *shard) {
    byte_index_free(&shard->seen); byte_index_free(&shard->users);
    free(shard->user_offsets); free(shard->records);
    memset(shard, 0, sizeof(*shard));
}

static void *dedup_click_shard(void *opaque) {
    ClickShard *shard = opaque;
    for (size_t r = 0; r < shard->input_length; ++r) {
        ParsedEvent *input = shard->input[r];
        size_t identifier = shard->seen.used;
        Py_ssize_t ignored, campaign, user;
        /* First copy wins BEFORE timestamp, user or campaign validation. */
        int fresh = byte_index_find(&shard->seen, input->identifier.data, input->identifier.length, 64, 0, &ignored);
        if (fresh < 0) goto oom;
        if (!fresh || !input->valid) continue;
        if (!byte_index_lookup(shard->campaigns, input->product.data, input->product.length, &campaign)) continue;
        if (shard->users.count >= UINT32_MAX) { shard->error = 3; return NULL; }
        size_t offset = shard->users.used;
        int new_user = byte_index_find(&shard->users, input->user.data, input->user.length, 64,
                                       (Py_ssize_t)shard->users.count, &user);
        if (new_user < 0) goto oom;
        if (new_user) {
            if ((size_t)user == shard->user_capacity) {
                size_t capacity = shard->user_capacity ? shard->user_capacity + shard->user_capacity / 2 : 1024;
                if (capacity > SIZE_MAX / sizeof(size_t)) goto oom;
                size_t *grown = realloc(shard->user_offsets, capacity * sizeof(size_t));
                if (!grown) goto oom;
                shard->user_offsets = grown; shard->user_capacity = capacity;
            }
            shard->user_offsets[user] = offset;
        }
        if (shard->length == shard->capacity) {
            size_t capacity = shard->capacity ? shard->capacity + shard->capacity / 2 : 4096;
            if (capacity > SIZE_MAX / sizeof(ShardClick) || capacity > (size_t)PY_SSIZE_T_MAX) goto oom;
            ShardClick *grown = realloc(shard->records, capacity * sizeof(ShardClick));
            if (!grown) goto oom;
            shard->records = grown; shard->capacity = capacity;
        }
        shard->records[shard->length++] = (ShardClick){input->ts, identifier, (uint32_t)user, (uint32_t)campaign};
    }
    return NULL;
oom:
    shard->error = 1; return NULL;
}

static int raw_click_compare(const void *av, const void *bv) {
    const SortClick *a = av, *b = bv;
    if (a->person != b->person) return a->person < b->person ? -1 : 1;
    if (a->ts != b->ts) return a->ts < b->ts ? -1 : 1;
    size_t length = a->length < b->length ? a->length : b->length;
    int result = memcmp(a->identifier, b->identifier, length);
    if (result) return result > 0 ? -1 : 1;
    return (b->length > a->length) - (b->length < a->length);
}

static PyObject *load_byte_clicks(PyObject *args) {
    PyObject *paths, *campaigns, *people, *resolve;
    long long start, end;
    if (!PyArg_ParseTuple(args, "OLLO!O!O", &paths, &start, &end,
                         &PyDict_Type, &campaigns, &PyDict_Type, &people, &resolve)) return NULL;
    Py_ssize_t nc = PyDict_Size(campaigns), np = PyDict_Size(people);
    if (nc >= UINT32_MAX || np >= UINT32_MAX) return PyErr_Format(PyExc_OverflowError, "too many people or campaigns");
    unsigned count = parser_threads();
    ClickShard shards[8] = {0};
    ParallelParser parser = {0};
    ByteIndex campaign_index = {0}, user_index = {0};
    ParsedEvent **refs = NULL;
    size_t ref_capacity = 0;
    SortClick *sorted = NULL;
    size_t length = 0;
    ClickIndex *self = (ClickIndex *)ClickIndexType.tp_alloc(&ClickIndexType, 0);
    if (!self) return NULL;
    self->current = -1; self->npeople = np; self->ncampaigns = nc;
    self->campaigns = PyTuple_New(nc + 1);
    if (!self->campaigns) goto fail;
    PyObject *organic = PyUnicode_FromString("organic");
    if (!organic) goto fail;
    PyTuple_SET_ITEM(self->campaigns, nc, organic);
    Py_ssize_t pos = 0, code = 0;
    PyObject *key, *value;
    while (PyDict_Next(campaigns, &pos, &key, &value)) {
        Py_ssize_t size, ignored;
        const char *bytes = PyUnicode_AsUTF8AndSize(key, &size);
        if (!bytes) goto fail;
        if (byte_index_find(&campaign_index, bytes, size, 64, code, &ignored) < 0) goto fail;
        PyTuple_SET_ITEM(self->campaigns, code++, Py_NewRef(key));
    }
    self->offsets = calloc((size_t)np + 1, sizeof(Py_ssize_t));
    self->counts = calloc((size_t)nc + 1, sizeof(int64_t));
    self->active = calloc((size_t)nc + 1, sizeof(uint32_t));
    self->positions = calloc((size_t)nc + 1, sizeof(uint32_t));
    if (!self->offsets || !self->counts || !self->active || !self->positions) { PyErr_NoMemory(); goto fail; }
    if (parallel_parser_init(&parser, paths, start, end, count) < 0) goto fail;
    parser.hash_ids = parser.clicks = 1;
    for (unsigned s = 0; s < count; ++s) {
        shards[s].seen.no_python_errors = shards[s].users.no_python_errors = 1;
        shards[s].campaigns = &campaign_index;
    }
    int batch;
    while ((batch = parallel_parser_batch(&parser)) > 0) {
        size_t lengths[8] = {0}, offsets[8], positions[8], total = 0;
        for (unsigned j = 0; j < parser.jobs; ++j)
            for (size_t r = 0; r < parser.tasks[j].length; ++r) ++lengths[parser.tasks[j].records[r].hash % count];
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
        for (unsigned j = 0; j < parser.jobs; ++j)
            for (size_t r = 0; r < parser.tasks[j].length; ++r) {
                ParsedEvent *record = &parser.tasks[j].records[r];
                refs[positions[record->hash % count]++] = record;
            }
        pthread_t workers[8]; int started[8] = {0};
        for (unsigned s = 0; s < count; ++s) {
            shards[s].input = total ? refs + offsets[s] : NULL;
            shards[s].input_length = lengths[s];
        }
        Py_BEGIN_ALLOW_THREADS
        for (unsigned s = 0; s < count; ++s) {
            if (!shards[s].input_length) continue;
            if (s + 1 < count && !pthread_create(&workers[s], NULL, dedup_click_shard, &shards[s])) started[s] = 1;
            else dedup_click_shard(&shards[s]);
        }
        for (unsigned s = 0; s < count; ++s) if (started[s]) pthread_join(workers[s], NULL);
        Py_END_ALLOW_THREADS
        for (unsigned s = 0; s < count; ++s) {
            if (shards[s].error == 1) { PyErr_NoMemory(); goto fail; }
            if (shards[s].error == 3) { PyErr_SetString(PyExc_OverflowError, "too many click users"); goto fail; }
        }
    }
    if (batch < 0) goto fail;
    parallel_parser_close(&parser); free(refs); refs = NULL;
    size_t total = 0;
    for (unsigned s = 0; s < count; ++s) {
        if (shards[s].length > (size_t)PY_SSIZE_T_MAX - total) { PyErr_NoMemory(); goto fail; }
        total += shards[s].length;
    }
    if (total > SIZE_MAX / sizeof(SortClick)) { PyErr_NoMemory(); goto fail; }
    if (total) { sorted = malloc(total * sizeof(SortClick)); if (!sorted) { PyErr_NoMemory(); goto fail; } }
    for (unsigned s = 0; s < count; ++s) {
        ClickShard *shard = &shards[s];
        uint32_t *codes = NULL;
        if (shard->users.count) {
            if (shard->users.count > SIZE_MAX / sizeof(uint32_t)) { PyErr_NoMemory(); goto fail; }
            codes = malloc(shard->users.count * sizeof(uint32_t));
            if (!codes) { PyErr_NoMemory(); goto fail; }
        }
        int failed = 0;
        for (size_t u = 0; u < shard->users.count; ++u) {
            const char *raw = shard->users.arena + shard->user_offsets[u];
            uint64_t size; memcpy(&size, raw, sizeof(size)); raw += sizeof(size);
            Py_ssize_t person;
            if (!byte_index_lookup(&user_index, raw, size, &person)) {
                PyObject *user = PyUnicode_DecodeUTF8(raw, size, "strict");
                if (!user) { failed = 1; break; }
                PyObject *name = PyObject_CallOneArg(resolve, user);
                Py_DECREF(user);
                if (!name) { failed = 1; break; }
                PyObject *pc = PyDict_GetItemWithError(people, name);
                Py_DECREF(name);
                if (!pc && PyErr_Occurred()) { failed = 1; break; }
                person = pc ? PyLong_AsSsize_t(pc) : (Py_ssize_t)UINT32_MAX;
                if (PyErr_Occurred()) { failed = 1; break; }
                if (pc && (person < 0 || person >= np)) { PyErr_SetString(PyExc_ValueError, "invalid person code"); failed = 1; break; }
                Py_ssize_t ignored;
                if (byte_index_find(&user_index, raw, size, 64, person, &ignored) < 0) { failed = 1; break; }
            }
            codes[u] = (uint32_t)person;
        }
        if (!failed) for (size_t r = 0; r < shard->length; ++r) {
            ShardClick *record = &shard->records[r];
            uint32_t person = codes[record->user];
            if (person == UINT32_MAX) continue;
            const char *raw = shard->seen.arena + record->identifier;
            uint64_t size; memcpy(&size, raw, sizeof(size)); raw += sizeof(size);
            sorted[length++] = (SortClick){record->ts, raw, size, person, record->campaign};
        }
        free(codes);
        if (failed) goto fail;
        /* ID arenas remain stable until sorting; all other shard state is done. */
        byte_index_free(&shard->users); free(shard->user_offsets); shard->user_offsets = NULL;
        free(shard->records); shard->records = NULL;
        free(shard->seen.buckets); shard->seen.buckets = NULL;
    }
    byte_index_free(&user_index); byte_index_free(&campaign_index);
    Py_BEGIN_ALLOW_THREADS
    if (length > 1) qsort(sorted, length, sizeof(SortClick), raw_click_compare);
    Py_END_ALLOW_THREADS
    if (length > SIZE_MAX / sizeof(ClickRecord)) { PyErr_NoMemory(); goto fail; }
    if (length) {
        self->records = malloc(length * sizeof(ClickRecord));
        if (!self->records) { PyErr_NoMemory(); goto fail; }
    }
    for (size_t i = 0; i < length; ++i) {
        self->records[self->length++] = (ClickRecord){sorted[i].ts, NULL, sorted[i].person, sorted[i].campaign};
        ++self->offsets[sorted[i].person + 1];
    }
    self->capacity = self->length;
    free(sorted);
    for (unsigned s = 0; s < count; ++s) click_shard_free(&shards[s]);
    for (Py_ssize_t p = 1; p <= np; ++p) self->offsets[p] += self->offsets[p - 1];
    return (PyObject *)self;
fail:
    parallel_parser_close(&parser); free(refs); free(sorted);
    for (unsigned s = 0; s < 8; ++s) click_shard_free(&shards[s]);
    byte_index_free(&campaign_index); byte_index_free(&user_index); Py_DECREF(self);
    return NULL;
}
