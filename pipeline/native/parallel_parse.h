#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

/* Spans borrow the current read-only file mapping, never Python objects. */
typedef struct {
    RawField identifier, user, product;
    int64_t ingested, ts;
    uint64_t hash;
    int price, valid;
} ParsedEvent;
typedef struct {
    const char *begin, *end;
    int64_t start, finish;
    ParsedEvent *records;
    size_t length, capacity;
    int error, hash_ids, clicks, cache_dates;
    TimestampCache dates;
} ParseJob;
typedef struct {
    PyObject *paths;
    char *mapping;
    size_t size, offset, chunk;
    unsigned threads, jobs, current;
    size_t row;
    int64_t start, finish;
    int hash_ids, clicks;
    ParseJob tasks[8];
} ParallelParser;

static unsigned parser_threads(void) {
    const char *serial = getenv("SS_SERIAL_PARSE");
    const char *parallel = getenv("SS_PARALLEL_PARSE");
    if ((serial && !strcmp(serial, "1")) || (parallel && strcmp(parallel, "1"))) return 1;
    const char *value = getenv("NCPU");
    if (!value) return 8;
    char *end;
    long count = strtol(value, &end, 10);
    if (end == value || *end || count < 2) return 1;
    return count > 8 ? 8 : (unsigned)count;
}

/* Return -1 only for invalid ingest (a contract error). Invalid payloads are
 * retained as candidates so an earlier bad copy still poisons dedup. */
static int parse_candidate(RawField *fields, int64_t start, int64_t end, ParsedEvent *out, TimestampCache *cache) {
    *out = (ParsedEvent){.identifier = fields[0], .user = fields[1], .product = fields[5]};
    if (!timestamp_with_cache(fields[3].data, fields[3].length, &out->ingested, cache)) return -1;
    if (!timestamp_with_cache(fields[2].data, fields[2].length, &out->ts, cache) ||
        !fields[1].length || !fields[5].length || out->ts < start || out->ts >= end) return 0;
    int purchase = fields[4].length == 8 && !memcmp(fields[4].data, "purchase", 8);
    if (!purchase && !(fields[4].length == 4 && (!memcmp(fields[4].data, "view", 4) ||
                                                !memcmp(fields[4].data, "cart", 4)))) return 0;
    if (out->ingested > out->ts && (uint64_t)out->ingested - (uint64_t)out->ts > 86400000) return 0;
    if (purchase) {
        if (!fields[6].length) return 0;
        for (Py_ssize_t i = 0; i < fields[6].length; ++i) {
            char digit = fields[6].data[i];
            if (digit < '0' || digit > '9') return 0;
            out->price = out->price * 10 + digit - '0';
            if (out->price > 10000000) return 0;
        }
        if (!out->price) return 0;
    }
    out->valid = 1;
    return 0;
}

static void *parse_event_job(void *opaque) {
    ParseJob *job = opaque;
    const char *cursor = job->begin;
    job->length = 0; job->error = 0;
    while (cursor < job->end) {
        const char *lf = memchr(cursor, '\n', (size_t)(job->end - cursor));
        const char *end = lf ? lf : job->end;
        RawField fields[7];
        int valid = csv_fields(cursor, end - cursor, job->clicks ? 4 : 7, fields);
        cursor = lf ? lf + 1 : end;
        if (!valid || !fields[0].length) continue;
        ParsedEvent parsed;
        if (job->clicks) {
            parsed = (ParsedEvent){.identifier=fields[0], .user=fields[1], .product=fields[2]};
            parsed.valid = fields[1].length && timestamp_with_cache(fields[3].data, fields[3].length, &parsed.ts, job->cache_dates ? &job->dates : NULL)
                           && parsed.ts >= job->start && parsed.ts < job->finish;
        } else if (parse_candidate(fields, job->start, job->finish, &parsed, job->cache_dates ? &job->dates : NULL) < 0) { job->error = 2; return NULL; }
        if (job->hash_ids) parsed.hash = byte_hash(parsed.identifier.data, (size_t)parsed.identifier.length, 64);
        if (job->length == job->capacity) {
            size_t capacity = job->capacity ? job->capacity + job->capacity / 2 : 1024;
            if (capacity > SIZE_MAX / sizeof(ParsedEvent)) { job->error = 1; return NULL; }
            ParsedEvent *grown = realloc(job->records, capacity * sizeof(ParsedEvent));
            if (!grown) { job->error = 1; return NULL; }
            job->records = grown; job->capacity = capacity;
        }
        job->records[job->length++] = parsed;
    }
    return NULL;
}

static void parallel_parser_close(ParallelParser *parser) {
    if (parser->mapping) munmap(parser->mapping, parser->size);
    for (unsigned i = 0; i < 8; ++i) free(parser->tasks[i].records);
    Py_XDECREF(parser->paths);
    memset(parser, 0, sizeof(*parser));
}

static int parallel_parser_init(ParallelParser *parser, PyObject *paths, int64_t start, int64_t end, unsigned threads) {
    parser->paths = PyObject_GetIter(paths);
    if (!parser->paths) return -1;
    parser->threads = threads; parser->start = start; parser->finish = end;
    const char *cached = getenv("SS_CACHE_DATES");
    for (unsigned i = 0; i < 8; ++i) parser->tasks[i].cache_dates = cached && !strcmp(cached, "1");
    parser->chunk = 8 * 1024 * 1024;
    const char *setting = getenv("SS_PARSE_CHUNK_BYTES");
    if (setting) {
        char *tail;
        unsigned long size = strtoul(setting, &tail, 10);
        if (tail != setting && !*tail && size >= 65536 && size <= 8 * 1024 * 1024) parser->chunk = size;
    }
    return 0;
}

static int parallel_parser_open(ParallelParser *parser) {
    if (parser->mapping) { munmap(parser->mapping, parser->size); parser->mapping = NULL; }
    for (;;) {
        PyObject *path = PyIter_Next(parser->paths);
        if (!path) return PyErr_Occurred() ? -1 : 0;
        PyObject *bytes = NULL;
        if (!PyUnicode_FSConverter(path, &bytes)) { Py_DECREF(path); return -1; }
        int fd = open(PyBytes_AS_STRING(bytes), O_RDONLY);
        Py_DECREF(bytes);
        if (fd < 0) { PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path); Py_DECREF(path); return -1; }
        struct stat info;
        if (fstat(fd, &info) < 0) { PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path); close(fd); Py_DECREF(path); return -1; }
        if (info.st_size < 0 || (uint64_t)info.st_size > (uint64_t)PY_SSIZE_T_MAX) {
            PyErr_SetString(PyExc_OverflowError, "input file too large"); close(fd); Py_DECREF(path); return -1;
        }
        parser->size = (size_t)info.st_size;
        if (!parser->size) { close(fd); Py_DECREF(path); continue; }
        void *mapping = mmap(NULL, parser->size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapping == MAP_FAILED) { PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path); close(fd); Py_DECREF(path); return -1; }
        close(fd); Py_DECREF(path);
        parser->mapping = mapping;
        const char *header = memchr(mapping, '\n', parser->size);
        parser->offset = header ? (size_t)(header - parser->mapping) + 1 : parser->size;
        return 1;
    }
}

/* Deliver candidates in file/line order. Batches are fully joined before the
 * main thread touches records or performs any Python identity/metadata calls. */
static int parallel_parser_batch(ParallelParser *parser) {
    for (;;) {
        if (!parser->mapping || parser->offset == parser->size) {
            int opened = parallel_parser_open(parser);
            if (opened <= 0) return opened;
            if (parser->offset == parser->size) continue;
        }
        parser->jobs = parser->current = 0; parser->row = 0;
        size_t remaining_file = parser->size - parser->offset;
        size_t target = remaining_file / parser->threads + (remaining_file % parser->threads != 0);
        if (target > parser->chunk) target = parser->chunk;
        if (target < 65536) target = 65536;
        while (parser->jobs < parser->threads && parser->offset < parser->size) {
            size_t remaining = parser->size - parser->offset;
            size_t end = parser->offset + (remaining < target ? remaining : target);
            if (end < parser->size && parser->mapping[end - 1] != '\n') {
                const char *lf = memchr(parser->mapping + end, '\n', parser->size - end);
                end = lf ? (size_t)(lf - parser->mapping) + 1 : parser->size;
            }
            ParseJob *job = &parser->tasks[parser->jobs++];
            job->begin = parser->mapping + parser->offset; job->end = parser->mapping + end;
            job->start = parser->start; job->finish = parser->finish;
            job->hash_ids = parser->hash_ids;
            job->clicks = parser->clicks;
            parser->offset = end;
        }
        pthread_t workers[8];
        int started[8] = {0};
        Py_BEGIN_ALLOW_THREADS
        for (unsigned i = 0; i < parser->jobs; ++i) {
            if (i + 1 < parser->jobs && !pthread_create(&workers[i], NULL, parse_event_job, &parser->tasks[i])) started[i] = 1;
            else parse_event_job(&parser->tasks[i]);
        }
        for (unsigned i = 0; i < parser->jobs; ++i) if (started[i]) pthread_join(workers[i], NULL);
        Py_END_ALLOW_THREADS
        for (unsigned i = 0; i < parser->jobs; ++i) {
            if (parser->tasks[i].error == 1) { PyErr_NoMemory(); return -1; }
            if (parser->tasks[i].error == 2) {
                PyErr_SetString(PyExc_ValueError, "ingest_ts must be valid F1 per the input contract"); return -1;
            }
        }
        return 1;
    }
}

static int parallel_parser_next(ParallelParser *parser, ParsedEvent **output) {
    for (;;) {
        while (parser->current < parser->jobs) {
            ParseJob *job = &parser->tasks[parser->current];
            if (parser->row < job->length) { *output = &job->records[parser->row++]; return 1; }
            ++parser->current; parser->row = 0;
        }
        int result = parallel_parser_batch(parser);
        if (result <= 0) return result;
    }
}
