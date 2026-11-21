#include <pthread.h>

typedef struct { SessionEvent *records; size_t length; } SortJob;

static void *sort_session_chunk(void *opaque) {
    SortJob *job = opaque;
    qsort(job->records, job->length, sizeof(SessionEvent), session_compare);
    return NULL;
}

static unsigned session_sort_threads(size_t length) {
    const char *parallel = getenv("SS_PARALLEL_SORT");
    const char *serial = getenv("SS_SERIAL_SORT");
    if (!parallel || strcmp(parallel, "1") || length < 100000 || (serial && !strcmp(serial, "1"))) return 1;
    const char *setting = getenv("NCPU");
    if (!setting) return 8;
    char *end;
    long count = strtol(setting, &end, 10);
    if (end == setting || *end || count < 2) return 1;
    return count > 8 ? 8 : (unsigned)count;
}

/* This unpublished array owns references, but comparisons/merges only move
 * pointer bits and compare numeric fields. No Python API runs without the GIL. */
static void sort_session_records(SessionEvent **records, size_t length) {
    unsigned threads = session_sort_threads(length);
    SessionEvent *scratch = NULL;
    if (threads > 1 && length <= SIZE_MAX / sizeof(SessionEvent))
        scratch = malloc(length * sizeof(SessionEvent));
    Py_BEGIN_ALLOW_THREADS
    if (!scratch) {
        if (length > 1) qsort(*records, length, sizeof(SessionEvent), session_compare);
    } else {
        SortJob jobs[8];
        pthread_t workers[8];
        int started[8] = {0};
        size_t chunk = length / threads + (length % threads != 0);
        for (unsigned i = 0; i < threads; ++i) {
            size_t begin = (size_t)i * chunk;
            if (begin > length) begin = length;
            size_t count = length - begin < chunk ? length - begin : chunk;
            jobs[i] = (SortJob){*records + begin, count};
            if (i + 1 < threads && !pthread_create(&workers[i], NULL, sort_session_chunk, &jobs[i])) {
                started[i] = 1;
            } else {
                /* Thread-creation failure keeps exactness via synchronous work. */
                sort_session_chunk(&jobs[i]);
            }
        }
        for (unsigned i = 0; i < threads; ++i)
            if (started[i]) pthread_join(workers[i], NULL);
        SessionEvent *source = *records, *target = scratch;
        for (size_t width = chunk; width < length; width *= 2) {
            for (size_t begin = 0; begin < length; begin += 2 * width) {
                size_t middle = begin + width < length ? begin + width : length;
                size_t limit = begin + 2 * width < length ? begin + 2 * width : length;
                size_t a = begin, b = middle, output = begin;
                while (a < middle && b < limit) {
                    if (session_compare(&source[a], &source[b]) <= 0) target[output++] = source[a++];
                    else target[output++] = source[b++];
                }
                while (a < middle) target[output++] = source[a++];
                while (b < limit) target[output++] = source[b++];
            }
            SessionEvent *swap = source; source = target; target = swap;
        }
        if (source != *records) {
            free(*records); *records = source;
        } else {
            free(scratch);
        }
    }
    Py_END_ALLOW_THREADS
}
