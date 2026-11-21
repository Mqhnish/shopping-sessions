/* Exact byte keys in an arena; no Python string/token per event ID.
 * A hash match always requires length and full memcmp equality. */
typedef struct { uint64_t hash, key_offset, record_plus_one; } ByteBucket;
typedef struct {
    ByteBucket *buckets;
    size_t capacity, count;
    char *arena;
    size_t used, allocated;
    int no_python_errors;
} ByteIndex;

static void byte_index_free(ByteIndex *index) {
    free(index->buckets); free(index->arena);
    memset(index, 0, sizeof(*index));
}

static uint64_t byte_hash(const char *key, size_t length, unsigned bits) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < length; ++i) {
        hash ^= (unsigned char)key[i]; hash *= 1099511628211ULL;
    }
    hash ^= hash >> 33; hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33; hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    /* Reduced hashes are only for explicit collision tests, never implicit. */
    return bits == 64 ? hash : hash & ((1ULL << bits) - 1);
}

static int byte_index_oom(ByteIndex *index) {
    if (!index->no_python_errors) PyErr_NoMemory();
    return -1;
}

static int byte_index_lookup(const ByteIndex *index, const char *key, size_t length, Py_ssize_t *record) {
    if (!index->capacity) return 0;
    uint64_t hash = byte_hash(key, length, 64);
    size_t slot = hash & (index->capacity - 1);
    while (index->buckets[slot].record_plus_one) {
        const ByteBucket *bucket = &index->buckets[slot];
        if (bucket->hash == hash) {
            uint64_t n;
            memcpy(&n, index->arena + bucket->key_offset, sizeof(n));
            if (n == length && !memcmp(index->arena + bucket->key_offset + sizeof(n), key, length)) {
                *record = (Py_ssize_t)(bucket->record_plus_one - 1); return 1;
            }
        }
        slot = (slot + 1) & (index->capacity - 1);
    }
    return 0;
}

static int byte_index_grow(ByteIndex *index) {
    if (index->capacity > SIZE_MAX / 2) { return byte_index_oom(index); }
    size_t capacity = index->capacity ? index->capacity * 2 : 1024;
    if (capacity > SIZE_MAX / sizeof(ByteBucket)) { return byte_index_oom(index); }
    ByteBucket *buckets = calloc(capacity, sizeof(ByteBucket));
    if (!buckets) { return byte_index_oom(index); }
    for (size_t i = 0; i < index->capacity; ++i) {
        ByteBucket bucket = index->buckets[i];
        if (!bucket.record_plus_one) continue;
        size_t slot = bucket.hash & (capacity - 1);
        while (buckets[slot].record_plus_one) slot = (slot + 1) & (capacity - 1);
        buckets[slot] = bucket;
    }
    free(index->buckets);
    index->buckets = buckets; index->capacity = capacity;
    return 0;
}

/* Return 1 for a new key, 0 for an existing key, -1 on allocation failure. */
static int byte_index_find(ByteIndex *index, const char *key, size_t length,
                           unsigned bits, Py_ssize_t next_record, Py_ssize_t *record) {
    if (!index->capacity && byte_index_grow(index) < 0) return -1;
    uint64_t hash = byte_hash(key, length, bits);
retry:
    ;
    size_t slot = hash & (index->capacity - 1);
    while (index->buckets[slot].record_plus_one) {
        ByteBucket *bucket = &index->buckets[slot];
        if (bucket->hash == hash) {
            uint64_t stored_length;
            memcpy(&stored_length, index->arena + bucket->key_offset, sizeof(stored_length));
            if (stored_length == length && !memcmp(index->arena + bucket->key_offset + sizeof(stored_length), key, length)) {
                *record = (Py_ssize_t)(bucket->record_plus_one - 1);
                return 0;
            }
        }
        slot = (slot + 1) & (index->capacity - 1);
    }
    if (index->count + 1 > index->capacity - index->capacity / 5) {
        if (byte_index_grow(index) < 0) return -1;
        goto retry;
    }
    if (length > SIZE_MAX - sizeof(uint64_t) || index->used > SIZE_MAX - sizeof(uint64_t) - length) {
        return byte_index_oom(index);
    }
    size_t needed = index->used + sizeof(uint64_t) + length;
    if (needed > index->allocated) {
        size_t allocated = index->allocated ? index->allocated : 65536;
        while (allocated < needed) {
            size_t growth = allocated / 2;
            if (allocated > SIZE_MAX - growth) { allocated = needed; break; }
            allocated += growth;
        }
        char *arena = realloc(index->arena, allocated);
        if (!arena) { return byte_index_oom(index); }
        index->arena = arena; index->allocated = allocated;
    }
    uint64_t stored_length = length;
    memcpy(index->arena + index->used, &stored_length, sizeof(stored_length));
    memcpy(index->arena + index->used + sizeof(stored_length), key, length);
    index->buckets[slot] = (ByteBucket){hash, index->used, (uint64_t)next_record + 1};
    index->used = needed; ++index->count;
    *record = next_record;
    return 1;
}

static int byte_store_consider(EventStore *store, ByteIndex *index, RawField identifier,
                               int64_t ingested, unsigned hash_bits, Py_ssize_t *offset) {
    int fresh = byte_index_find(index, identifier.data, (size_t)identifier.length,
                                hash_bits, store->length, offset);
    if (fresh < 0) return -1;
    if (fresh) {
        if (store_allocate_record(store) < 0) return -1;
        ++store->length;
    }
    EventRecord *record = store_record(store, *offset);
    if (!fresh && ingested >= record->ingested) return 0;
    record->ingested = ingested;
    Py_CLEAR(record->person); Py_CLEAR(record->category);
    return 1;
}
