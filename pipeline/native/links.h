/* Packed union/find over exact Unicode IDs. A flat canonical mapping survives;
 * integer indices, ranks and lexical-min offsets are only build-time state. */
typedef struct { uint32_t parent, size, minimum; } LinkNode;
typedef struct {
    PyObject *index, *names;
    LinkNode *nodes;
    Py_ssize_t length, capacity;
} LinkBuild;

static int link_intern(LinkBuild *build, PyObject *name, uint32_t *output) {
    PyObject *code = PyDict_GetItemWithError(build->index, name);
    if (code) {
        unsigned long value = PyLong_AsUnsignedLong(code);
        if (PyErr_Occurred()) return -1;
        *output = (uint32_t)value;
        return 0;
    }
    if (PyErr_Occurred()) return -1;
    if ((uint64_t)build->length >= UINT32_MAX) {
        PyErr_SetString(PyExc_OverflowError, "too many linked IDs"); return -1;
    }
    if (build->length == build->capacity) {
        size_t next = build->capacity ? (size_t)build->capacity + (size_t)build->capacity / 2 : 1024;
        if (next > SIZE_MAX / sizeof(LinkNode) || next > (size_t)PY_SSIZE_T_MAX) { PyErr_NoMemory(); return -1; }
        LinkNode *grown = realloc(build->nodes, next * sizeof(LinkNode));
        if (!grown) { PyErr_NoMemory(); return -1; }
        build->nodes = grown; build->capacity = (Py_ssize_t)next;
    }
    uint32_t offset = (uint32_t)build->length;
    code = PyLong_FromUnsignedLong(offset);
    if (!code) return -1;
    int rc = PyDict_SetItem(build->index, name, code);
    Py_DECREF(code);
    if (rc < 0 || PyList_Append(build->names, name) < 0) return -1;
    build->nodes[build->length++] = (LinkNode){offset, 1, offset};
    *output = offset;
    return 0;
}

static uint32_t link_root(LinkBuild *build, uint32_t node) {
    uint32_t root = node;
    while (build->nodes[root].parent != root) root = build->nodes[root].parent;
    while (node != root) {
        uint32_t parent = build->nodes[node].parent;
        build->nodes[node].parent = root;
        node = parent;
    }
    return root;
}

static PyObject *load_links(PyObject *module, PyObject *paths) {
    LinkBuild build = {0};
    PyObject *iterator = NULL, *a = NULL, *b = NULL;
    build.index = PyDict_New(); build.names = PyList_New(0);
    if (!build.index || !build.names) goto fail;
    PyObject *args = Py_BuildValue("Oi", paths, 2);
    if (!args) goto fail;
    iterator = make_rows(NULL, args);
    Py_DECREF(args);
    if (!iterator) goto fail;
    int next;
    while ((next = rows_raw((Rows *)iterator)) > 0) {
        RawField *fields = ((Rows *)iterator)->fields;
        if (!fields[0].length || !fields[1].length) continue;
        a = PyUnicode_DecodeUTF8(fields[0].data, fields[0].length, "strict");
        b = PyUnicode_DecodeUTF8(fields[1].data, fields[1].length, "strict");
        if (!a || !b) goto fail;
        uint32_t ai, bi;
        if (link_intern(&build, a, &ai) < 0 || link_intern(&build, b, &bi) < 0) goto fail;
        uint32_t ar = link_root(&build, ai), br = link_root(&build, bi);
        if (ar != br) {
            if (build.nodes[ar].size < build.nodes[br].size) { uint32_t swap = ar; ar = br; br = swap; }
            int compare = PyUnicode_Compare(PyList_GET_ITEM(build.names, build.nodes[ar].minimum),
                                           PyList_GET_ITEM(build.names, build.nodes[br].minimum));
            if (PyErr_Occurred()) goto fail;
            build.nodes[br].parent = ar;
            build.nodes[ar].size += build.nodes[br].size;
            if (compare > 0) build.nodes[ar].minimum = build.nodes[br].minimum;
        }
        Py_CLEAR(a); Py_CLEAR(b);
    }
    if (next < 0) goto fail;
    /* Only values change during PyDict_Next: exact keys/cardinality are stable. */
    Py_ssize_t position = 0;
    PyObject *key, *value;
    while (PyDict_Next(build.index, &position, &key, &value)) {
        unsigned long code = PyLong_AsUnsignedLong(value);
        if (PyErr_Occurred()) goto fail;
        uint32_t root = link_root(&build, (uint32_t)code);
        PyObject *name = PyList_GET_ITEM(build.names, build.nodes[root].minimum);
        if (PyDict_SetItem(build.index, key, name) < 0) goto fail;
    }
    Py_DECREF(iterator); Py_DECREF(build.names); free(build.nodes);
    return build.index;
fail:
    Py_XDECREF(a); Py_XDECREF(b); Py_XDECREF(iterator);
    Py_XDECREF(build.index); Py_XDECREF(build.names); free(build.nodes);
    return NULL;
}
