typedef struct { int64_t sessions, purchasing, revenue, assists; } DailyCampaign;
typedef struct { int64_t day; DailyCampaign *campaigns; } CampaignDay;

static int append_user_day(PyObject *rows, PyObject *person, int64_t day, int64_t sessions,
                            int64_t events, int64_t revenue, PyObject *categories) {
    PyObject *top = Py_None;
    int64_t highest = -1;
    if (categories) {
        Py_ssize_t position = 0;
        PyObject *key, *value;
        while (PyDict_Next(categories, &position, &key, &value)) {
            int64_t amount = PyLong_AsLongLong(value);
            if (PyErr_Occurred()) return -1;
            if (amount > highest || (amount == highest && PyUnicode_Compare(key, top) < 0)) {
                highest = amount; top = key;
            }
            if (PyErr_Occurred()) return -1;
        }
    }
    PyObject *row = Py_BuildValue("OLLLLO", person, (long long)day, (long long)sessions,
                                  (long long)events, (long long)revenue, top);
    if (!row) return -1;
    int rc = PyList_Append(rows, row);
    Py_DECREF(row);
    return rc;
}

static void aggregate_consume_sessions(Sessions *sessions) {
    for (Py_ssize_t i = 0; i < sessions->length; ++i) Py_CLEAR(sessions->records[i].category);
    free(sessions->records); sessions->records = NULL;
    sessions->length = sessions->position = 0;
}

static PyObject *aggregate_daily(PyObject *module, PyObject *args) {
    PyObject *events_obj, *clicks_obj, *channels;
    if (!PyArg_ParseTuple(args, "OOO!", &events_obj, &clicks_obj, &PyDict_Type, &channels)) return NULL;
    if (!PyObject_TypeCheck(events_obj, &SessionsType) || !PyObject_TypeCheck(clicks_obj, &ClickIndexType)) Py_RETURN_NONE;
    Sessions *sessions = (Sessions *)events_obj;
    ClickIndex *clicks = (ClickIndex *)clicks_obj;
    if (sessions->position || clicks->current != -1)
        return PyErr_Format(PyExc_RuntimeError, "aggregation requires fresh sessions and clicks");
    if (clicks->npeople != PyList_GET_SIZE(sessions->names))
        return PyErr_Format(PyExc_ValueError, "session/click person mappings differ");

    PyObject *users = PyList_New(0), *campaign_rows = PyList_New(0), *categories = NULL;
    ByteIndex day_index = {0};
    CampaignDay *days = NULL;
    Py_ssize_t ndays = 0, day_capacity = 0;
    int64_t user_day = 0, user_sessions = 0, user_events = 0, user_revenue = 0;
    uint32_t user_person = 0;
    int have_user = 0;
    if (!users || !campaign_rows) goto fail;
    for (Py_ssize_t first = 0; first < sessions->length;) {
        SessionEvent *record = &sessions->records[first];
        int64_t day = record->ts / 86400000LL;
        if (record->ts % 86400000LL < 0) --day;
        if (!have_user || record->person != user_person || day != user_day) {
            if (have_user && append_user_day(users, PyList_GET_ITEM(sessions->names, user_person),
                                             user_day, user_sessions, user_events, user_revenue, categories) < 0) goto fail;
            Py_CLEAR(categories);
            user_day = day; user_person = record->person;
            user_sessions = user_events = user_revenue = 0; have_user = 1;
        }
        Py_ssize_t limit = first + 1;
        while (limit < sessions->length && sessions->records[limit].person == record->person &&
               (uint64_t)sessions->records[limit].ts - (uint64_t)sessions->records[limit - 1].ts <= 1800000) ++limit;
        int64_t revenue = 0;
        for (Py_ssize_t i = first; i < limit; ++i) {
            SessionEvent *event = &sessions->records[i];
            if (event->price) {
                if (event->price < 0 || revenue > INT64_MAX - event->price) {
                    PyErr_SetString(PyExc_OverflowError, "session revenue outside int64"); goto fail;
                }
                if (!event->category || !PyUnicode_Check(event->category)) {
                    PyErr_SetString(PyExc_TypeError, "purchase category must be str"); goto fail;
                }
                revenue += event->price;
                if (!categories) { categories = PyDict_New(); if (!categories) goto fail; }
                PyObject *previous = PyDict_GetItemWithError(categories, event->category);
                if (!previous && PyErr_Occurred()) goto fail;
                int64_t amount = previous ? PyLong_AsLongLong(previous) : 0;
                if (PyErr_Occurred()) goto fail;
                if (amount > INT64_MAX - event->price) { PyErr_SetString(PyExc_OverflowError, "category revenue outside int64"); goto fail; }
                PyObject *total = PyLong_FromLongLong(amount + event->price);
                if (!total) goto fail;
                int rc = PyDict_SetItem(categories, event->category, total);
                Py_DECREF(total);
                if (rc < 0) goto fail;
            }
        }
        if (user_revenue > INT64_MAX - revenue) { PyErr_SetString(PyExc_OverflowError, "user revenue outside int64"); goto fail; }
        user_revenue += revenue; ++user_sessions; user_events += limit - first;
        if (clicks_advance(clicks, record->person, record->ts) < 0) goto fail;
        Py_ssize_t day_code;
        int fresh = byte_index_find(&day_index, (const char *)&day, sizeof(day), 64, ndays, &day_code);
        if (fresh < 0) goto fail;
        if (fresh) {
            if (ndays == day_capacity) {
                size_t capacity = day_capacity ? (size_t)day_capacity * 2 : 16;
                if (capacity > SIZE_MAX / sizeof(CampaignDay) || capacity > (size_t)PY_SSIZE_T_MAX) { PyErr_NoMemory(); goto fail; }
                CampaignDay *grown = realloc(days, capacity * sizeof(CampaignDay));
                if (!grown) { PyErr_NoMemory(); goto fail; }
                days = grown; day_capacity = (Py_ssize_t)capacity;
            }
            size_t count = (size_t)clicks->ncampaigns + 1;
            if (count > SIZE_MAX / sizeof(DailyCampaign)) { PyErr_NoMemory(); goto fail; }
            DailyCampaign *totals = calloc(count, sizeof(DailyCampaign));
            if (!totals) { PyErr_NoMemory(); goto fail; }
            days[ndays++] = (CampaignDay){day, totals};
        }
        Py_ssize_t credited = clicks->left < clicks->right ? clicks->records[clicks->right - 1].campaign : clicks->ncampaigns;
        DailyCampaign *total = &days[day_code].campaigns[credited];
        if (total->revenue > INT64_MAX - revenue) { PyErr_SetString(PyExc_OverflowError, "campaign revenue outside int64"); goto fail; }
        ++total->sessions; total->purchasing += revenue > 0; total->revenue += revenue;
        for (Py_ssize_t i = 0; i < clicks->nactive; ++i) ++days[day_code].campaigns[clicks->active[i]].assists;
        for (Py_ssize_t i = first; i < limit; ++i) Py_CLEAR(sessions->records[i].category);
        sessions->position = limit;
        first = limit;
    }
    if (have_user && append_user_day(users, PyList_GET_ITEM(sessions->names, user_person), user_day,
                                     user_sessions, user_events, user_revenue, categories) < 0) goto fail;
    Py_CLEAR(categories);
    aggregate_consume_sessions(sessions);
    for (Py_ssize_t d = 0; d < ndays; ++d) {
        for (Py_ssize_t c = 0; c <= clicks->ncampaigns; ++c) {
            DailyCampaign total = days[d].campaigns[c];
            if (!total.sessions && !total.assists) continue;
            PyObject *name = PyTuple_GET_ITEM(clicks->campaigns, c);
            PyObject *channel = c == clicks->ncampaigns ? name : PyDict_GetItemWithError(channels, name);
            if (!channel) { if (!PyErr_Occurred()) PyErr_SetObject(PyExc_KeyError, name); goto fail; }
            PyObject *row = Py_BuildValue("OOLLLLL", name, channel, (long long)days[d].day,
                                          (long long)total.sessions, (long long)total.purchasing,
                                          (long long)total.revenue, (long long)total.assists);
            if (!row) goto fail;
            int rc = PyList_Append(campaign_rows, row);
            Py_DECREF(row);
            if (rc < 0) goto fail;
        }
    }
    PyObject *result = PyTuple_Pack(2, users, campaign_rows);
    Py_DECREF(users); Py_DECREF(campaign_rows);
    for (Py_ssize_t d = 0; d < ndays; ++d) free(days[d].campaigns);
    free(days); byte_index_free(&day_index);
    return result;
fail:
    aggregate_consume_sessions(sessions);
    Py_XDECREF(categories); Py_XDECREF(users); Py_XDECREF(campaign_rows);
    for (Py_ssize_t d = 0; d < ndays; ++d) free(days[d].campaigns);
    free(days); byte_index_free(&day_index);
    return NULL;
}
