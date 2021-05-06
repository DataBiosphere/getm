PyDoc_STRVAR(_posixshmem_shm_open__doc__,
"shm_open($module, /, path, flags, mode=511)\n"
"--\n"
"\n"
"Open a shared memory object.  Returns a file descriptor (integer).");

#define _POSIXSHMEM_SHM_OPEN_METHODDEF    \
    {"shm_open", (PyCFunction)_posixshmem_shm_open, METH_FASTCALL|METH_KEYWORDS, _posixshmem_shm_open__doc__},

static int
_posixshmem_shm_open_impl(PyObject *module, PyObject *path, int flags, int mode);

static PyObject *
_posixshmem_shm_open(PyObject *module, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *return_value = NULL;
    static const char * const _keywords[] = {"path", "flags", "mode", NULL};
    static _PyArg_Parser _parser = {"O&i|i:shm_open", _keywords, 0};
	PyObject *path = NULL;
    int flags;
    int mode = 511;
    int _return_value;

    if (!_PyArg_ParseStackAndKeywords(args, nargs, kwnames, &_parser,
        PyUnicode_FSConverter, &path, &flags, &mode)) {
        goto exit;
    }

    _return_value = _posixshmem_shm_open_impl(module, path, flags, mode);
    if ((_return_value == -1) && PyErr_Occurred()) {
        goto exit;
    }
    return_value = PyLong_FromLong((long)_return_value);

exit:
    Py_XDECREF(path);
    return return_value;
}

PyDoc_STRVAR(_posixshmem_shm_unlink__doc__,
"shm_unlink($module, /, path)\n"
"--\n"
"\n"
"Remove a shared memory object (similar to unlink()).\n"
"\n"
"Remove a shared memory object name, and, once all processes  have  unmapped\n"
"the object, de-allocates and destroys the contents of the associated memory\n"
"region.");

#define _POSIXSHMEM_SHM_UNLINK_METHODDEF    \
    {"shm_unlink", (PyCFunction)_posixshmem_shm_unlink, METH_FASTCALL|METH_KEYWORDS, _posixshmem_shm_unlink__doc__},

static PyObject *
_posixshmem_shm_unlink_impl(PyObject *module, PyObject *path);

static PyObject *
_posixshmem_shm_unlink(PyObject *module, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
{
    PyObject *return_value = NULL;
    static const char * const _keywords[] = {"path", NULL};
    static _PyArg_Parser _parser = {"O&:shm_unlink", _keywords, 0};
	PyObject *path = NULL;

    if (!_PyArg_ParseStackAndKeywords(args, nargs, kwnames, &_parser,
		PyUnicode_FSConverter, &path)) {
        goto exit;
    }

    return_value = _posixshmem_shm_unlink_impl(module, path);

exit:
    Py_XDECREF(path);
    return return_value;
}
