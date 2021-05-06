/*
posixshmem - A Python extension that provides shm_open() and shm_unlink()
*/

#include <Python.h>
#include <sys/mman.h> // for shm_open() and shm_unlink()
#include "clinic/posixshmem.c.h"

static int
_posixshmem_shm_open_impl(PyObject *module, PyObject *path, int flags,
                          int mode)
{
    int fd;
    int async_err = 0;
    const char *name = PyBytes_AsString(path);

    if (name == NULL) {
        return -1;
    }
    do {
        Py_BEGIN_ALLOW_THREADS
        fd = shm_open(name, flags, mode);
        Py_END_ALLOW_THREADS
    } while (fd < 0 && errno == EINTR && !(async_err = PyErr_CheckSignals()));

    if (fd < 0) {
        if (!async_err)
            PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path);
        return -1;
    }

    return fd;
}

static PyObject * _posixshmem_shm_unlink_impl(PyObject *module, PyObject *path)
{
    int rv;
    int async_err = 0;
    const char *name = PyBytes_AsString(path);

    if (name == NULL) {
        return NULL;
    }
    do {
        Py_BEGIN_ALLOW_THREADS
        rv = shm_unlink(name);
        Py_END_ALLOW_THREADS
    } while (rv < 0 && errno == EINTR && !(async_err = PyErr_CheckSignals()));

    if (rv < 0) {
        if (!async_err)
            PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path);
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyMethodDef module_methods[ ] = {
    _POSIXSHMEM_SHM_OPEN_METHODDEF
    _POSIXSHMEM_SHM_UNLINK_METHODDEF
    {NULL}
};

static struct PyModuleDef _posixshmemmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_posixshmem",
    .m_doc = "POSIX shared memory module",
    .m_size = 0,
    .m_methods = module_methods,
};

PyMODINIT_FUNC PyInit__posixshmem(void)
{
    return PyModuleDef_Init(&_posixshmemmodule);
}
