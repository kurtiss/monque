#!/usr/bin/env python
# encoding: utf-8
"""
util.py

Created by Kurtiss Hare on 2010-03-12.
"""

_import_cache = dict()
_setprocname = None


def get_toplevel_attrname(obj):
    return "{0.__module__}.{0.__name__}".format(obj)

def get_toplevel_attr(name):
    global _import_cache

    if not _import_cache.has_key(name):
        mod_name, attr_name = name.rsplit('.', 1)
        mod = __import__(str(mod_name), {}, {}, [str(attr_name)])
        attr = getattr(mod, attr_name)
        _import_cache[name] = attr

    return _import_cache[name]

def setprocname(name):
    global _setprocname

    if _setprocname is None:
        try:
            import procname
            _setprocname = procname.setprocname
        except ImportError:
            try:
                from ctypes import byref, cdll, create_string_buffer
                libc = cdll.LoadLibrary('libc.so.6')

                def _setprocname(name):
                    buff = create_string_buffer(len(name) + 1)
                    buff.value = name
                    libc.prctl(15, byref(buff), 0, 0, 0)
            except (ImportError, OSError):
                def _setprocname(name):
                    pass

    return _setprocname(name)