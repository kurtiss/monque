#!/usr/bin/env python
# encoding: utf-8
"""
job.py

Created by Kurtiss Hare on 2010-03-12.
Copyright (c) 2010 Medium Entertainment, Inc. All rights reserved.
"""


class MonqueJob(object):
    @classmethod
    def __decorator__(cls, **kwargs):
        def decorator(undecorated):
            @functools.wraps(undecorated)
            def decorated(*func_args, **func_kwargs):
                job = cls(undecorated, func_args, func_kwargs)
                job.__configure__(kwargs)
                return job
            return decorated
        return decorator
        
    @classmethod
    def __deserialize__(cls, message):
        job_cls_path = message.get('cls')
        
        if job_cls_path:
            job_cls = util.get_toplevel_attr(job_cls_path)
            
            if job_cls != cls:
                return job_cls.__deserialize__(message)
        
        func = util.get_toplevel_attr(message['func'])

        return cls(func, message['args'], message['kwargs'], 
            id = message['id'], 
            failures    = message['failures'],
            retries     = message['retries']
        )

    def __init__(self, func, func_args, func_kwargs, id = None, failures = None, retries = None):
        self._func = func
        self._func_args = func_args
        self._func_kwargs = func_kwargs
        self._configuration = dict()
        self.id = id or pymongo.objectid.ObjectId()
        self._failures = failures or []
        self._configuration['retries'] = retries

        super(MonqueJob, self).__init__(func, func_args, func_kwargs)

    def __configure__(self, **kwargs):
        self._configuration.setdefault('queue', kwargs.get('queue', None))
        self._configuration.setdefault('retries', kwargs.get('retries', None))
        self._configuration.setdefault('delay', kwargs.get('delay', None))

        return self

    def __serialize__(self):
        serialized = dict(
            id          = self.id,
            func        = util.get_toplevel_attrname(self._func),
            args        = self._func_args,
            kwargs      = self._func_kwargs,
            retries     = self._configuration['retries'],
            failures    = self._failures,
        )

        if self.__class__ != MonqueJob:
            serialized['cls'] = util.get_toplevel_attrname(self.__class__)

        return serialized
        
    def fail(self, e):
        self.retries -= 1
        self.failures.append(str(e))
        
    @property
    def queue(self):
        return self._configuration['queue']
        
    @property
    def retries(self):
        return self._configuration['retries']
        
    @property
    def delay(self):
        return self._configuration['delay']
        
    def run(self):
        kwargs = dict((str(k), v) for (k,v) in self._func_kwargs.items())
        return self.func(*self._func_args, **kwargs)
        
job = MonqueJob.__decorator__