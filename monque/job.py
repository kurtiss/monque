#!/usr/bin/env python
# encoding: utf-8
"""
job.py

Created by Kurtiss Hare on 2010-03-12.
"""

import datetime
import functools
import pymongo
import types
import util


class MonqueJob(object):
    @classmethod
    def job_decorator(cls, **kwargs):
        def decorator(undecorated):
            return MonqueJobDecoration(cls, undecorated, kwargs)
        return decorator
        
    @classmethod
    def __deserialize__(cls, message):
        func = util.get_toplevel_attr(message['func'])

        if isinstance(func, MonqueJobDecoration):
            func = func.undecorated
        
        return cls(func, message['args'], message['kwargs'], id = message['id'])
    
    def __init__(self, func, func_args, func_kwargs, id = None):
        self._func = func
        self._func_args = func_args
        self._func_kwargs = func_kwargs
        self.id = id or pymongo.objectid.ObjectId()

        super(MonqueJob, self).__init__()

    def __configure__(self, kwargs):
        pass

    def __serialize__(self):
        return dict(
            id          = self.id,
            func        = util.get_toplevel_attrname(self._func),
            args        = self._func_args,
            kwargs      = self._func_kwargs,
        )

    def run(self):
        kwargs = dict((str(k), v) for (k,v) in self._func_kwargs.items())
        return self._func(*self._func_args, **kwargs)
    
    def __eq__(self, other):
        return (
            type(self) == type(other) and
            util.get_toplevel_attrname(self._func) == util.get_toplevel_attrname(other._func) and
            tuple(self._func_args) == tuple(other._func_args) and
            set((str(k), v) for (k,v) in self._func_kwargs.items()) == set((str(k), v) for (k,v) in other._func_kwargs.items())
        )
        
    def __ne__(self, other):
        return not (self == other)


class MonqueJobDecoration(object):
    def __init__(self, job_cls, undecorated, configuration):
        self.job_cls = job_cls
        self.undecorated = undecorated
        self.configuration = configuration
    
    def __call__(self, *args, **kwargs):
        j = self.job_cls(self.undecorated, args, kwargs)
        work_order = MonqueWorkOrder(j)
        work_order.__configure__(self.configuration)
        return work_order


class MonqueWorkOrder(object):
    def __init__(self, job):
        self.job = job

    def fail(self, e):
        self.retries = self.retries - 1
        self.failures.append(str(e))

    def __configure__(self, values):
        configure_values = dict(values.items())

        self._config_set('job_id', configure_values)
        self._config_set('queue', configure_values)
        self._config_set('delay', configure_values)
        self._config_set('retries', configure_values)
        self._config_set('failures', configure_values)
        self.job.__configure__(configure_values)

    def _config_set(self, name, values):
        value = values.pop(name, None)

        if not hasattr(self, name) and value is not None:
            custom_setter = "_set_{0}".format(name)
            if hasattr(self, custom_setter):
                getattr(self, custom_setter)(value)
            else:
                setattr(self, name, value)
    
    def mark_start(self):
        self.startTime = datetime.datetime.now()
    
    def mark_completion(self):
        self.endTime = datetime.datetime.now()
        return self.endTime-self.startTime
    
    def _set_delay(self, delay):
        if isinstance(delay, types.IntType):
            delay = datetime.timedelta(seconds = delay)

        self.delay = delay
        
    def __eq__(self, other):
        return (
            type(self) == type(other) and
            self.job == other.job and
            getattr(self, 'queue', None) == getattr(other, 'queue', None) and
            getattr(self, 'delay', None) == getattr(other, 'delay', None) and
            getattr(self, 'retries', None) == getattr(other, 'retries', None) and
            getattr(self, 'failures', None) == getattr(other, 'failures', None)
        )
    
    def __ne__(self, other):
        return not (self == other)


job = MonqueJob.job_decorator