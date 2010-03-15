#!/usr/bin/env python
# encoding: utf-8
"""
base.py

Created by Kurtiss Hare on 2010-03-12.
"""

import base64
import datetime
import functools
import job
import operator
import pymongo
import pymongo.errors
import pymongo.son
import util
import uuid
import worker


class Monque(object):
    def __init__(self, mongodb, collection_prefix = "monque", default_queue = 'default_queue', max_retries = 5):
        self.mongodb = mongodb
        self._collection_prefix = collection_prefix
        self._initialized_queues = dict()
        self._workorder_defaults = dict(
            queue       = default_queue,
            retries     = max_retries,
            delay       = datetime.timedelta(0),
            failures    = []
        )
        
    def clear(self, queues = None):
        if not queues:
            queues = (self._workorder_defaults['queue'],)
        
        for queue in queues:
            self.get_queue_collection(queue).remove()
    
    def enqueue(self, work_order, **kwargs):
        work_order.__configure__(kwargs)
        work_order.__configure__(self._workorder_defaults)
        now = datetime.datetime.utcnow()

        c = self.get_queue_collection(work_order.queue)
        c.insert(dict(
            inserted_time   = now,
            scheduled_time  = now + work_order.delay,
            random_token    = _random_token(),
            retries         = work_order.retries,
            failures        = work_order.failures,
            job_cls         = util.get_toplevel_attrname(work_order.job.__class__),
            job_message     = work_order.job.__serialize__()
        ))

    def dequeue(self, queues = None):
        if not queues:
            queues = (self._workorder_defaults['queue'],)

        for queue in queues:
            result = self._dequeue_from(queue)

            if result:
                return result
    
    def _dequeue_from(self, queue):
        c = self.get_queue_collection(queue)
        now = datetime.datetime.utcnow()
        capture_token = _random_token()
        directions = (('$gte', pymongo.ASCENDING), ('$lt', pymongo.DESCENDING))

        for operator, order in directions:
            try:
                result = self.mongodb.command(pymongo.son.SON([
                    ('findandmodify', c.name),
                    ('query', dict(
                        scheduled_time  = {'$lte' : now},
                        random_token    = { operator : capture_token }
                    )),
                    ('sort', dict(random_token = order)),
                    ('remove', True)
                ]))
            except pymongo.errors.OperationFailure:
                pass
            else:
                row = result['value']
                JobCls = util.get_toplevel_attr(row['job_cls'])
                j = JobCls.__deserialize__(row['job_message'])

                work_order = job.MonqueWorkOrder(j)
                work_order.__configure__(dict(
                    queue       = queue,
                    retries     = row['retries'],
                    failures    = row['failures'],
                    delay       = datetime.timedelta(0)
                ))

                return work_order
    
    def _initialize_queue(self, coll):
        coll.ensure_index([
            ('scheduled_time',  pymongo.ASCENDING),
            ('random_token',    pymongo.ASCENDING)
        ])
        
        coll.ensure_index([
            ('scheduled_time',  pymongo.ASCENDING),
            ('random_token',    pymongo.DESCENDING)
        ])

    def get_collection(self, *args):
        return operator.getitem(self.mongodb, ':'.join([self._collection_prefix] + list(args)))

    def get_queue_collection(self, queue):
        coll = self.get_collection('queues', queue)

        if not self._initialized_queues.has_key(coll.name):
            self._initialize_queue(coll)
            self._initialized_queues[coll.name] = True

        return coll
        
    def new_worker(self, *args, **kwargs):
        kwargs.setdefault('queues', [self._workorder_defaults['queue']])
        return worker.MonqueWorker(self, *args, **kwargs)

        
def _random_token():
    return base64.b64encode(uuid.uuid4().bytes, ('-', '_')).rstrip('=')        