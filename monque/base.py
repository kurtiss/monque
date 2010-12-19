#!/usr/bin/env python
# encoding: utf-8
"""
base.py

Created by Kurtiss Hare on 2010-03-12.
"""

import base64
import datetime
import job
import pymongo
import pymongo.errors
import pymongo.son
import util
import uuid
import worker
from pymongo.objectid import ObjectId


class Monque(object):
    def __init__(self, mongodb, collection_prefix = "monque", default_queue = 'default_queue', max_retries = 5):
        self.mongodb = mongodb
        self._collection_prefix = collection_prefix
        self._initialized_queues = dict()
        self._workorder_defaults = dict(
            queue       = default_queue,
            retries     = max_retries,
            delay       = datetime.timedelta(0),
            failures    = [],
        )
    
    def clear(self, queues = None):
        if not queues:
            queues = (self._workorder_defaults['queue'],)
        
        for queue in queues:
            self.get_queue_collection(queue).remove()
    
    # low-level
    
    def push(self, queue, item, delay=0, retries=5):
        now = datetime.datetime.utcnow()
        c = self.get_queue_collection(queue)
        _id = c.insert(dict(
            inserted_time   = now,
            scheduled_time  = now + (delay if isinstance(delay, datetime.timedelta) else datetime.timedelta(seconds=delay)),
            random_token    = self._random_token(),
            retries         = retries,
            failures        = [],
            body            = item,
        ))
        return str(_id)
    
    def pop(self, queue, grabfor=None, ordered=True):
        c = self.get_queue_collection(queue)
        now = datetime.datetime.utcnow()
        
        result = None

        if grabfor:
            extra = [('update', {'$set': {"scheduled_time": now + datetime.timedelta(seconds=grabfor)}})]
        else:
            extra = [('remove', True)]
            
        query = dict(
            scheduled_time = {'$lte' : now},
            retries = {'$gt' : 0}
        )

        if not ordered:
            capture_token = self._random_token()
            directions = (('$gte', pymongo.ASCENDING), ('$lt', pymongo.DESCENDING))

            for operator, order in directions:
                query['random_token'] = { operator : capture_token }
                try:
                    result = self.mongodb.command(pymongo.son.SON([
                        ('findandmodify', c.name),
                        ('query', query),
                        ('sort', dict(random_token = order)),
                    ] + extra))
                except pymongo.errors.OperationFailure:
                    pass # No matching object found
                else:
                    break
        else:
            try:
                result = self.mongodb.command(pymongo.son.SON([
                    ('findandmodify', c.name),
                    ('query', query),
                    ('sort', dict(scheduled_time = pymongo.ASCENDING)),
                ] + extra))
            except pymongo.errors.OperationFailure:
                pass # No matching object found

        if not result:
            return None

        result = result['value']
        result['_id'] = str(result['_id'])
        return result

    def update(self, queue, job_id, delay=None, failure=None):
        spec = {}
        if delay is not None:
            now = datetime.datetime.utcnow()
            spec['$set'] = {"scheduled_time": now + datetime.timedelta(seconds=delay)}
        if failure:
            spec.update({
                "$push": {"failures": failure},
                "$inc": {"retries": -1},
            })

        if not spec:
            return

        c = self.get_queue_collection(queue)
        c.update({"_id": ObjectId(job_id)}, spec)

    def remove(self, queue, job_id):
        c = self.get_queue_collection(queue)
        c.remove({"_id": ObjectId(job_id)})

    # high-level

    def enqueue(self, work_order, **kwargs):
        work_order.__configure__(kwargs)
        work_order.__configure__(self._workorder_defaults)
        self.push(
            queue = work_order.queue,
            item = dict(
                cls = util.get_toplevel_attrname(work_order.job.__class__),
                message = work_order.job.__serialize__(),
            ),
            delay = work_order.delay,
            retries = work_order.retries)
        c = self.get_collection('queue_stats')

    def dequeue(self, queues=None, grabfor=None):
        if not queues:
            queues = (self._workorder_defaults['queue'],)
        
        for queue in queues:
            result = self._dequeue_from(queue, grabfor=grabfor)
            
            if result:
                return result
    
    def _dequeue_from(self, queue, grabfor=None):
        row = self.pop(queue, grabfor=grabfor)
        if row:
            JobCls = util.get_toplevel_attr(row['body']['cls'])
            j = JobCls.__deserialize__(row['body']['message'])
            
            work_order = job.MonqueWorkOrder(j)
            work_order.__configure__(dict(
                job_id      = row['_id'],
                queue       = queue,
                retries     = row['retries'],
                failures    = row['failures'],
                delay       = datetime.timedelta(0)
            ))
            
            return work_order

    #
    
    def _initialize_queue(self, coll):
        coll.ensure_index([
            ('scheduled_time',  pymongo.ASCENDING),
            ('random_token',    pymongo.ASCENDING)
        ])
    
    def get_collection(self, *args):
        return self.mongodb[':'.join([self._collection_prefix] + list(args))]
    
    def get_queue_collection(self, queue):
        coll = self.get_collection('queues', queue)
        
        if not self._initialized_queues.has_key(coll.name):
            self._initialize_queue(coll)
            self._initialized_queues[coll.name] = True
        
        return coll
    
    def new_worker(self, *args, **kwargs):
        kwargs.setdefault('queues', [self._workorder_defaults['queue']])
        return worker.MonqueWorker(self, *args, **kwargs)

    def _random_token(self):
        return base64.b64encode(uuid.uuid4().bytes, ('-', '_')).rstrip('=')
