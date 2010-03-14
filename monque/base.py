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
import uuid


class Monque(object):
    def __init__(self, mongodb, collection_prefix = "monque", default_queue = 'default_queue', max_retries = 5):
        self.mongodb = mongodb
        self._collection_prefix = collection_prefix
        self._job_configuration = dict(
            queue   = default_queue,
            retries = max_retries,
            delay   = datetime.timedelta()
        )
        self._initialized_queues = dict()
    
    def enqueue(self, job, **kwargs):
        job = job.__configure__(kwargs)
        c = self._get_job_collection(job)
        now = datetime.datetime.utcnow()

        c = self._get_queue_collection(job.queue)
        c.insert(dict(
            inserted_time   = now,
            scheduled_time  = now + job.delay,
            random_token    = _random_token(),
            queue           = job.queue,
            message         = job.__serialize__()
        ))

    def dequeue(self, queues):
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
            message = self.mongodb.command(pymongo.son.SON([
                ('findandmodify', c.name),
                ('query', dict(
                    scheduled_time  = {'$lte' : now},
                    random_token    = { random_operator : capture_token }
                )),
                ('sort', dict(random_token = order)),
                ('remove', True)
            ]))

            if message:
                return job.MonqueJob.__deserialize__(message)
    
    def _initialize_queue(coll):
        coll.ensure_index([
            ('scheduled_time',  pymongo.ASCENDING),
            ('random_token',    pymongo.ASCENDING)
        ])
        
        coll.ensure_index([
            ('scheduled_time',  pymongo.ASCENDING),
            ('random_token',    pymongo.DESCENDING)
        ])

    def get_collection(self, *args):
        return operator.getitem(self.mongodb, ':'.join([self._collection_prefix] + args))

    def get_queue_collection(self, queue):
        coll = self.get_collection('queues', queue)

        if not self._initialized_queues.has_key(coll.name):
            self._initialize_queue(coll)
            self._initialized_queues[coll.name] = True

        return coll

        
def _random_token():
    return base64.b64encode(uuid.uuid4().bytes, ('-', '_')).rstrip('=')        