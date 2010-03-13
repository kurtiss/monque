#!/usr/bin/env python
# encoding: utf-8
"""
base.py

Created by Kurtiss Hare on 2010-03-12.
"""

import datetime
import functools
import job
import operator
import pymongo
import pymongo.errors
import pymongo.son


class Monque(object):
	def __init__(self, mongodb, collection_prefix = "monque", default_queue = 'default_q', max_retries = 5):
		self.mongodb = mongodb
		self._collection_prefix = collection_prefix
		self._job_configuration = dict(
			queue	= default_queue,
			retries	= max_retries,
			delay	= datetime.timedelta()
		)
	
	def enqueue(self, job, **kwargs):
		job = job.__configure__(kwargs)
		c = self._get_job_collection(job)
		now = datetime.datetime.utcnow()

		if job.delay:
			c = self._get_delayed_queue_collection(job.queue)
			c.insert(dict(
				inserted_time	= now,
				scheduled_time	= now + job.delay,
				queue			= job.queue,
				job				= job.__serialize__()
			))
		else:
			c = self.get_queue_collection(job.queue)
			c.insert(dict(
				inserted_time	= now,
				queue			= job.queue,
				job				= job.__serialize__()
			))

	def dequeue(self, queues):
		for queue in queues:
			job = self._dequeue_from(queue)

			if job:
				return job
	
	def _dequeue_from(self, queue):
		c = self.get_queue_collection(queue)
		dc = self._get_delayed_queue_collection(queue)
		now = datetime.datetime.utcnow()
		result = None

		delayed_messages = dc.find(dict(
			scheduled_time = { '$lte' : now }
		), limit = 10).sort('scheduled_time', pymongo.ASCENDING)
		
		for delayed_message in delayed_messages:
			try:
				dc.remove(delayed_job._id, safe = True)
			except pymongo.errors(pymongo.errors.OperationFailure):
				pass
			else:
				delayed_message.pop('_id', None)
				delayed_message.pop('scheduled_time', None)
				c.insert(delayed_message)
		
		message = self.mongodb.command(pymongo.son.SON([
			('findandmodify', c.name),
			('remove', True)
		]))

		if message:
			result = job.MonqueJob.__deserialize__(message)
		
		return result

	def get_collection(self, *args):
		return operator.getitem(self.mongodb, ':'.join([self._collection_prefix] + args))

	def get_queue_collection(self, queue):
		return self.get_collection('queues', queue)

	def _get_delayed_queue_collection(self, queue):
		return self.get_collection('queues', queue, 'delayed')