#!/usr/bin/env python
# encoding: utf-8
"""
worker.py

Created by Kurtiss Hare on 2010-03-12.
"""

import logging
import multiprocessing
import pid
import pymongo.objectid
import signal
import socket
import time
import util


class MonqueWorker(object):
	def __init__(self, monque, queues):
		self._monque = monque
		self._queues = queues
		self._worker_id = None
		self._child = None
		self._shutdown_status = None
	
	def work(self, interval=5):
		self.register_worker()
		self._register_signal_handlers()
		
		util.setprocname("monque: Starting")
		
		try:
			while not self._shutdown_status:
				worked = self._work_once()

				if interval == 0:
					break

				if not worked:
					util.setprocname("monque: Waiting on queues: {0}".format(','.join(self._queues)))
					time.sleep(interval)
		finally:
			self.unregister_worker()
			
	def _work_once(self):
		job = self._monque.dequeue(self._queues)

		if not job:
			return False

		if job:
			try:
				self.working_on(job)
				self.process(job)
			except Exception, e:
				self._handle_job_failure(job, e)
			else:
				self.done_working()

		return True
		
	def working_on(self, job):
		c = self._monque.get_collection('workers')

		c.update(dict(_id = self._worker_id), {
			'$set' : dict(
				start_time	= datetime.datetime.utcnow(),
				job			= job.__serialize__(),
			)
		})

	def process(job):
		child = self.child = multiprocessing.Process(target=self._process_target, args=(job,))
		self.child.start()

		util.setprocname("monque: Forked {0} at {1}".format(self.child.pid, time.time()))

		while True:
			try:
				self.child.join()
			except OSError, e:
				if 'Interrupted system call' not in e:
					raise
				continue
			break

		self.child = None

		if child.exitcode != 0:
			raise Exception("Job failed with exit code {0}".format(child.exitcode))
			
	def done_working(self):
		self.processed()
		c = self._monque.get_collection('workers')
		c.remove(dict(_id = self._worker_id))
	
	def _process_target(self, job):
		self.reset_signal_handlers()
		util.setprocname("monque: Processing {0} since {1}".format(job.queue, time.time()))
		job.run()
		
	def _handle_job_failure(self, job, e):
		import traceback
		logging.warn("Job failed ({0}): {1}\n{2}".format(job, str(e), traceback.format_exc()))

		if job.retries > 0:
			job.fail(e)
			self._monque.enqueue(job)

			wc = self._monque.get_collection('workers')
			wc.update(dict(_id = self._worker_id), {'$inc' : dict(retried = 1)})
		else:
			self.failed()

	 def processed(self):
		wc = self._monque.get_collection('workers')
		wc.update(dict(_id = self._worker_id), {'$inc' : dict(processed = 1)})

	def failed(self):
		wc = self._monque.get_collection('workers')
		wc.update(dict(_id = self._worker_id), {'$inc' : dict(failed = 1)})
		
	def register_worker(self):
		self._worker_id = pymongo.objectid.ObjectId()
		c = self._monque.get_collection('workers')

		c.insert(dict(
			_id			= self._worker_id,
			hostname	= socket.gethostname(),
			pid			= os.getpid(),
			start_time	= None,
			job			= None,
			retried		= 0,
			processed	= 0,
			failed		= 0
		))
		
	def unregister_worker(self):
		wc = self._monque.get_collection('workers')
		wc.remove(dict(_id = self._worker_id))

	def _register_signal_handlers(self):
		signal.signal(signal.SIGTERM,	lambda num, frame: self._shutdown())
		signal.signal(signal.SIGINT,	lambda num, frame: self._shutdown())
		signal.signal(signal.SIGQUIT,	lambda num, frame: self._shutdown(graceful=True))
		signal.signal(signal.SIGUSR1,	lambda num, frame: self._kill_child())
	
	def reset_signal_handlers(self):
		signal.signal(signal.SIGTERM,	signal.SIG_DFL)
		signal.signal(signal.SIGINT,	signal.SIG_DFL)
		signal.signal(signal.SIGQUIT,	signal.SIG_DFL)
		signal.signal(signal.SIGUSR1,	signal.SIG_DFL)
	
	def _shutdown(self, graceful = False):
		if graceful:
			logging.info("Worker {0._worker_id} shutting down gracefully.".format(self))
			self._shutdown_status = "graceful"
		else:
			logging.info("Worker {0._worker_id} shutting down immediately.".format(self))
			self._shutdown_status = "immediate"
			self._kill_child()

	def _kill_child(self):
		if self.child:
			logging.info("Killing child {0}".format(self.child))

			if self.child.is_alive():
				self.child.terminate()

			self.child = None