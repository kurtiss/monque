#!/usr/bin/env python
# encoding: utf-8
"""
worker.py

Created by Kurtiss Hare on 2010-03-12.
"""

import datetime
import logging
import multiprocessing
import os
import pymongo.objectid
import signal
import socket
import time
import util

class MonqueWorker(object):
    def __init__(self, monque, queues=None, dispatcher="fork"):
        self._monque = monque
        self._queues = queues or []
        self._worker_id = None
        self._child = None
        self._shutdown_status = None
        self._dispatcher = dispatcher
    
    def register_worker(self):
        self._worker_id = pymongo.objectid.ObjectId()
        c = self._monque.get_collection('workers')

        c.insert(dict(
            _id         = self._worker_id,
            hostname    = socket.gethostname(),
            pid         = os.getpid(),
            start_time  = None,
            job         = None,
            retried     = 0,
            processed   = 0,
            failed      = 0
        ))

    def unregister_worker(self):
        wc = self._monque.get_collection('workers')
        wc.remove(dict(_id = self._worker_id))    
    
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
        order = self._monque.dequeue(self._queues, grabfor=60*60)

        if not order:
            return False

        if order:
            try:
                self.working_on(order)
                self.process(order)
            except Exception, e:
                self._handle_job_failure(order, e)
            else:
                self.done_working(order)

        return True
        
    def working_on(self, order):
        c = self._monque.get_collection('workers')

        c.update(dict(_id = self._worker_id), {
            '$set' : dict(
                start_time  = datetime.datetime.utcnow(),
                job         = order.job.__serialize__(),
            )
        })

    def process(self, order):
        if self._dispatcher == "fork":
            child = self._child = multiprocessing.Process(target=self._process_target, args=(order,))
            self._child.start()

            util.setprocname("monque: Forked {0} at {1}".format(self._child.pid, time.time()))

            while True:
                try:
                    child.join()
                except OSError, e:
                    if 'Interrupted system call' not in e:
                        raise
                    continue
                break

            self._child = None

            if child.exitcode != 0:
                raise Exception("Job failed with exit code {0}".format(child.exitcode))
        else:
            self.dispatch(order)
    
    def done_working(self, order):
        self._monque.remove(order.queue, order.job_id)
        self.processed()
        c = self._monque.get_collection('workers')
        c.remove(dict(_id = self._worker_id))
    
    def _process_target(self, order):
        self.reset_signal_handlers()
        self.dispatch(order)

    def dispatch(self, order):
        util.setprocname("monque: Processing {0} since {1}".format(order.queue, time.time()))
        order.job.run()
    
    def _handle_job_failure(self, order, e):
        import traceback
        logging.warn("Job failed ({0}): {1}\n{2}".format(order.job, str(e), traceback.format_exc()))

        if order.retries > 0:
            order.fail(e)
            self._monque.update(order.queue, order.job_id, delay=max(2**(len(order.failures)-1), 60), failure=str(e))

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
    
    def _register_signal_handlers(self):
        signal.signal(signal.SIGQUIT,   lambda num, frame: self._shutdown(graceful=True))
        if self._dispatcher == "fork":
            signal.signal(signal.SIGTERM,   lambda num, frame: self._shutdown())
            signal.signal(signal.SIGINT,    lambda num, frame: self._shutdown())
            signal.signal(signal.SIGUSR1,   lambda num, frame: self._kill_child())
    
    def reset_signal_handlers(self):
        signal.signal(signal.SIGTERM,   signal.SIG_DFL)
        signal.signal(signal.SIGINT,    signal.SIG_DFL)
        signal.signal(signal.SIGQUIT,   signal.SIG_DFL)
        signal.signal(signal.SIGUSR1,   signal.SIG_DFL)
    
    def _shutdown(self, graceful = False):
        if graceful:
            logging.info("Worker {0._worker_id} shutting down gracefully.".format(self))
            self._shutdown_status = "graceful"
        else:
            logging.info("Worker {0._worker_id} shutting down immediately.".format(self))
            self._shutdown_status = "immediate"
            self._kill_child()
    
    def _kill_child(self):
        if self._child:
            logging.info("Killing child {0}".format(self._child))
            
            if self._child.is_alive():
                self._child.terminate()
            
            self._child = None
