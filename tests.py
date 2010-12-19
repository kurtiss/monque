#!/usr/bin/env python
# encoding: utf-8
"""
tests.py

Created by Kurtiss Hare on 2010-03-14.
Copyright (c) 2010 Medium Entertainment, Inc. All rights reserved.
"""

import pymongo
import monque
import multiprocessing
import os
import pickle
import signal
import tempfile
import time
import unittest

retry_job_limit = 3

def do_set_test_values(tmpfile, *test_args, **test_kwargs):
    with open(tmpfile, "w+") as f:
        f.write(pickle.dumps((test_args, test_kwargs)))

def do_get_test_values(tmpfile, *default_args, **default_kwargs):
    if os.path.exists(tmpfile) and os.stat(tmpfile).st_size > 0:
        with open(tmpfile, "rb") as f:
            args, kwargs = pickle.loads(f.read())
    else:
        args = default_args
        kwargs = default_kwargs

    return args, kwargs
    
def item_set(values):
    try:
        return set(values.items())
    except (AttributeError, ValueError), e:
        pass

@monque.job()
def set_test_values(*args, **kwargs):
    do_set_test_values(*args, **kwargs)

@monque.job()
def long_job(*args, **kwargs):
    time.sleep(2)
    do_set_test_values(*args, **kwargs) 

@monque.job()
def retry_job(tmpfile):
    (retry_count,), empty = do_get_test_values(tmpfile, 0)
    retry_count += 1
    do_set_test_values(tmpfile, retry_count)

    if retry_count <= retry_job_limit:
        raise RuntimeError('Forced retry job failure, this is to be expected...')
    else:
        raise RuntimeError('Job is being retried too many times, hopefully this gets caught...')


class TestMonque(unittest.TestCase):
    def setUp(self):
        import logging
        logging.basicConfig(level=logging.ERROR)

        connection = pymongo.Connection()
        self.monque = monque.Monque(connection['monque-test'], default_queue = 'test_queue')
        self.tmpfile = tempfile.mkstemp()[1]
        self.monque.clear()
    
    def failUnlessTestValuesEqual(self, args, kwargs):
        test_args, test_kwargs = do_get_test_values(self.tmpfile)
        self.failUnlessEqual(tuple(args), tuple(test_args))
        self.failUnlessEqual(item_set(kwargs), item_set(test_kwargs))
        os.unlink(self.tmpfile)
    
    def testPushPop(self):
        self.monque.push("test_queue", "alf")
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job['body'], "alf")
    
    def testRemove(self):
        _id = self.monque.push("test_queue", "alf")
        self.monque.remove("test_queue", _id)
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job, None)
    
    def testDelayedPush(self):
        self.monque.push("test_queue", "alf", delay=1)
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job, None)
        time.sleep(1)
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job['body'], "alf")
    
    def testPopGrabFor(self):
        self.monque.push("test_queue", "alf")
        job = self.monque.pop("test_queue", grabfor=1)
        self.failUnlessEqual(job['body'], "alf")
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job, None)
        time.sleep(1)
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job['body'], "alf")

    def testUpdate(self):
        _id = self.monque.push("test_queue", "alf")
        self.monque.update("test_queue", _id, failure="cats")
        job = self.monque.pop("test_queue", grabfor=5)
        self.failUnlessEqual(job['failures'], ["cats"])
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job, None)
        self.monque.update("test_queue", _id, delay=0)
        job = self.monque.pop("test_queue")
        self.failUnlessEqual(job['body'], "alf")
    
    def testJobs(self):
        import tests

        args = (1, 2)
        kwargs = dict(
            a = 1,
            b = 2,
            c = 3
        )
        
        self.monque.clear()

        enqueued = tests.set_test_values(self.tmpfile, *args, **kwargs)
        self.monque.enqueue(enqueued)

        dequeued = self.monque.dequeue()
        self.failUnlessEqual(enqueued, dequeued)

        dequeued.job.run()
        self.failUnlessTestValuesEqual(args, kwargs)

    def testWorker(self):
        import tests
    
        args = (1,)
        kwargs = dict(a = 2)
        
        self.monque.clear()
        
        self.monque.enqueue(tests.set_test_values(self.tmpfile, *args, **kwargs))
        worker = self.monque.new_worker()
        worker.work(interval = 0)

        self.failUnlessTestValuesEqual(args, kwargs)    
     
    def testDelayedWorker(self):
        import tests
        
        args = (1,)
        kwargs = dict(a = 2)
        delay = 2

        self.monque.clear()
    
        worker = self.monque.new_worker()
        self.monque.enqueue(tests.set_test_values(self.tmpfile, *args, **kwargs), delay = delay)
        worker.work(interval = 0)
    
        time.sleep(delay)
        worker.work(interval = 0)
        self.failUnlessTestValuesEqual(args, kwargs)
    
    def doShutdownTest(self, sig, job_should_run):
        import tests
    
        args = (2,)
        kwargs = dict(z = 3)
        
        self.monque.clear()
    
        self.monque.enqueue(tests.long_job(self.tmpfile, *args, **kwargs))
    
        child = multiprocessing.Process(target = self.monque.new_worker().work)
        child.start()

        time.sleep(0.5)
        os.kill(child.pid, sig)
        child.join()
    
        self.failUnlessEqual(child.exitcode, 0)
    
        if job_should_run:
            self.failUnlessTestValuesEqual(args, kwargs)
        else:
            a = os.path.exists(self.tmpfile)
            self.failUnless(not a or os.stat(self.tmpfile).st_size == 0)

    def testGracefulShutdown(self):
        self.doShutdownTest(signal.SIGQUIT, True)

    def testForcefulShutdown(self):
        self.doShutdownTest(signal.SIGINT, False)

    def testRetries(self):
        import tests

        def worker():
            self.monque.new_worker().work(interval = 0.1)
        
        self.monque.clear()

        self.monque.enqueue(tests.retry_job(self.tmpfile), retries = retry_job_limit)
    
        child = multiprocessing.Process(target = worker)
        child.start()
    
        time.sleep(10)
        os.kill(child.pid, signal.SIGQUIT)
        child.join()
        
        self.failUnlessTestValuesEqual((retry_job_limit,), dict())
 
if __name__ == '__main__':
    # import logging
    # logging.basicConfig(level=logging.INFO)
    unittest.main()
