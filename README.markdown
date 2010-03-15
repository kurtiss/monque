
Monque
======

Monque is a persistent job queue, written in Python, using MongoDB for storage.
It is basically a MongoDB version of Dreque (http://github.com/samuel/dreque).

License
-------

BSD License

See 'LICENSE' for details.

Usage
-----

Submitting jobs:

    from monque import Monque, job

    @job()
    def some_job(argument):
        pass

    monque = Monque(mongodb)
    monque.enqueue(some_job(argument), queue = 'my_queue')

Worker:

    from monque import Monque, MonqueWorker

    monque = Monque(mongodb)
		worker = monque.new_worker(queue = 'my_queue')
    worker.work()
