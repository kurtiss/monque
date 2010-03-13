
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
    monque.enqueue("queue", some_job(argument))

Worker:

    from monque import Monque, MonqueWorker

    monque = Monque(mongodb)
    worker = MonqueWorker(monque, ["queue"])
    worker.work()
