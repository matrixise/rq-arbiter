# -*- coding: utf-8 -*-
import errno
import logging
import os
import signal
import sys
import time
import traceback
from rq.connections import Connection, get_current_connection
from rq.exceptions import NoQueueError, UnpickleError, DequeueTimeout
from rq.job import Job, Status
from rq.logutils import setup_loghandlers
from rq.queue import Queue, get_failed_queue
from rq.utils import make_colorizer

logger = logging.getLogger(__name__)

green = make_colorizer('darkgreen')

def iterable(x):
    return hasattr(x, '__iter__')

_signames = dict((getattr(signal, signame), signame)
        for signame in dir(signal)
        if signame.startswith('SIG') and '_' not in signame)

def signal_name(signum):
    try:
        return _signames[signum]
    except KeyError:
        return 'SIG_UNKNOWN'

class Worker(object):
    def __init__(self, connection, job, log):
        self.connection = connection
        self.job = job
        self.log = log

    def init_process(self):
        time.sleep(5)
        pass


class Arbiter(object):
    WORKERS = {}
    def __init__(self, queues, number_of_processes=1, connection=None):
        if connection is None:
            conenction = get_current_connection()

        self.connection = connection
        if isinstance(queues, Queue):
            queues = [queues]

        self.log = logger

        self.queues = queues
        self.validate_queues()

        self.number_of_processes = number_of_processes

        self.failed_queue = get_failed_queue(connection=self.connection)

        self._stopped = False

    def _install_signal_handlers(self):
        def request_stop(signum, frame):
            self.log.debug('Got signal %s.' % signal_name(signum))

            #signal.signal(signal.SIGINT, request_force_stop)
            #signal.signal(signal.SIGTERM, request_force_stop)

            msg = 'Warm shut down requested.'
            self.log.warning(msg)

            self._stopped = True

        def handle_sigchld(signum, frame):
            try:
                while True:
                    wpid, status = os.waitpid(-1, os.WNOHANG)
                    print "wpid, status: %r %r" % (wpid, status,)
                    if not wpid:
                        break
                    self.WORKERS.pop(wpid)
            except OSError as e:
                pass

        #signal.signal(signal.SIGINT, request_stop)
        #signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGCHLD, handle_sigchld)

    def validate_queues(self):
        if not iterable(self.queues):
            raise ValueError('Argument queues not iterable.')
        for queue in self.queues:
            if not isinstance(queue, Queue):
                raise NoQueueError('Give each worker at least one Queue.')

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return map(lambda q: q.name, self.queues)

    @property
    def stopped(self):
        return self._stopped

    def run(self):
        setup_loghandlers()
        self._install_signal_handlers()
        self.log.info('Arbiter started')
        qnames = self.queue_names()
        self.log.info('*** Listening on %s...' % green(', '.join(qnames)))
        while True:
            workers = len(self.WORKERS)
            self.log.info("Loop: %r", workers)
            self.log.info("Stopped: %r", self.stopped)
            if self.stopped:
                self.log.info('Stopping on request.')
                break

            # Naive, but if there is no free worker, we wait for one second.
            must_wait = workers == self.number_of_processes
            self.log.info("Exceded: %r", must_wait)
            if must_wait:
                time.sleep(1)
                continue

            timeout = 5
            result = self.dequeue_job_and_maintain_ttl(timeout)
            self.log.info("Result: %r", result,)
            if result:
                job, queue = result
                self.spawn_worker(job)

    def dequeue_job_and_maintain_ttl(self, timeout):
        while True:
            try:
                return Queue.dequeue_any(self.queues, timeout,
                        connection=self.connection)
            except DequeueTimeout:
                return None

    def kill_workers(self, sig):
        for pid in self.WORKERS.keys():
            self.kill_worker(pid, sig)

    def kill_worker(self, pid, sig):
        try:
            os.kill(pid, sig)
        except OSError as e:
            if e.errno != errno.ESRCH:
                raise

    def spawn_worker(self, job):
        connection = Connection()
        worker = Worker(connection, job, self.log)
        worker_pid = os.fork()
        if worker_pid == 0:
            try:
                self.log.info("Booting worker with pid: %s", worker_pid)
                worker.init_process()
                sys.exit(0)
            except SystemExit:
                raise
            except:
                self.log.exception("Exception in worker process:\n%s",
                        traceback.format_exc())
                sys.exit(-1)
        else:
            # We are in the parent
            self.WORKERS[worker_pid] = worker

    @property
    def pid(self):
        return os.getpid()

if __name__ == '__main__':
    print "PID: %r" % (os.getpid(),)
    with Connection():
        q_builds = Queue('builds')
        arbiter = Arbiter([q_builds], number_of_processes=4)
        arbiter.run()

