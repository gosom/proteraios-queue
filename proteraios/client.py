# -*- coding: utf-8 -*-
import logging
import json
from copy import deepcopy
import time
import uuid

from .queue import factory


class Client(object):

    job_tpl = {'id': None,
                     'msg': None,
                     'create_time': None,
                     'timeout': None,
                     'finish_time': None,
                     'status': None,
                     'result': None,
                     'errors': None}

    def __init__(self, rediscon, qname, qtype, sleep_func,
                       max_poll_time=120, poll_freq=1):
        self.log = logging.getLogger(self.__class__.__name__)
        self._rediscon = rediscon
        self._queue = factory(qtype, rediscon, qname)
        self.sleep_func = sleep_func
        self.max_poll_time = max_poll_time
        self.poll_freq = poll_freq

    def qsize(self):
        return len(self._queue)

    def send(self, msg, timeout=None):
        """
        Creates a job submits to the queue
        and polls until job is done or job times out
        """
        job = self._create_job(msg, timeout)
        status = self._poll(job)
        ret = self._rediscon.hgetall(job['id'])
        self._rediscon.delete(job['id'])
        return ret

    def _create_job(self, msg, timeout):
        job = deepcopy(self.job_tpl)
        job['id'] = uuid.uuid4().hex
        job['msg'] = json.dumps(msg)
        job['create_time'] = int(time.time())
        job['timeout'] = timeout
        job['status'] = 'new'
        self._rediscon.hmset(job['id'], job)
        self._queue.push(job['id'])
        return job

    def _poll(self, job):
        timeout = job['timeout'] if job['timeout'] else self.max_poll_time
        poll_until = int(time.time()) + timeout
        status = None
        while True:
            if time.time() > poll_until:
                self._rediscon.hset(job['id'], 'status', 'expire')
                break
            status = self._get_status(job)
            if status in ('complete', 'fail', 'expire' ):
                break
            self.sleep_func(self.poll_freq)
        return self._get_status(job)

    def _is_completed(self, job):
        status = self._get_status(job)
        return status in ('complete', 'fail')

    def _get_status(self, job):
        return self._rediscon.hget(job['id'], 'status')

