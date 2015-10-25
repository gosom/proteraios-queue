# -*- coding: utf-8 -*-
import time

import redis

import proteraios.client as client
import proteraios.queue as queues


def test_fifo_client():
    r = redis.Redis()
    try:
        c = client.Client(r, 'test_fifo', 'FifoQueue',
                                  sleep_func=time.sleep,
                                  max_poll_time=5,
                                  poll_freq=1)
        assert c
        assert c.qsize() == 0
        msg = 'hello wold'
        job = c.send(msg)
        assert isinstance(job, dict)
        assert job['status'] == 'expire'
    finally:
        r.flushdb()