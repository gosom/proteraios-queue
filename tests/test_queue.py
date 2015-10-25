# -*- coding: utf-8 -*-
import time

import redis

from proteraios.queue import (PriorityQueue,
                                                 FifoQueue,
                                                 ReliableQueue)


def test_scripting():
    r = redis.StrictRedis()
    try:
        q = PriorityQueue(r, 'test_pqueue1')

        script = """
        local result = redis.call('zrange', KEYS[1], 0, 0)
        local msg
        if result then
            msg = result[1]
            redis.call('zrem', KEYS[1], msg)
        else
            msg = nil
        end
        return msg
        """
        zpop = r.register_script(script)
        item = zpop(keys=['test_pqueue1'], client=r)
        q.push("hi", 1)
        assert item is None
        item = zpop(keys=['test_pqueue1'], client=r)
        assert 'hi' == item
        item = zpop(keys=['test_pqueue1'], client=r)
        assert not item
    finally:
        r.delete('test_pqueue1')


def test_pqueue():
    r = redis.StrictRedis()
    try:
        q = PriorityQueue(r, 'test_pqueue2')

        items = [("one", 0), ("two", -1.0), ("three", 5.2)]
        for msg, score in items:
            q.push(msg, score)

        assert len(q) == len(items)

        expected = ["two", 'one', 'three']
        found = []
        while True:
            msg = q.pop()
            if msg is None:
                break
            else:
                found.append(msg)

        assert expected == found
    finally:
        r.delete('test_pqueue2')


def test_fifo():
    r = redis.StrictRedis()
    try:
        q = FifoQueue(r, 'test_fifo')
        items = ('one', 'two', 'three')
        map(q.push, items)
        for item in items:
            assert q.pop() == item
    finally:
        r.delete('test_fifo')


def test_reliable():
    r = redis.StrictRedis()
    try:
        q = ReliableQueue(r, 'test_reliable')
        item = q.pop()
        assert item is None

        q.push('hi')
        item = q.pop()
        assert item == ('hi', None)
        q.complete(item[0])
        item = q.pop()
        assert len(q) == 0

        item = q.pop()
        assert item is None


        q.push('hi')
        assert len(q) == 1

        msg1, _ = q.pop()
        assert len(q) == 1
        msg2, ts = q.pop()
        assert msg1 == msg2
        time.sleep(1)
        assert len(q) == 1

        if time.time() >= ts + 0.5:
            p = q.reprocess(msg2, ts)
            assert len(q) == 1
            assert p == msg2
            assert len(q.items()) == 1
            q.complete(p)
            assert len(q) == 1
            assert q.pop() is None
            assert len(q) == 0
        # time.sleep(0.5)
        # if time.time() >= ts + 1:
        #     p = q.reprocess(msg)
        # print q.items()

    finally:
        r.delete('test_reliable')