# -*- coding: utf-8 -*-
import logging
import time


class BaseQueue(object):
    """Base class that all queues inherit from"""

    def __init__(self, rediscon, name):
        self.log = logging.getLogger(self.__class__.__name__)
        self._redis = rediscon
        self.name = name
        self.log.debug('initializing queue %s', self.name)

    def __len__(self):
        raise NotImplementedError

    def push(self, data):
        raise NotImplementedError

    def pop(self):
        raise NotImplementedError


class FifoQueue(BaseQueue):
    """
    Redis Message queue based on redis lists (FIFO)
    """

    def __len__(self):
        return self._redis.llen(self.name)

    def push(self, msg):
        """
        Pushes msg to the queue
        Returns the number of elements in the queue
        after the push
        """
        return self._redis.rpush(self.name, msg)

    def pop(self):
        """
        Pops an element from the queue
        """
        return self._redis.lpop(self.name)


class PriorityQueue(BaseQueue):
    """
    Priority Message queue based on Redis sorted sets.

    Each message contains a score.
    Messages with lower score have higher priority!

    No duplicate messages are allowed, when you try to
    add a message that already exists in the queue
    then the score of the message is updated with the new score.

    In order to remove the msg with the lowest score from the sorted set
    I am using a zrange folloed by a zrem command.
    We need to ensure that these two operations are atomic.
    One way to do it is to use the watch command on the key,
    another way is to use some kind of locking and
    maybe(?) if you pipeline the commands is enough.
    I chose to use a lua script to do that since
    a. it is documented that it an atomic operation.
    b. According to a quick google search it is fast and
        performs better than watch
    c. very easy to implement
    """

    def __init__(self, rediscon, name):
        """
        Constructs a redis based priority queue named name.
        rediscon is a StrictRedis instance
        """
        super(PriorityQueue, self).__init__(rediscon, name)
        zpop_script = """
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
        self.zpop = self._redis.register_script(zpop_script)

    def __len__(self):
        return self._redis.zcard(self.name)

    def push(self, msg, score):
        """
        Adds the msg to the queue.
        Returns 1 if the msg was added.
        If msg already exists returns 0 but updates the msg's score.

        score should be a a float
        Messages with lower score have higher priority!
        """
        n = self._redis.zadd(self.name, score, msg)
        if n == 1:
            self.log.debug('Added %.1f , %s', score, msg)
        else:
            self.log.debug('Updated %.1f, %s', score, msg)
        return n

    def pop(self):
        """
        Pops the item with the lowest score( == highest priority).
        Returns None if no element is popped otherwise the popped element
        """
        return self.zpop(keys=[self.name], client=self._redis)


class ReliableQueue(BaseQueue):
    """
    Originally the idea is from Salvatore Sanfilippo (redis creator)
    Fore more details: http://oldblog.antirez.com/post/250

    This is an implementation of a reliable queue.
    The goal is to create a queue that is able to execute
    jobs and not losing jobs if they are not processed.

    The idea is to create a circulal queue the jobs are removed only
    id they are processed.

    When a client (consumer) finishes processing a job he should
    call the comlete method.

    Notice:

    From the above post (Improvement):
        If tasks take a lot of time to complete using LREM
        to delete the task may not be optimal.
        We may use an additional key with a Redis set where
        we store all the completed tasks, that the lua script
        will remove every time an item in the processing state
        is encountered and is also in the Set.

    The above functionality is implemented.
    The client should not care if a job is completed.
    Only unprocessed job will be returned from the pop method.
    A drawback is that the pop method can return None even
    if the queue is not emtpy.
    The client can check the len of the queue to
    determine if the list is empty.
    """

    def __init__(self, rediscon, name):
        """
        Constructs a redis based reliable queue named name.
        rediscon is a StrictRedis instance
        """
        super(ReliableQueue, self).__init__(rediscon, name)
        rpop_script = """
        local ret = nil
        local elem = redis.call('rpop', KEYS[1])
        local tmp = ''
        if elem then
            local l, msg, ts = string.match(elem, '(%d+):(.+):(%d*)')
            ret = {msg, ts}
            if ts == '' then
                tmp = l..':'..msg..':'..ARGV[1]
                redis.call('lpush', KEYS[1], tmp)
            else
                local completed_set = KEYS[1] .. ':completed_set'
                local completed = redis.call('sismember', completed_set, msg)
                if completed == 0 then
                    redis.call('lpush', KEYS[1], elem)
                else
                    redis.call('srem', completed_set, msg)
                    ret = nil
                end
            end
        end
        return ret
        """
        redo_script = """
        redis.call('lrem', KEYS[1], 0, ARGV[1])
        redis.call('srem',  KEYS[1] .. ':completed_set', ARGV[1])
        local l, msg, ts = string.match(ARGV[1], '(%d+):(.+):(%d*)')
        local nmsg = l .. ':' .. msg .. ':' .. ARGV[2]
        redis.call('lpush', KEYS[1], nmsg)
        return msg
        """
        self.rpop = self._redis.register_script(rpop_script)
        self.redo = self._redis.register_script(redo_script)
        self.completed_set = '%s:completed_set' % self.name

    def __len__(self):
        return self._redis.llen(self.name)

    def items(self):
        return self._redis.lrange(self.name, 0, -1)

    def push(self, msg):
        """
        Pushes msg into the queue
        """
        msg = "%d:%s:" % (len(msg,), msg)
        return self._redis.lpush(self.name, msg)

    def pop(self):
        """
        Pops an item out of the queue.
        If queue is empty returns None
        otherwise it returs a tuple (msg, unix_timestamp).

        If unix_timestamp is not in processing state the
        unix_timestamp is None.

        From http://oldblog.antirez.com/post/250

        What the consumer does with the returned value

        If the item is in processing state but is still young enough
        (no timeout) it is discarded and the script is called again to
        fetch the next ID.
        Otherwise if the item timed out the consumer will check if
        the item was actually processed or not by the original client,
        in an application-specific way, and will remove it from the
        queue if needed, otherwise the client will call another
        script that atomically remove the old item and add a new
        one with the new timestamp. And of course it will start processing it.
        When an item was processed successfully it gets removed
        from the queue using LREM.
        """
        ts = int(time.time())
        item = self.rpop(keys=[self.name], client=self._redis, args=[ts])
        if item is None:
            return item
        msg, ts = item
        ts = int(ts) if ts else None
        return msg, ts

    def complete(self, msg):
        """
        Marks the msg as completed so it will be deleted
        form the circulal list
        """
        self._redis.sadd(self.completed_set, msg)

    def remove(self, item):
        """
        Removes item from the queue
        """
        ts = str(item[1]) if item[1] else ''
        s = '%d:%s:%s' % (len(item[0]), item[0], ts)
        with self._redis.pipeline() as pipe:
            pipe.lrem(self.name, 0, s)
            pipe.srem(self.completed_set, item[0])
            pipe.execute()

    def reprocess(self, msg, cts):
        ts = int(time.time())
        msg = str(len(msg)) + ':' + msg + ':' + str(cts)
        return self.redo(keys=[self.name], client=self._redis, args=[msg, ts])
