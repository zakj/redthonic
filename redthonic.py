class RedisValue(object):
    base_commands = (
        'exists',
        'delete',
        'type',
        'rename',
        'renamenx',
    )
    commands = ()

    def __init__(self, db, key):
        self.db = db
        self.key = key

    def _curry(self, cmd):
        def f(*args, **kwargs):
            return getattr(self.db, cmd)(self.key, *args, **kwargs)
        return f

    def __getattr__(self, attr):
        if attr in self.commands or attr in self.base_commands:
            return self._curry(attr)
        raise AttributeError("'%s' object has no attribute '%s'" %
                             (self.__class__.__name__, attr))

    def rename(self, new):
        success = self.db.rename(self.key, new)
        self.key = new
        return success

    def renamenx(self, new):
        success = self.db.renamenx(self.key, new)
        if success:
            self.key = new
        return success


class RedisString(RedisValue):
    """A Pythonic representation of a Redis string value.

    >>> s = RedisString(db, 'testkey')
    >>> s.set('spam')
    True
    >>> s.type()
    'string'
    >>> print s
    spam
    >>> len(s)
    4

    String methods also work as expected:

    >>> s.endswith('am')
    True
    >>> s.endswith('eggs')
    False

    """
    commands = (
        'set',
        'get',
        'getset',
        'setnx',
        'setex',
        'incr',
        'incrby',
        'decr',
        'decrby',
        'append',
        'substr',
    )

    def __str__(self):
        return self.get()

    def __len__(self):
        return len(str(self))

    def __set__(self, instance, value):
        self.set(value)

    def __getattr__(self, attr):
        # For attribute access that does not match a Redis command, attempt to
        # find a matching string attribute.
        try:
            return super(RedisString, self).__getattr__(attr)
        except AttributeError, e:
            if hasattr(str, attr):
                return getattr(str(self), attr)
            raise e


class RedisList(RedisValue):
    """A Pythonic representation of a Redis list value.

    >>> l = RedisList(db, key='testlist')
    >>> l.append('spam')
    >>> l.type()
    'list'
    >>> l[:]
    ['spam']
    >>> l.extend(['spam', 'spam', 'eggs', 'spam'])
    >>> l[:]
    ['spam', 'spam', 'spam', 'eggs', 'spam']
    >>> l[1] = 'sausage'
    >>> l[:]
    ['spam', 'sausage', 'spam', 'eggs', 'spam']
    >>> len(l)
    5
    >>> l[-2]
    'eggs'
    >>> l[:2]
    ['spam', 'sausage']
    >>> [item for item in l if item == 'spam']
    ['spam', 'spam', 'spam']
    >>> 'sausage' in l
    True
    >>> 'python' in l
    False

    """
    commands = (
        'rpush',
        'lpush',
        'llen',
        'lrange',
        'ltrim',
        'lindex',
        'lset',
        'lrem',
        'lpop',
        'rpop',
    )

    def __len__(self):
        return self.llen()

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.step is not None and index.step != 1:
                raise IndexError('step must be 1')
            start = index.start if index.start is not None else 0
            stop = index.stop if index.stop is not None else -1
            # The rightmost item is included in Redis, but not Python.
            if stop > 0:
                stop -= 1
            if start == stop:
                return []
            return self.lrange(start, stop)
        return self.lindex(index)

    def __setitem__(self, index, value):
        return self.lset(index, value)

    def __iter__(self):
        # Callers might be iterating through very long lists; it doesn't make
        # sense to retrieve items one at a time, but retrieving the entire list
        # at once is probably also overkill.  Instead, get bufsize at a time.
        left = 0
        bufsize = 20  # TODO: make this configurable?
        while left < len(self):
            # bufsize - 1 because Redis includes the rightmost item.
            for item in self.lrange(left, left + bufsize - 1):
                yield item
            left += bufsize

    def append(self, value):
        self.rpush(value)

    def extend(self, values):
        for value in values:
            self.append(value)


class RedisSet(RedisValue):
    """A Pythonic representation of a Redis set value.

    >>> s = RedisSet(db, 'testset')
    >>> s.add('graham')
    >>> s.add('eric')
    >>> len(s)
    2
    >>> 'graham' in s
    True
    >>> 'john' in s
    False
    >>> s.add('john'); s.add('terry')
    >>> s2 = set(['terry', 'michael'])
    >>> sorted(s.union(s2))
    ['eric', 'graham', 'john', 'michael', 'terry']
    >>> sorted(s.difference(s2))
    ['eric', 'graham', 'john']
    >>> s.intersection(s2)
    set(['terry'])
    >>> s.remove('john')
    >>> len(s)
    3

    """
    commands = (
        'sadd',
        'srem',
        'spop',
        'scard',
        'sismember',
        'smembers',
        'srandmembers',
    )

    def __contains__(self, value):
        return self.sismember(value)

    def __len__(self):
        return self.scard()

    def add(self, value):
        self.sadd(value)

    def difference(self, other):
        if isinstance(other, self.__class__):
            return self.sdiff(other.key)
        return self.smembers().difference(other)

    def intersection(self, other):
        if isinstance(other, self.__class__):
            return self.sinter(other.key)
        return self.smembers().intersection(other)

    def pop(self):
        return self.spop()

    def remove(self, value):
        if not self.srem(value):
            raise KeyError(value)

    def union(self, other):
        if isinstance(other, self.__class__):
            return self.sunion(other.key)
        return self.smembers().union(other)


if __name__ == '__main__':
    import doctest
    import redis
    db = redis.Redis(db=9)  # XXX make this configurable, since we're flushing
    db.flushdb()
    doctest.testmod()
    db.flushdb()
