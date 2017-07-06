from abc import ABCMeta, abstractmethod
import json
import logging
import zlib
import pickle

from ._six import add_metaclass, string_types
from .errors import (
    CacheError, RemoteCacheCommError, CacheDecodeError,
)
from .data_tools import JSONEncoder, json_object_hook


logger = logging.getLogger(__name__)


# Some sentinels. Use objects so that if somehow
# they leak, is easier to track down.
class _DEFAULT(object): pass
class _DORAISE(object): pass


__ALL__ = (
    'BaseCache', 'BaseNoTTLCache', 'BaseTTLCache',
    'ZLibCompressor',
    'JSONSerializer', 'PickleSerializer',
    'BaseRedisCache', 'BaseContextCache',
    'LocalContextCache', 'LocalContextAndRemoteTTLCache',
    'ZLibJsonRedisCache', 'ZLibPickleRedisCache', 'PickleRedisCache',
)


#
# Base interface definitions
#

@add_metaclass(ABCMeta)
class BaseCache(object):
    serializer = None
    compressor = None

    def get(self, key, default=None):
        """Get a single item in the cache.

            `key` - the key for the item in the cache to return.
            `default` - the value to return if the item is not found.

            returns: <object>
                the value for the item in the cache, or `default` if not found.
        """
        if not isinstance(key, string_types):
            raise TypeError("key must be a string")

        try:
            val = self._get(key, _DEFAULT)
        except CacheError as e:
            logger.error("Error during cache get: %s", e)
            return default

        if val is not _DEFAULT:
            return self._decode(val, default)

        return default

    def get_many(self, keys, default=None):
        """Get many items from the cache

            `keys` - iterable of cache keys to find.

            returns: list
                a list of values in the same order as the keys. If any cache
                keys are not found, then their value will be None.
        """
        keys = list(keys) # reduce a generator/iter if it is one.
        if not all(isinstance(key, string_types) for key in keys):
            raise TypeError("keys must be strings")

        if not keys:
            return {}

        try:
            values = self._get_many(keys, _DEFAULT)
        except CacheError as e:
            logger.error("Error during cache get_many: %s", e)
            return {key: default for key in keys}

        values = {
            key: self._decode(value, default) if value is not _DEFAULT else default
            for key, value in zip(keys, values)
        }
        return values

    def remove(self, key):
        """Remove an item from the cache

            `key` - the key for the item to remove

            returns: True/False
                True if key was succesfully removed
                False if key was not present.
        """
        if not isinstance(key, string_types):
            raise TypeError("key must be a string")

        try:
            return self._remove(key)
        except CacheError as e:
            logger.error("Error during cache remove: %s", e)
            return False

    def remove_many(self, keys):
        """remove multple items from the cache

            `keys` - an iterable of cache keys to remove

            returns: int
                a count of the number of items which were present.
        """
        if not all(isinstance(key, string_types) for key in keys):
            raise TypeError("keys must be strings")

        if not keys:
            return 0

        try:
            return self._remove_many(keys)
        except CacheError as e:
            logger.error("Error during cache remove_many: %s", e)
            return 0

    def __getitem__(self, key):
        val = self.get(key, _DEFAULT)
        if val is _DEFAULT:
            raise KeyError(key)
        return val

    def __delitem__(self, key):
        existed = self.remove(key)
        if not existed:
            raise KeyError(key)

    #
    # Internal logic, abstract methods _must_ be overridden,
    # other may or may not be overridden if different behaviour
    # is required.
    #
    @abstractmethod
    def _get(self, key, default):
        """override to return the raw cached value, or `default` if not found.
        """
        raise NotImplementedError

    def _get_many(self, keys, default):
        """override for a more effecient implementation.
            return a list of the raw cached values, or `default` if not found.
        """
        return [self._get(key, default) for key in keys]

    @abstractmethod
    def _remove(self, key):
        """override to remove an item from the cache, and return True if the
            item was present, or False if the item was not present.
        """
        raise NotImplementedError

    def _remove_many(self, keys):
        """override for a more effecient implementation.
            return a count of items which were presentin the cache.
        """
        return sum(1 if self._remove(key) is True else 0 for key in keys)


    #
    # Serialization and compression.
    #
    @classmethod
    def _encode(cls, raw_data):
        if cls.serializer is None:
            return raw_data

        serialized = cls.serializer.serialize(raw_data)

        if cls.compressor is None:
            return serialized

        return cls.compressor.compress(serialized)

    @classmethod
    def _decode(cls, encoded, fallback=_DORAISE):
        if cls.serializer is None:
            return encoded

        if cls.compressor is not None:
            try:
                decompressed = cls.compressor.decompress(encoded)
            except CacheDecodeError:
                if fallback is not _DORAISE:
                    return fallback
                raise
        else:
            decompressed = encoded

        try:
            return cls.serializer.deserialize(decompressed)
        except CacheDecodeError:
            if fallback is not _DORAISE:
                return fallback
            raise


class BaseNoTTLCache(BaseCache):
    #
    # Interface methods, try not to override.
    #
    def set(self, key, value):
        """Set `key` to be `value` in the cache

            `key` must be a string.

            `value` must be encodable by the cache class. See the
                class definition for more details.

        """
        if not isinstance(key, string_types):
            raise TypeError("key must be a string")

        value = self._encode(value)

        try:
            self._set(key, value)
        except CacheError as e:
            logger.error("(noTTL) Error during cache set: %s", e)

    def set_many(self, sequence=None, **kwarg_values):
        """Set many items in the cache.

            `sequence` - A Mapping, values must be encodable by the cache
                class. See the class definition for more details.
            **`kwarg_values` - keyword arguments will also be added to
                the cache. Values must be encodable by the cache
                class. See the class definition for more details.
        """
        if sequence is not None:
            try:
                values = dict(sequence, **kwarg_values)
            except TypeError as e:
                raise TypeError("Invalid items sequence: {}".format(e.args[0]))
            except ValueError as e:
                raise ValueError("Invalid items sequence: {}".format(e.args[0]))
        else:
            values = kwarg_values

        if not values:
            return

        for key in values.keys():
            if not isinstance(key, string_types):
                raise TypeError("keys must be strings")

            values[key] = self._encode(values[key])

        try:
            self._set_many(values)
        except CacheError as e:
            logger.error("(noTTL) Error during cache set_many: %s", e)

    def __setitem__(self, key, val):
        self.set(key, val)
    #
    # Internal logic, abstract methods _must_ be overridden,
    # other may or may not be overridden if different behaviour
    # is required.
    #
    @abstractmethod
    def _set(self, key, value):
        """override to accept an encoded value"""
        raise NotImplementedError

    def _set_many(self, dict_vals):
        """override for a more efficient implementation.
            This is given encoded values.
        """
        for k, v in dict_vals.items():
            self._set(k, v)


class BaseTTLCache(BaseCache):
    #
    # Interface methods, try not to override.
    #
    def set(self, key, value, ttl_seconds):
        """Set `key` to be `value` in the cache

            `key` must be a string.

            `value` must be encodable by the cache class. See the
                class definition for more details.

            `ttl_seconds` number of seconds (int or float) for which to cache
                the item.
        """
        if not isinstance(key, string_types):
            raise TypeError("key must be a string")

        if ttl_seconds is not None:
            try:
                ttl_seconds = float(ttl_seconds)
            except TypeError as e:
                raise TypeError("invalid ttl_seconds {}".format(e.args[0]))
            except ValueError as e:
                raise ValueError("Invalid ttl_seconds {}".format(e.args[0]))

        value = self._encode(value)

        try:
            self._set(key, value, ttl_seconds)
        except CacheError as e:
            logger.error("(TTL) Error during cache set: %s", e)
    #
    # Internal logic, abstract methods _must_ be overridden,
    # other may or may not be overridden if different behaviour
    # is required.
    #
    @abstractmethod
    def _set(self, key, value, ttl_seconds):
        """override to accept an encoded value
            and to adhere to ttl_seconds
        """
        raise NotImplementedError


#
# Compressors
#
class ZLibCompressor(object):
    @staticmethod
    def compress(serialized):
        if isinstance(serialized, string_types):
            serialized = serialized.encode()
        return zlib.compress(serialized)

    @staticmethod
    def decompress(compressed):
        try:
            return zlib.decompress(compressed)
        except (TypeError, ValueError, zlib.error) as e:
            raise CacheDecodeError(e)

#
# Serializers
#
class JSONSerializer(object):

    _json_decoder = json.JSONDecoder(object_hook=json_object_hook)
    _json_encoder = JSONEncoder()

    @classmethod
    def serialize(cls, raw_data):
        return cls._json_encoder.encode(raw_data)

    @classmethod
    def deserialize(cls, serialized):
        if not isinstance(serialized, string_types):
            serialized = serialized.decode()

        try:
            return cls._json_decoder.decode(serialized)
        except (ValueError, TypeError) as e:
            raise CacheDecodeError(e)


class PickleSerializer(object):
    @staticmethod
    def serialize(raw_data):
        return pickle.dumps(raw_data)

    @staticmethod
    def deserialize(serialized):
        try:
            return pickle.loads(serialized)
        except (EOFError, pickle.UnpicklingError, TypeError, ValueError) as e:
            raise CacheDecodeError(e)


#
# Base remote caches
#
class BaseRedisCache(BaseTTLCache):
    """Base generic redis class, does not implement encoding/decoding.

        This class does not create a redis connection, however it takes
        one as an argument.
        This must be an object which adheres to the same interface as the
        (at the time of writing) main redis library redis-py by Andy McCurdy
        https://github.com/andymccurdy/redis-py
    """
    _default_ttl = 60 # seconds

    def __init__(self, redis_connection, prefix=''):
        self._conn = redis_connection
        self._prefix = prefix

    @staticmethod
    def _try_redis_action(cb, *args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except Exception as e:
            # This module isn't dependant on the actual redis library, and
            # therefore can't catch the actual redis exceptions here. So just
            # catch everything.
            msg = "Failed to talk to redis {}: {}".format(
                    e.__class__.__name__, e)
            raise RemoteCacheCommError(msg)

    def _make_key(self, key):
        if self._prefix:
            return self._prefix + ':' + key
        else:
            return key

    def _set(self, key, value, ttl_seconds):
        ttl = round(ttl_seconds or self._default_ttl)
        key = self._make_key(key)
        self._try_redis_action(self._conn.set, key, value, ex=ttl)

    def _get(self, key, default):
        key = self._make_key(key)
        val = self._try_redis_action(self._conn.get, key)

        if val is None:
            return default

        return val

    def _get_many(self, keys, default):
        vals = self._try_redis_action(
            self._conn.mget,
            [self._make_key(key) for key in keys]
        )
        return [
            i if i is not None else default
            for i in vals
        ]

    def _remove(self, key):
        result = self._try_redis_action(self._conn.delete, self._make_key(key))
        return result == 1


#
# Base context cache
#
@add_metaclass(ABCMeta)
class BaseContextCache(BaseCache):
    def __init__(self):
        self._count = 0

    @property
    def _active(self):
        return self._count > 0

    def __enter__(self):
        self._count += 1
        return self

    def __exit__(self, *exc_info):
        self._count -= 1

        if self._count == 0:
            self._clear()

        if self._count < 0:
            self._count = 0

    def check_exited(self):
        if self._count > 0:
            self._count = 0
            self._clear()
            raise RuntimeError("Context Cache was supposed to be fully exited.")

    @abstractmethod
    def _clear(self):
        """method called when no longer in context."""
        raise NotImplementedError

#
# Cache implementations
#
class LocalContextCache(BaseContextCache, BaseNoTTLCache):
    """A context manager to cache items in memory. This is to be used
        for a short period (e.g. a single HTTP request) to avoid items being
        queried out of the database, or retrieved from an external cache,
        more than needed.

        Handles being nested silently.
    """
    def __init__(self):
        super(LocalContextCache, self).__init__()
        self._cache = {}

    #
    # Methods that have to be overridden for BaseContextCache
    #
    def _clear(self):
        self._cache.clear()

    #
    # Methods that have to be overridden for BaseNoTTLCache
    #
    def _set(self, key, value):
        if self._active:
            self._cache[key] = value

    def _get(self, key, default):
        return self._cache.get(key, default)

    def _remove(self, key):
        return self._cache.pop(key, _DEFAULT) is not _DEFAULT


class LocalContextAndRemoteTTLCache(BaseContextCache, BaseTTLCache):
    """Context cache which will always try to get values from a remote cache,
        but will cache things locally if entered.
    """
    # N.B. it is paramount that this class never does any
    # serialization or compression.
    def __init__(self, remote_cache):
        super(LocalContextAndRemoteTTLCache, self).__init__()
        self._cache = {}
        self._remote_cache = remote_cache

    #
    # Methods that have to be overridden for BaseContextCache
    #
    def _clear(self):
        self._cache.clear()

    #
    # Methods that have to be overridden for BaseTTLCache
    #
    def _set(self, key, value, ttl):
        if self._active:
            self._cache[key] = value
        self._remote_cache.set(key, value, ttl)

    def _get(self, key, default):
        val = self._cache.get(key, _DEFAULT)

        if val is _DEFAULT:
            val = self._remote_cache.get(key, _DEFAULT)

            if val is not _DEFAULT and self._active:
                # Found in remote, so cache it locally.
                self._cache[key] = val

        return val

    def _get_many(self, keys, default):
        vals = {
            key: self._cache[key]
            for key in keys
            if key in self._cache
        }
        missing_keys = [key for key in keys if key not in vals]

        if missing_keys:
            missing_vals = self._remote_cache.get_many(missing_keys, _DEFAULT)
            vals.update(missing_vals)

            if self._active:
                # This will put _DEFAULT into our cache, but that's ok.
                self._cache.update(missing_vals)

        return [
            vals[key] if vals[key] is not _DEFAULT else default
            for key in keys
        ]

    def _remove(self, key):
        existed_local = self._cache.pop(key, _DEFAULT) is not _DEFAULT
        existed_remote = self._remote_cache.remove(key)
        return existed_local or existed_remote


class ZLibJsonRedisCache(BaseRedisCache):
    compressor = ZLibCompressor
    serializer = JSONSerializer


class ZLibPickleRedisCache(BaseRedisCache):
    compressor = ZLibCompressor
    serializer = PickleSerializer


class PickleRedisCache(BaseRedisCache):
    compressor = None
    serializer = PickleSerializer
