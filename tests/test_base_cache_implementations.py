import sys
import datetime
from unittest import TestCase
import zlib
import json

import mock

from condecache import cache

def skip_if_py2(func):
    if sys.version_info[0] == 2:
        def skipper(self, *a, **k):
            self.skipTest("Not running under PY2")
        skipper.__name__ = func.__name__
        return skipper
    return func


class TestBaseRedisCache(TestCase):
    _cls = cache.BaseRedisCache

    def test_set_no_ttl(self):
        redis_conn = mock.Mock(name='redis_conn')
        inst = self._cls(redis_conn, prefix='testing_is_fun')
        inst._default_ttl = 1000

        result = inst._set('key_a', 'some_awesome_value', None)

        redis_conn.set.assert_called_once_with(
                'testing_is_fun:key_a', 'some_awesome_value', ex=1000)
        self.assertIs(result, None)

    def test_set_ttl(self):
        redis_conn = mock.Mock(name='redis_conn')
        inst = self._cls(redis_conn, prefix='testing_is_fun')
        inst._default_ttl = 1000

        result = inst._set('key_a', 'some_awesome_value', 56.6)

        redis_conn.set.assert_called_once_with(
                'testing_is_fun:key_a', 'some_awesome_value', ex=57)
        self.assertIs(result, None)

    def test_set_redis_error_raised_internally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.set.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        with self.assertRaises(cache.RemoteCacheCommError):
            result = inst._set('a_key', 'a_value', 1)

    def test_set_redis_error_not_raised_externally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.set.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        try:
            inst.set('a_key', 'a_value', 1)
        except cache.RemoteCacheCommError:
            self.fail("RemoteCacheCommError raised")

    def test_get_exists(self):
        redis_conn = mock.Mock(name='redis_conn')
        inst = self._cls(redis_conn, prefix='testing_is_fun')

        default = mock.Mock(name='default')
        result = inst._get('key_a', default)

        redis_conn.get.assert_called_once_with('testing_is_fun:key_a')
        self.assertIs(result, redis_conn.get.return_value)

    def test_get_not_exists(self):
        redis_conn = mock.Mock(name='redis_conn')
        redis_conn.get.return_value = None
        inst = self._cls(redis_conn, prefix='testing_is_fun')

        default = mock.Mock(name='default')
        result = inst._get('key_a', default)

        redis_conn.get.assert_called_once_with('testing_is_fun:key_a')
        self.assertIs(result, default)

    def test_get_redis_error_raised_internally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.get.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        with self.assertRaises(cache.RemoteCacheCommError):
            result = inst._get('a_key', mock.Mock(name='default'))

    def test_get_redis_error_not_raised_externally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.get.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        try:
            inst.get('a_key')
        except cache.RemoteCacheCommError:
            self.fail("RemoteCacheCommError raised")

    def test_get_many(self):
        redis_conn = mock.Mock(name='redis_conn')
        val_1, val_2 = mock.Mock(name='val_1'), mock.Mock(name='val_2')
        default = mock.Mock(name='default')
        redis_conn.mget.return_value = [None, val_1, val_2, None]
        inst = self._cls(redis_conn, prefix='testing_is_the_best')

        result = inst._get_many(['key_a', 'key_b', 'key_c', 'key_d'], default)

        redis_conn.mget.assert_called_once_with(
            ['testing_is_the_best:key_a', 'testing_is_the_best:key_b',
            'testing_is_the_best:key_c', 'testing_is_the_best:key_d'],
        )
        self.assertEqual(result, [default, val_1, val_2, default])

    def test_get_many_redis_error_raised_internally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.mget.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        with self.assertRaises(cache.RemoteCacheCommError):
            result = inst._get_many(['a_key'], mock.Mock(name='default'))

    def test_get_many_redis_error_not_raised_externally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.mget.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        try:
            inst.get_many(['a_key'])
        except cache.RemoteCacheCommError:
            self.fail("RemoteCacheCommError raised")

    def test_remove_exists(self):
        redis_conn = mock.Mock(name='redis_conn')
        redis_conn.delete.return_value = 1
        inst = self._cls(redis_conn, prefix='testing_rocks')

        result = inst._remove('key_a')

        redis_conn.delete.assert_called_once_with('testing_rocks:key_a')
        self.assertIs(result, True)

    def test_remove_not_exists(self):
        redis_conn = mock.Mock(name='redis_conn')
        redis_conn.delete.return_value = 0
        inst = self._cls(redis_conn, prefix='testing_rocks')

        result = inst._remove('key_a')

        redis_conn.delete.assert_called_once_with('testing_rocks:key_a')
        self.assertIs(result, False)

    def test_remove_redis_error_raised_internally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.delete.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        with self.assertRaises(cache.RemoteCacheCommError):
            result = inst._remove('a_key')

    def test_remove_redis_error_not_raised_externally(self):
        redis_conn = mock.Mock(name='redis_conn')
        def raiser(*args, **kwargs):
            raise Exception("CATCH ME")
        redis_conn.delete.side_effect = raiser
        inst = self._cls(redis_conn, prefix='bad_things')

        try:
            inst.remove('a_key')
        except cache.RemoteCacheCommError:
            self.fail("RemoteCacheCommError raised")


class TestBaseContextCache(TestCase):
    _cls = cache.BaseContextCache

    def setUp(self):
        class TestClass(self._cls):
            _get = mock.Mock(name='_get')
            _remove = mock.Mock(name='_remove')
            _clear = mock.Mock(name='_clear')

        # Some tests need some more methods mocked.
        class HigherTestClass(TestClass):
            _encode = mock.Mock(name='_encode')
            _decode = mock.Mock(name='_decode')
            _get_many = mock.Mock(name='_get_many')
            _remove_many = mock.Mock(name='_remove_many')

        self._test_cls = TestClass
        self._higher_test_cls = HigherTestClass

    def tearDown(self):
        del self._test_cls
        del self._higher_test_cls

    @skip_if_py2
    def test_clear_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self._cls._clear(None)

    def test_enter_once(self):
        inst = self._test_cls()

        self.assertIs(inst._active, False)
        with inst:
            self.assertIs(inst._active, True)
        inst._clear.assert_called_once_with()
        self.assertIs(inst._active, False)

    def test_enter_multiple_times(self):
        inst = self._test_cls()

        self.assertIs(inst._active, False)
        with inst:
            self.assertIs(inst._active, True)
            with inst:
                self.assertIs(inst._active, True)
            self.assertIs(inst._active, True)
        inst._clear.assert_called_once_with()
        self.assertIs(inst._active, False)

    def test_exit_multiple_times(self):
        inst = self._test_cls()

        self.assertIs(inst._active, False)
        inst.__exit__((None,)*3)
        self.assertIs(inst._active, False)

    def test_check_exited_ok(self):
        inst = self._test_cls()

        inst.check_exited()

        inst._clear.assert_not_called()

    def test_check_exited_not_ok(self):
        inst = self._test_cls()

        with inst:
            with self.assertRaises(RuntimeError):
                inst.check_exited()
            inst._clear.assert_called_once_with()
            self.assertIs(inst._active, False)


class TestLocalContextCache(TestCase):
    _cls = cache.LocalContextCache

    def test_store_unentered(self):
        inst = self._cls()
        default = mock.Mock(name='default')

        set_result = inst.set('key_a', mock.Mock(name='object_a'))
        get_result = inst.get('key_a', default)

        self.assertIs(set_result, None)
        self.assertIs(get_result, default)

    def test_store_entered(self):
        inst = self._cls()
        default = mock.Mock(name='default')
        val_1 = mock.Mock(name='object_a')

        with inst:
            set_result = inst.set('key_a', val_1)
            get_result = inst.get('key_a', default)

            self.assertIs(set_result, None)
            self.assertIs(get_result, val_1)

        get_result = inst.get('key_a', default)
        self.assertIs(get_result, default)

    def test_remove(self):
        inst = self._cls()

        default = mock.Mock(name='default')
        val_1 = mock.Mock(name='val_1')

        with inst:
            set_result = inst.set('key_a', val_1)
            get_result_1 = inst.get('key_a', default)
            remove_result_1 = inst.remove('key_a')
            get_result_2 = inst.get('key_a', default)
            remove_result_2 = inst.remove('key_a')

        self.assertIs(set_result, None)
        self.assertIs(get_result_1, val_1)
        self.assertIs(remove_result_1, True)
        self.assertIs(get_result_2, default)
        self.assertIs(remove_result_2, False)

    def test_get_many(self):
        inst = self._cls()

        default = mock.Mock(name='default')
        val_1 = mock.Mock(name='val_1')
        val_2 = mock.Mock(name='val_2')
        val_3 = mock.Mock(name='val_3')

        vals = {
            'key_a': val_1,
            'key_b': val_2,
            # no key_c
            'key_d': val_3,
        }

        get_many_result_1 = inst.get_many(
                ['key_a', 'key_b', 'key_c', 'key_d'], default)
        with inst:
            set_many_result = inst.set_many(vals)
            get_many_result_2 = inst.get_many(
                    ['key_a', 'key_b', 'key_c', 'key_d'], default)

        self.assertEqual(get_many_result_1, {
            'key_a': default, 'key_b': default,
            'key_c': default, 'key_d': default,
        })
        self.assertIs(set_many_result, None)
        self.assertEqual(get_many_result_2, {
            'key_a': val_1, 'key_b': val_2,
            'key_c': default, 'key_d': val_3,
        })

    def test_remove_many(self):
        inst = self._cls()

        default = mock.Mock(name='default')
        val_1 = mock.Mock(name='val_1')
        val_2 = mock.Mock(name='val_2')

        remove_result_1 = inst.remove_many(['key_a', 'key_b', 'key_c'])
        with inst:
            inst.set('key_a', val_1)
            inst.set('key_b', val_2)
            remove_result_2 = inst.remove_many(['key_a', 'key_b', 'key_c'])

        self.assertEqual(remove_result_1, 0)
        self.assertEqual(remove_result_2, 2)

    def test_check_exited(self):
        inst = self._cls()

        try:
            inst.check_exited()
        except Exception as e:
            self.fail("check_exited raised an exception: {}".format(e))

        with inst:
            inst.set('a', 'some_value')

            with self.assertRaises(RuntimeError):
                inst.check_exited()
            self.assertIs(inst.get('a', None), None)

        try:
            inst.check_exited()
        except Exception as e:
            self.fail("check_exited raised an exception: {}".format(e))


class TestLocalContextAndRemoteTTLCache(TestCase):
    _cls = cache.LocalContextAndRemoteTTLCache
    _default = cache._DEFAULT

    def test_not_entered(self):
        remote_cache = mock.Mock(name='remote_cache')
        inst = self._cls(remote_cache)

        key, val, ttl = 'key', mock.Mock(name='val'), 1.0

        inst.set(key, val, ttl)

        result = inst.get(key)

        remote_cache.set.assert_called_once_with(key, val, ttl)
        # Check that the cache went to the remote for the value
        remote_cache.get.assert_called_once_with(key, self._default)
        self.assertIs(result, remote_cache.get.return_value)

    def test_entered(self):
        remote_cache = mock.Mock(name='remote_cache')
        inst = self._cls(remote_cache)

        key, val, ttl = 'key', mock.Mock(name='val'), 1.0

        with inst:
            inst.set(key, val, ttl)

            remote_cache.set.assert_called_once_with(key, val, ttl)

            result = inst.get(key)

            self.assertIs(result, val)
            remote_cache.get.assert_not_called()

        result = inst.get(key)
        remote_cache.get.assert_called_once_with(key, self._default)
        self.assertIs(result, remote_cache.get.return_value)


    def test_entered_not_exist(self):
        remote_cache = mock.Mock(name='remote_cache')
        inst = self._cls(remote_cache)

        key = 'key'

        with inst:
            result = inst.get(key)

            remote_cache.get.assert_called_once_with(key, self._default)
            self.assertIs(result, remote_cache.get.return_value)

    def test_get_many_not_entered(self):
        remote_cache = mock.Mock(name='remote_cache')

        key_1, val_1, ttl_1 = 'key_1', mock.Mock(name='val_1'), 1.0
        key_2, val_2, ttl_2 = 'key_2', mock.Mock(name='val_2'), 2.0
        key_3, val_3, ttl_3 = 'key_3', mock.Mock(name='val_3'), 3.0
        key_4 = 'key_4'

        remote_val_1 = mock.Mock(name='remote_val_1')
        remote_val_2 = mock.Mock(name='remote_val_2')
        remote_val_3 = mock.Mock(name='remote_val_3')

        remote_return_val = {
            key_1: remote_val_1,
            key_2: remote_val_2,
            key_3: remote_val_3,
            key_4: self._default,
        }
        default = mock.Mock(name='default')

        expected = {
            key_1: remote_val_1,
            key_2: remote_val_2,
            key_3: remote_val_3,
            key_4: default,
        }

        remote_cache.get_many.return_value = remote_return_val
        inst = self._cls(remote_cache)

        inst.set(key_1, val_1, ttl_1)
        inst.set(key_2, val_2, ttl_2)
        inst.set(key_3, val_3, ttl_3)

        result = inst.get_many([key_1, key_2, key_3, key_4], default)

        remote_cache.get_many.assert_called_once_with(
                [key_1, key_2, key_3, key_4], self._default)
        self.assertEqual(expected, result)

    def test_get_many_entered(self):
        remote_cache = mock.Mock(name='remote_cache')

        key_1, val_1, ttl_1 = 'key_1', mock.Mock(name='val_1'), 1.0
        key_2, val_2, ttl_2 = 'key_2', mock.Mock(name='val_2'), 2.0
        key_3, val_3, ttl_3 = 'key_3', mock.Mock(name='val_3'), 3.0
        key_4 = 'key_4'

        remote_val_1 = mock.Mock(name='remote_val_1')
        remote_val_2 = mock.Mock(name='remote_val_2')
        remote_val_3 = mock.Mock(name='remote_val_3')

        remote_return_val = {
            key_1: remote_val_1,
            key_4: self._default,
        }
        default = mock.Mock(name='default')

        expected = {
            key_1: remote_val_1,
            key_2: val_2,
            key_3: val_3,
            key_4: default,
        }

        remote_cache.get_many.return_value = remote_return_val
        inst = self._cls(remote_cache)

        inst.set(key_1, val_1, ttl_1)
        with inst:
            inst.set(key_2, val_2, ttl_2)
            inst.set(key_3, val_3, ttl_3)

            result = inst.get_many([key_1, key_2, key_3, key_4], default)

        remote_cache.get_many.assert_called_once_with([key_1, key_4], self._default)
        self.assertEqual(expected, result)

    def test_get_many_entered_all_local(self):
        remote_cache = mock.Mock(name='remote_cache')

        key_1, val_1, ttl_1 = 'key_1', mock.Mock(name='val_1'), 1.0
        key_2, val_2, ttl_2 = 'key_2', mock.Mock(name='val_2'), 2.0
        key_3, val_3, ttl_3 = 'key_3', mock.Mock(name='val_3'), 3.0

        default = mock.Mock(name='default')

        expected = {
            key_1: val_1,
            key_2: val_2,
            key_3: val_3,
        }

        inst = self._cls(remote_cache)

        with inst:
            inst.set(key_1, val_1, ttl_1)
            inst.set(key_2, val_2, ttl_2)
            inst.set(key_3, val_3, ttl_3)

            result = inst.get_many([key_1, key_2, key_3], default)

        remote_cache.get_many.assert_not_called()
        self.assertEqual(expected, result)

    def test_remove_not_entered_exist_remotely(self):
        remote_cache = mock.Mock(name='remote_cache')
        remote_cache.remove.return_value = True
        inst = self._cls(remote_cache)

        result = inst.remove('key')

        self.assertIs(result, True)
        remote_cache.remove.assert_called_once_with('key')

    def test_remove_not_entered_not_exist_remotely(self):
        remote_cache = mock.Mock(name='remote_cache')
        remote_cache.remove.return_value = False
        inst = self._cls(remote_cache)

        result = inst.remove('key')

        self.assertIs(result, False)
        remote_cache.remove.assert_called_once_with('key')

    def test_remove_entered_not_exist_locally_exist_remotely(self):
        remote_cache = mock.Mock(name='remote_cache')
        remote_cache.remove.return_value = True
        inst = self._cls(remote_cache)

        with inst:
            result = inst.remove('key')

        self.assertIs(result, True)
        remote_cache.remove.assert_called_once_with('key')

    def test_remove_entered_not_exist_locally_exist_remotely(self):
        remote_cache = mock.Mock(name='remote_cache')
        remote_cache.remove.return_value = False
        inst = self._cls(remote_cache)

        with inst:
            result = inst.remove('key')

        self.assertIs(result, False)
        remote_cache.remove.assert_called_once_with('key')

    def test_remove_entered_exit_locally_not_exist_remotely(self):
        remote_cache = mock.Mock(name='remote_cache')
        remote_cache.remove.return_value = False
        inst = self._cls(remote_cache)

        with inst:
            inst.set('key', mock.Mock(name='val'), 1)
            result = inst.remove('key')

        self.assertIs(result, True)
        remote_cache.remove.assert_called_once_with('key')

    def test_remove_entered_exit_locally_not_exist_remotely(self):
        remote_cache = mock.Mock(name='remote_cache')
        remote_cache.remove.return_value = True
        inst = self._cls(remote_cache)

        with inst:
            inst.set('key', mock.Mock(name='val'), 1)
            result = inst.remove('key')

        self.assertIs(result, True)
        remote_cache.remove.assert_called_once_with('key')
