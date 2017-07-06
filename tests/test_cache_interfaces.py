import sys
from unittest import TestCase

import mock

from condecache import cache


def skip_if_py2(func):
    if sys.version_info[0] == 2:
        def skipper(self, *a, **k):
            self.skipTest("Not running under PY2")
        skipper.__name__ = func.__name__
        return skipper
    return func


class TestBaseCache(TestCase):
    _cls = cache.BaseCache
    _default = cache._DEFAULT

    def setUp(self):
        class TestClass(self._cls):
            _get = mock.Mock(name='_get')
            _remove = mock.Mock(name='_remove')

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

    def test_is_abstract(self):
        with self.assertRaises(TypeError):
            self._cls()

    @skip_if_py2
    def test_get_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            self._cls._get(None, 'a', None)

    @skip_if_py2
    def test_remove_not_implement(self):
        with self.assertRaises(NotImplementedError):
            self._cls._remove(None, 'a')

    def test_get_exists(self):
        inst = self._higher_test_cls()
        key = 'key_a'
        result = inst.get(key)

        inst._get.assert_called_once_with(key, self._default)
        inst._decode.assert_called_once_with(inst._get.return_value, None)
        self.assertIs(result, inst._decode.return_value)

    def test_get_not_exist_no_default(self):
        inst = self._higher_test_cls()
        inst._get.side_effect = lambda key, default: default
        key = 'key_a'
        result = inst.get(key)

        inst._get.assert_called_once_with(key, self._default)
        self.assertIs(result, None)

    def test_get_not_exist_with_default(self):
        inst = self._higher_test_cls()
        inst._get.return_value = self._default
        key = 'key_a'
        default = mock.Mock()
        result = inst.get(key, default)

        inst._get.assert_called_once_with(key, self._default)
        self.assertIs(result, default)

    def test_get_invalikey_d(self):
        inst = self._higher_test_cls()
        inputs = [4, None, []]
        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.get(input_)
            inst._get.assert_not_called()

    def test_get_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _get(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.get('key_a')
        except cache.CacheError:
            self.fail("CacheError was raised")

    def test_get_many_all_exists(self):
        inst = self._higher_test_cls()
        keys = ['key_a', 'key_b']
        values = (mock.Mock(name='value_a'), mock.Mock('value_b'))
        inst._get_many.return_value = values
        result = inst.get_many(keys)

        inst._get_many.assert_called_once_with(keys, self._default)
        inst._decode.assert_has_calls([mock.call(v, None) for v in values])
        self.assertEqual(result, {
            'key_a': inst._decode.return_value,
            'key_b': inst._decode.return_value,
        })

    def test_get_many_not_exist_no_default(self):
        inst = self._higher_test_cls()
        keys = ['key_a', 'key_b']
        inst._get_many.return_value = [self._default, self._default]
        result = inst.get_many(keys)

        inst._get_many.assert_called_once_with(keys, self._default)
        inst._decode.assert_not_called()
        self.assertEqual(result, {'key_a': None, 'key_b': None})

    def test_get_many_not_exist_with_default(self):
        inst = self._higher_test_cls()
        keys = ['key_a', 'key_b']
        default = mock.Mock(name='default')
        inst._get_many.return_value = [self._default, self._default]
        result = inst.get_many(keys, default)

        inst._get_many.assert_called_once_with(keys, self._default)
        inst._decode.assert_not_called()
        self.assertEqual(result, {'key_a': default, 'key_b': default})

    def test_get_many_some_not_exist_no_default(self):
        inst = self._higher_test_cls()
        keys = ['key_a', 'key_b', 'key_c', 'key_d']
        value_1, value_2 = mock.Mock(name='val_1'), mock.Mock(name='val_2')
        inst._get_many.return_value = [
                self._default, value_1, value_2, self._default]
        result = inst.get_many(keys)

        inst._get_many.assert_called_once_with(keys, self._default)
        inst._decode.assert_has_calls(
                [mock.call(value_1, None), mock.call(value_2, None)])
        self.assertEqual(result, {
            'key_a': None,
            'key_b': inst._decode.return_value,
            'key_c': inst._decode.return_value,
            'key_d': None,
        })

    def test_get_many_some_not_exist_with_default(self):
        inst = self._higher_test_cls()
        keys = ['key_a', 'key_b', 'key_c', 'key_d']
        value_1, value_2 = mock.Mock(name='val_1'), mock.Mock(name='val_2')
        default = mock.Mock(name='default')
        inst._get_many.return_value = [
                self._default, value_1, value_2, self._default]
        result = inst.get_many(keys, default)

        inst._get_many.assert_called_once_with(keys, self._default)
        inst._decode.assert_has_calls(
                [mock.call(value_1, default), mock.call(value_2, default)])
        self.assertEqual(result, {
            'key_a': default,
            'key_b': inst._decode.return_value,
            'key_c': inst._decode.return_value,
            'key_d': default,
        })

    def test_get_many_invalikey_ds(self):
        inst = self._higher_test_cls()
        inputs = [
            [1], [mock.Mock(name='not_a_string')], [None],
            ['a', None], ['a', 'b', None, 4],
            [4, 5, 1, None, 'a', 'b', 45, mock.Mock(), object()]
        ]

        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.get_many(input_)

            inst._get_many.assert_not_called()

    def test_get_many_empty_keys(self):
        inst = self._higher_test_cls()
        result = inst.get_many([])

        inst._get_many.assert_not_called()
        self.assertEqual(result, {})

    def test_get_many_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _get_many(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.get_many('key_a')
        except cache.CacheError:
            self.fail("CacheError was raised")

    def test_remove(self):
        inst = self._higher_test_cls()
        result = inst.remove('key_a')

        inst._remove.assert_called_once_with('key_a')
        self.assertIs(result, inst._remove.return_value)

    def test_remove_bad_key(self):
        inst = self._higher_test_cls()
        inputs = [35.5, object(), None, []]
        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.remove(input_)
            inst._get.assert_not_called()

    def test_remove_many(self):
        inst = self._higher_test_cls()
        result = inst.remove_many(['key_a', 'key_b'])

        inst._remove_many.assert_called_once_with(['key_a', 'key_b'])
        self.assertIs(result, inst._remove_many.return_value)

    def test_remove_many_no_keys(self):
        inst = self._higher_test_cls()
        result = inst.remove_many([])

        inst._remove_many.assert_not_called()
        self.assertIs(result, 0)

    def test_remove_many_bad_keys(self):
        inst = self._higher_test_cls()
        inputs = [
            [43.2], [mock.Mock()], [None], [False],
            [4,1245, 'ae', None], [object(), object(), 3, 4, 'a', 'fwef32r'],
        ]
        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.remove_many(input_)
            inst._remove_many.assert_not_called()

    def test_remove_many_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _remove_many(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.remove_many('key_a')
        except cache.CacheError:
            self.fail("CacheError was raised")

    def test_remove_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _remove(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.remove('key_a')
        except cache.CacheError:
            self.fail("CacheError was raised")

    def test_getitem_exists(self):
        class TestClass(self._higher_test_cls):
            get = mock.Mock(name='get')

        inst = TestClass()

        result = inst['key_a']

        inst.get.assert_called_once_with('key_a', self._default)
        self.assertIs(result, inst.get.return_value)

    def test_getitem_not_exist(self):
        class TestClass(self._higher_test_cls):
            get = mock.Mock(name='get')
            get.return_value = self._default

        inst = TestClass()

        with self.assertRaises(KeyError):
            inst['key_a']

        inst.get.assert_called_once_with('key_a', self._default)

    def test_delitem_exists(self):
        class TestClass(self._higher_test_cls):
            remove = mock.Mock(name='remove')

        inst = TestClass()

        del inst['key_a']

        inst.remove.assert_called_once_with('key_a')

    def test_delitem_not_exist(self):
        class TestClass(self._higher_test_cls):
            remove = mock.Mock(name='remove')
            remove.return_value = False

        inst = TestClass()

        with self.assertRaises(KeyError):
            del inst['key_a']

        inst.remove.assert_called_once_with('key_a')

    def test_get_many_default_handler(self):
        inst = self._test_cls()

        default = mock.Mock(name='default')
        result = inst._get_many(['key_a', 'key_b'], default)

        inst._get.assert_has_calls([
            mock.call('key_a', default), mock.call('key_b', default),
        ])
        self.assertEqual(result,
                [inst._get.return_value, inst._get.return_value])

    def test_remove_many_default_handler(self):
        inst = self._test_cls()

        inst._remove.side_effect = [
            True, True, False, True, False, False, False, True,
        ]
        # 8 total, 4 true expected count is 4
        keys = ['key_1', 'key_2', 'key_3', 'key_4', 'key_5',
                'key_6', 'key_7', 'key_8']

        result = inst._remove_many(keys)

        inst._remove.assert_has_calls([mock.call(i) for i in keys])
        self.assertEqual(result, 4)

    def test_encode_no_serializer_no_compressor(self):
        inst = self._test_cls()

        input = mock.Mock(name='input')
        result = inst._encode(input)

        self.assertIs(input, result)

    def test_encode_no_compressor(self):
        class TestClass(self._test_cls):
            serializer = mock.Mock(name='serializer')

        inst = TestClass()
        serialize_cb = TestClass.serializer.serialize

        input = mock.Mock(name='input')
        result = inst._encode(input)

        self.assertIs(serialize_cb.return_value, result)

    def test_encode_no_serializer(self):
        class TestClass(self._test_cls):
            compressor = mock.Mock(name='compressor')

        inst = TestClass()
        compress_cb = TestClass.compressor.compress

        input = mock.Mock(name='input')
        result = inst._encode(input)

        self.assertIs(result, input)
        compress_cb.assert_not_called()

    def test_encode(self):
        class TestClass(self._test_cls):
            compressor = mock.Mock(name='compressor')
            serializer = mock.Mock(name='serializer')

        inst = TestClass()
        compress_cb = TestClass.compressor.compress
        serialize_cb = TestClass.serializer.serialize

        input = mock.Mock(name='input')
        result = inst._encode(input)

        serialize_cb.assert_called_once_with(input)
        compress_cb.assert_called_once_with(serialize_cb.return_value)
        self.assertIs(result, compress_cb.return_value)

    def test_decode_not_serializer_no_compressor(self):
        inst = self._test_cls()

        input = mock.Mock(name='input')
        result = inst._encode(input)

        self.assertIs(input, result)

    def test_decode_no_compressor(self):
        class TestClass(self._test_cls):
            serializer = mock.Mock(name='serializer')

        inst = TestClass()
        deserialize_cb = TestClass.serializer.deserialize

        input = mock.Mock(name='input')
        result = inst._decode(input)

        self.assertIs(deserialize_cb.return_value, result)

    def test_decode_no_serializer(self):
        class TestClass(self._test_cls):
            compressor = mock.Mock(name='compressor')

        inst = TestClass()
        decompress_cb = TestClass.compressor.decompress

        input = mock.Mock(name='input')
        result = inst._decode(input)

        self.assertIs(result, input)
        decompress_cb.assert_not_called()

    def test_decode(self):
        class TestClass(self._test_cls):
            compressor = mock.Mock(name='compressor')
            serializer = mock.Mock(name='serializer')

        inst = TestClass()
        decompress_cb = TestClass.compressor.decompress
        deserialize_cb = TestClass.serializer.deserialize

        input = mock.Mock(name='input')
        result = inst._decode(input)

        decompress_cb.assert_called_once_with(input)
        deserialize_cb.assert_called_once_with(decompress_cb.return_value)
        self.assertIs(result, deserialize_cb.return_value)

    def test_decode_decompress_error_raise(self):
        the_error = cache.CacheDecodeError(TypeError("TEST ERROR"))
        def raiser(*args, **kwargs):
            raise the_error

        class TestClass(self._test_cls):
            compressor = mock.Mock(name='compressor')
            serializer = mock.Mock(name='serializer')

            compressor.decompress.side_effect = raiser

        input = mock.Mock(name='input')

        inst = TestClass()
        with self.assertRaises(cache.CacheDecodeError) as cm:
            inst._decode(input)

        self.assertIs(cm.exception, the_error)

    def test_decode_decompress_error_noraise(self):
        the_error = cache.CacheDecodeError(TypeError("TEST ERROR"))
        def raiser(*args, **kwargs):
            raise the_error

        class TestClass(self._test_cls):
            compressor = mock.Mock(name='compressor')
            serializer = mock.Mock(name='serializer')

            compressor.decompress.side_effect = raiser

        input = mock.Mock(name='input')
        default = mock.Mock(name='default')

        inst = TestClass()
        try:
            result = inst._decode(input, default)
        except cache.CacheDecodeError:
            self.fail("CacheDecodeError was raised")

        self.assertIs(result, default)

    def test_decode_deserialize_error_raise(self):
        the_error = cache.CacheDecodeError(ValueError("TEST ERROR"))
        def raiser(*args, **kwargs):
            raise the_error

        class TestClass(self._test_cls):
            compressor = None
            serializer = mock.Mock(name='serializer')

            serializer.deserialize.side_effect = raiser

        input = mock.Mock(name='input')

        inst = TestClass()
        with self.assertRaises(cache.CacheDecodeError) as cm:
            inst._decode(input)

        self.assertIs(cm.exception, the_error)

    def test_decode_deserialize_error_noraise(self):
        the_error = cache.CacheDecodeError(ValueError("TEST ERROR"))
        def raiser(*args, **kwargs):
            raise the_error

        class TestClass(self._test_cls):
            compressor = None
            serializer = mock.Mock(name='serializer')

            serializer.deserialize.side_effect = raiser

        input = mock.Mock(name='input')
        default = mock.Mock(name='default')

        inst = TestClass()
        try:
            result = inst._decode(input, default)
        except cache.CacheDecodeError:
            self.fail("CacheDecodeError was raised")

        self.assertIs(result, default)


class TestBaseNoTTLCache(TestBaseCache):
    _cls = cache.BaseNoTTLCache

    def setUp(self):
        class TestClass(self._cls):
            _set = mock.Mock(name='_set')
            _get = mock.Mock(name='_get')
            _remove = mock.Mock(name='_remove')

        # Some tests need some more methods mocked.
        class HigherTestClass(TestClass):
            _encode = mock.Mock(name='_encode')
            _decode = mock.Mock(name='_decode')
            _get_many = mock.Mock(name='_get_many')
            _remove_many = mock.Mock(name='_remove_many')
            _set_many = mock.Mock(name='_set_many')

        self._test_cls = TestClass
        self._higher_test_cls = HigherTestClass

    def tearDown(self):
        del self._test_cls
        del self._higher_test_cls

    @skip_if_py2
    def test_set_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self._cls._set(None, 'key_a', object())

    def test_set(self):
        inst = self._higher_test_cls()
        key, val = 'key_a', mock.Mock(name='val')
        result = inst.set(key, val)

        inst._encode.assert_called_once_with(val)
        inst._set.assert_called_once_with(key, inst._encode.return_value)
        self.assertIs(result, None)

    def test_set_bad_key(self):
        inst = self._higher_test_cls()
        inputs = [1, 5.5, None, object(), mock.Mock()]
        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.set(input_, object())
            inst._set.assert_not_called()

    def test_set_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _set(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.set("key", mock.Mock(name="val"))
        except cache.CacheError:
            self.fail("CacheError was raised")

    def test_set_many_none(self):
        inst = self._higher_test_cls()

        result = inst.set_many()

        inst._set_many.assert_not_called()
        inst._encode.assert_not_called()
        self.assertIs(result, None)

    def test_set_many_dict_sequence(self):
        inst = self._higher_test_cls()

        input = {
            'key_a': mock.Mock(name='object_a'),
            'key_b': mock.Mock(name='object_b'),
            'key_c': mock.Mock(name='object_c'),
        }
        result = inst.set_many(input)

        inst._encode.assert_has_calls([
                mock.call(i) for i in input.values()
            ],
            any_order=True,
        )
        inst._set_many.assert_called_once_with({
            'key_a': inst._encode.return_value,
            'key_b': inst._encode.return_value,
            'key_c': inst._encode.return_value,
        })
        self.assertIs(result, None)

    def test_set_many_list_sequence(self):
        inst = self._higher_test_cls()

        input = [
            ('key_a', mock.Mock(name='object_a')),
            ('key_b', mock.Mock(name='object_b')),
            ('key_c', mock.Mock(name='object_c')),
        ]
        result = inst.set_many(input)

        inst._encode.assert_has_calls([
                mock.call(i[1]) for i in input
            ],
            any_order=True,
        )
        inst._set_many.assert_called_once_with({
            'key_a': inst._encode.return_value,
            'key_b': inst._encode.return_value,
            'key_c': inst._encode.return_value,
        })
        self.assertIs(result, None)

    def test_set_many_kwargs(self):
        inst = self._higher_test_cls()

        input_kwargs = {
            'key_a': mock.Mock(name='object_a'),
            'key_b': mock.Mock(name='object_b'),
            'key_c': mock.Mock(name='object_c'),
        }
        result = inst.set_many(**input_kwargs)

        inst._encode.assert_has_calls([
                mock.call(i) for i in input_kwargs.values()
            ],
            any_order=True,
        )
        inst._set_many.assert_called_once_with({
            'key_a': inst._encode.return_value,
            'key_b': inst._encode.return_value,
            'key_c': inst._encode.return_value,
        })
        self.assertIs(result, None)

    def test_set_many_sequence_and_kwargs(self):
        inst = self._higher_test_cls()

        input_sequence = {
            'key_a': mock.Mock(name='object_a'),
            'key_b': mock.Mock(name='object_b'),
        }
        input_kwargs = {
            'key_c': mock.Mock(name='object_c'),
            'key_d': mock.Mock(name='object_d'),
        }

        result = inst.set_many(input_sequence, **input_kwargs)

        inst._encode.assert_has_calls([
                mock.call(i) for i in input_sequence.values()
            ] + [
                mock.call(i) for i in input_kwargs.values()
            ],
            any_order=True,
        )
        inst._set_many.assert_called_once_with({
            'key_a': inst._encode.return_value,
            'key_b': inst._encode.return_value,
            'key_c': inst._encode.return_value,
            'key_d': inst._encode.return_value,
        })
        self.assertIs(result, None)

    def test_set_many_invalid_sequence_types(self):
        inst = self._higher_test_cls()

        input_sequence = 4.3

        with self.assertRaises(TypeError):
            inst.set_many(input_sequence)

    def test_set_many_invalid_sequence_value(self):
        inst = self._higher_test_cls()

        input_sequence = [('too', 'many', 'variables!')]

        with self.assertRaises(ValueError):
            inst.set_many(input_sequence)

    def test_set_many_invalikey_ds(self):
        inst = self._higher_test_cls()

        input_sequence = [('a', 'banana'), (4, 'wrong')]

        with self.assertRaises(TypeError):
            inst.set_many(input_sequence)

    def test_set_many_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _set_many(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.set_many({"key_a": mock.Mock(name="val")})
        except cache.CacheError:
            self.fail("CacheError was raised")

    def test_set_item(self):
        class TestClass(self._test_cls):
            set = mock.Mock()

        inst = TestClass()

        key, val = mock.Mock(name='key'), mock.Mock(name='val')

        inst[key] = val

        inst.set.assert_called_once_with(key, val)

    def test_set_many_internal(self):
        inst = self._test_cls()

        input = {
            'key_a': mock.Mock(name='object_a'),
            'key_b': mock.Mock(name='object_b'),
            'key_c': mock.Mock(name='object_c'),
        }
        result = inst._set_many(input)

        inst._set.assert_has_calls([
                mock.call(k, v) for k, v in input.items()
            ],
            any_order=True,
        )
        self.assertIs(result, None)


class TestBaseTTLCache(TestBaseCache):
    _cls = cache.BaseTTLCache

    def setUp(self):
        class TestClass(self._cls):
            _set = mock.Mock(name='_set')
            _get = mock.Mock(name='_get')
            _remove = mock.Mock(name='_remove')

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
    def test_set_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self._cls._set(None, 'key_a', object(), 3)

    def test_set(self):
        inst = self._higher_test_cls()

        key, val, ttl = 'key_a', mock.Mock(name='object_a'), 1

        result = inst.set(key, val, ttl)

        inst._encode.assert_called_once_with(val)
        inst._set.assert_called_once_with(key, inst._encode.return_value, 1.0)
        self.assertIs(result, None)

    def test_set_no_ttl(self):
        inst = self._higher_test_cls()

        key, val, ttl = 'key', mock.Mock(name='object'), None

        result = inst.set(key, val, ttl)

        inst._encode.assert_called_once_with(val)
        inst._set.assert_called_once_with(key, inst._encode.return_value, None)
        self.assertIs(result, None)

    def test_bad_key(self):
        inst = self._higher_test_cls()

        inputs = [3, None, object(), mock.Mock(), 64.1]
        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.set(input_, mock.Mock(), 1)
            inst._encode.assert_not_called()
            inst._set.assert_not_called()

    def test_bad_ttl_type(self):
        inst = self._higher_test_cls()

        inputs = [object(), None, mock.Mock()]
        for input_ in inputs:
            with self.assertRaises(TypeError):
                inst.set('key_a', mock.Mock(), input)
            inst._encode.assert_not_called()
            inst._set.assert_not_called()

    def test_bad_ttl_value(self):
        inst = self._higher_test_cls()

        with self.assertRaises(ValueError):
            inst.set('key_a', mock.Mock(), 'hello!')
        inst._encode.assert_not_called()
        inst._set.assert_not_called()


    def test_set_cache_error(self):
        class TestClass(self._higher_test_cls):
            def _set(*args, **kwargs):
                raise cache.CacheError("TEST")
        inst = TestClass()

        try:
            inst.set("key_a", mock.Mock(name="val"), 1)
        except cache.CacheError:
            self.fail("CacheError was raised")
