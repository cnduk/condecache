import datetime
from unittest import TestCase
import zlib
import json
import pickle

import mock

from condecache import cache

class TestZLibCompressor(TestCase):
    _cls = cache.ZLibCompressor

    def test_compress(self):
        input = b'Hello, world!'
        result = self._cls.compress(input)

        self.assertIsInstance(result, type(b''))
        self.assertEqual(result, zlib.compress(b'Hello, world!'))

    def test_compress_string(self):
        input = 'Hello, world!'
        result = self._cls.compress(input)

        self.assertIsInstance(result, type(b''))
        self.assertEqual(result, zlib.compress(b'Hello, world!'))

    def test_decompress(self):
        input = zlib.compress(b'hello, world!')
        result = self._cls.decompress(input)

        self.assertEqual(result, b'hello, world!')

    @mock.patch('condecache.cache.zlib')
    def test_decompress_type_error(self, mock_zlib):
        the_error = TypeError("TEST ERROR")
        def raiser(*args, **kwargs):
            raise the_error
        mock_zlib.decompress.side_effect = raiser
        # put this back.
        mock_zlib.error = zlib.error

        input = mock.Mock(name='compressed')

        with self.assertRaises(cache.CacheDecodeError) as cm:
            self._cls.decompress(input)

        self.assertIs(cm.exception.from_err, the_error)

    @mock.patch('condecache.cache.zlib')
    def test_decompress_value_error(self, mock_zlib):
        the_error = ValueError("TEST ERROR")
        def raiser(*args, **kwargs):
            raise the_error
        mock_zlib.decompress.side_effect = raiser
        # put this back.
        mock_zlib.error = zlib.error

        input = mock.Mock(name='compressed')

        with self.assertRaises(cache.CacheDecodeError) as cm:
            self._cls.decompress(input)

        self.assertIs(cm.exception.from_err, the_error)

    @mock.patch('condecache.cache.zlib')
    def test_decompress_zlib_error(self, mock_zlib):
        the_error = zlib.error("TEST ERROR")
        def raiser(*args, **kwargs):
            raise the_error
        mock_zlib.decompress.side_effect = raiser
        # put this back.
        mock_zlib.error = zlib.error

        input = mock.Mock(name='compressed')

        with self.assertRaises(cache.CacheDecodeError) as cm:
            self._cls.decompress(input)

        self.assertIs(cm.exception.from_err, the_error)


class TestJSONSerializer(TestCase):
    _cls = cache.JSONSerializer

    def test_serialize_simple(self):
        input = {
            'a': 'value',
            'another': 'value',
        }
        result = self._cls.serialize(input)

        self.assertIsInstance(result, type(''))
        self.assertEqual(result, json.dumps(input))
        self.assertEqual(input, json.loads(result))

    def test_serialize_datetime_nofail(self):
        input = datetime.datetime(2000, 1, 1, 12, 24, 35)
        result = self._cls.serialize(input)

        self.assertIsInstance(result, type(''))

    def test_serialize_datetime_with_timezone_nofail(self):
        try:
            tz = datetime.timezone
        except AttributeError:
            self.skipTest("Cannot find datetime timezone (py2?)")

        input = datetime.datetime(
            2000, 1, 1, 12, 24, 35, tzinfo=tz(datetime.timedelta(hours=1)),
        )
        result = self._cls.serialize(input)

        self.assertIsInstance(result, type(''))

    def test_serialize_and_deserialize_datetime(self):
        input = datetime.datetime(2014, 1, 4, 14, 53)
        intermediate_result = self._cls.serialize(input)
        result = self._cls.deserialize(intermediate_result)

        self.assertEqual(input, result)

    def test_serialize_and_deserialize_datetime_with_timezone(self):
        try:
            tz = datetime.timezone
        except AttributeError:
            self.skipTest("Cannot find datetime timezone (py2?)")

        input = datetime.datetime(
            2000, 1, 1, 12, 24, 35, tzinfo=tz(datetime.timedelta(hours=1)),
        )
        intermediate_result = self._cls.serialize(input)
        result = self._cls.deserialize(intermediate_result)

        self.assertEqual(input, result)

    def _test_serialize_keeping_dict_key_types(self):
        # TODO correct this behaviour and put the test back?

        # because json will take a dict like {1:1} and change it to
        # '{"1":1}' which gets turned back to {'1': 1}.
        input = {1:None, 2.4:None, True:None, False:None, None:None}
        intermediate_result = self._cls.serialize(input)
        result = self._cls.deserialize(intermediate_result)

        self.assertEqual(input, result)

    def test_decode_type_error_raises_cache_error(self):
        the_error = TypeError("TEST CATCHME")
        def raiser(*args, **kwargs):
            raise the_error
        class TestCls(self._cls):
            _json_decoder = mock.Mock()
            _json_decoder.decode.side_effect = raiser

        input = mock.Mock(name='serialized')

        with self.assertRaises(cache.CacheDecodeError) as cm:
            TestCls.deserialize(input)

        self.assertIs(cm.exception.from_err, the_error)

    def test_decode_value_error_raises_cache_error(self):
        the_error = ValueError("TEST CATCHME")
        def raiser(*args, **kwargs):
            raise the_error
        class TestCls(self._cls):
            _json_decoder = mock.Mock()
            _json_decoder.decode.side_effect = raiser

        input = mock.Mock()

        with self.assertRaises(cache.CacheDecodeError) as cm:
            TestCls.deserialize(input)

        self.assertIs(cm.exception.from_err, the_error)


class TestPickleSerializer(TestCase):
    _cls = cache.PickleSerializer

    def test_serialize_and_deserialize(self):
        input = {
            'a': 'b',
            'a': ['a', 'b', 'c', 'd'],
            1: datetime.datetime(2014, 4, 2, 5, 1),
            None: {True: None, False:{4.3:None}},
        }

        intermediate = self._cls.serialize(input)
        result = self._cls.deserialize(intermediate)

        self.assertEqual(input, result)

    def test_serialize_and_deserialize_with_timezone(self):
        try:
            tz = datetime.timezone
        except AttributeError:
            self.skipTest("Cannot find datetime timezone (py2?)")

        input = datetime.datetime(
            2017, 1, 20, 12, 54,
            tzinfo=tz(datetime.timedelta(hours=-3))
        )

        intermediate = self._cls.serialize(input)
        result = self._cls.deserialize(intermediate)

        self.assertEqual(input, result)

    @mock.patch('condecache.cache.pickle')
    def _test_deserialize_error(self, exception, mock_pickle):
        def raiser(*args, **kwargs):
            raise exception

        mock_pickle.loads.side_effect = raiser
        # Put this back
        mock_pickle.UnpicklingError = pickle.UnpicklingError

        input = mock.Mock(name='input')

        with self.assertRaises(cache.CacheDecodeError) as cm:
            self._cls.deserialize(input)

        self.assertIs(cm.exception.from_err, exception)

    def test_deserialize_eoferror(self):
        self._test_deserialize_error(EOFError("TEST ERROR"))

    def test_deserialize_type_error(self):
        self._test_deserialize_error(TypeError("TEST ERROR"))

    def test_deserialize_value_error(self):
        self._test_deserialize_error(ValueError("TEST ERROR"))

    def test_deserialize_unpickling_error(self):
        self._test_deserialize_error(pickle.UnpicklingError("TEST ERROR"))
