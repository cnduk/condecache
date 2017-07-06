from datetime import datetime, timedelta
import json


from ._six import timezone


__ALL__ = ('json_object_hook', 'JSONEncoder')


def json_object_hook(data):
    if '__conde_item_type__' not in data:
        return data

    type_ = data['__conde_item_type__']

    if type_ == 'datetime':
        if data['tzinfo'] is not None:
            tzinfo=_tz_from_seconds(**data['tzinfo'])
        else:
            tzinfo = None
        dt = datetime(tzinfo=tzinfo, **data['dtinfo'])
        return dt

    raise ValueError("Got unexpected __conde_item_type__ '{}'".format(type_))


class JSONEncoder(json.JSONEncoder):
    def default(self, data):
        if isinstance(data, datetime):
            dtinfo = {
                k: getattr(data, k)
                for k in ('year', 'month', 'day', 'hour', 'minute',
                    'second', 'microsecond')
            }
            if data.tzinfo:
                tzinfo = {
                    'tzname': data.tzname(),
                    'seconds': data.utcoffset().total_seconds(),
                }
            else:
                tzinfo = None

            return {
                '__conde_item_type__': 'datetime',
                'dtinfo': dtinfo,
                'tzinfo': tzinfo,
            }

        return super(JSONEncoder, self).default(data)


_tz_cache = {}
def _tz_from_seconds(seconds, tzname):
    try:
        return _tz_cache[seconds,tzname]
    except KeyError:
        _tz_cache[seconds,tzname] = tz = timezone(
                timedelta(seconds=seconds), name=tzname)
        return tz

