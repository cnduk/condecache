# Copyright (c) 2017 Conde Nast Britain
#
# Some parts have been copied from original six package by Benjamin Peterson,
# and credit has been noted below.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


# Rather than depending on the entirety of the six backport lib,
# store our own backports here. Also add a few extras.

import sys

try:
    from datetime import timezone
except ImportError:
    from datetime import tzinfo

    class timezone(tzinfo):
        def __init__(self, offset, name=None):
            self._tzname = name
            self._offset = offset

        def utcoffset(self, dt):
            return self._offset

        def tzname(self, dt):
            return self._tzname

        def dst(self, dt):
            return None

        def __repr__(self):
            return "<{}({}, {})>".format(self.__class__.__name__,
                    self._tzname, self._offset)

    del tzinfo


#
# Below snippets are taken from or adapted from the original
# six package by Benjamin Peterson
# Copyright (c) 2010-2015 Benjamin Peterson
#
if sys.version_info[0] == 2:
    string_types = basestring,
else:
    string_types = str,

def add_metaclass(metaclass):
    """Class decorator for creating a class with a metaclass."""
    def wrapper(cls):
        orig_vars = cls.__dict__.copy()
        slots = orig_vars.get('__slots__')
        if slots is not None:
            if isinstance(slots, str):
                slots = [slots]
            for slots_var in slots:
                orig_vars.pop(slots_var)
        orig_vars.pop('__dict__', None)
        orig_vars.pop('__weakref__', None)
        return metaclass(cls.__name__, cls.__bases__, orig_vars)
    return wrapper
