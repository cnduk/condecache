class CacheError(Exception):
    pass

class RemoteCacheCommError(CacheError):
    pass


class CacheDecodeError(CacheError):
    def __init__(self, from_err):
        self.from_err = from_err
        msg = '<{}: "{}: {}">'.format(
            self.__class__.__name__,
            from_err.__class__.__name__,
            from_err
        )
        super(CacheDecodeError, self).__init__(msg)

