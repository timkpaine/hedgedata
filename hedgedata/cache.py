import pyEX
from functools import lru_cache
from .data import Data
from .define import ALL_FIELDS
from .transform import whichTransform


class Cache(object):
    def __init__(self, host='localhost', offline=False):
        self.data = Data(host, offline=offline)

    @lru_cache(100)
    def _fetch(self, symbol, field):
        return self._fetch_nocache(symbol, field)

    def _fetch_nocache(self, symbol, field):
        field = field.upper()
        symbol = symbol.upper()

        if field != 'ALL' and field not in ALL_FIELDS:
            raise Exception('Unknown field %s' % field)

        if field == 'ALL':
            fields = ALL_FIELDS
        else:
            fields = [field]

        ret = {}
        for field in fields:
            df = self._fetchDF_nocache(symbol, field)
            ret[field] = whichTransform(field)(df, symbol, self.data)
        return ret

    def fetch(self, symbol, field, cache=True):
        if cache:
            return self._fetch(symbol, field)
        return self._fetch_nocache(symbol, field)

    @lru_cache(100)
    def _fetchDF(self, symbol, field):
        return self._fetchDF_nocache(symbol, field)

    def _fetchDF_nocache(self, symbol, field):
        field = field.upper()
        symbol = symbol.upper()

        if field == 'ALL' and field not in ALL_FIELDS:
            raise Exception('Unknown or Invalid field %s' % field)

        return self.data.read(symbol, field)

    def fetchDF(self, symbol, field, cache=True):
        if cache:
            return self._fetchDF(symbol, field)
        return self._fetchDF_nocache(symbol, field)

    @lru_cache(1)
    def tickers(self):
        return pyEX.symbolsDF()
