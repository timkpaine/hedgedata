import pyEX
from arctic import Arctic
from .data import Data
from .define import ALL_FIELDS
from .transform import whichTransform


class Cache(object):
    def __init__(self, host='localhost'):
        self.data = Data(Arctic(host))

    def fetch(self, symbol, field):
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
            df = self.fetchDF(symbol, field)
            ret[field] = whichTransform(field)(df, symbol, self.data)
        return ret

    def fetchDF(self, symbol, field):
        field = field.upper()
        symbol = symbol.upper()

        if field == 'ALL' and field not in ALL_FIELDS:
            raise Exception('Unknown or Invalid field %s' % field)

        return self.data.read(symbol, field)

    def tickers(self):
        return pyEX.symbolsDF()
