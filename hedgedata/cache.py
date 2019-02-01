import pandas as pd
from .define import MARKET_DATA_FIELDS
from .utils import last_year
import pyEX


class Cache(object):
    def __init__(self, cache_type='NONE'):
        self._cache_type = cache_type.upper()
        self._cache = {k: pd.DataFrame() for k in MARKET_DATA_FIELDS}
        self._client = pyEX.Client()

    def daily(self,
              symbols,
              from_=pd.Timestamp(last_year()),
              count=0):
        dat = self._cache['DAILY']
        symbols = [_.upper() for _ in symbols]
        index_fields = ['date', 'symbol']

        # drop existing index
        dat.reset_index(inplace=True)

        if dat.empty and self._cache_type == 'ARCTIC':
            # read from arctic
            raise NotImplementedError()

        # symbols missing
        fetch = set(symbols) if dat.empty else set(symbols) - set(dat['symbol'].unique())

        # symbols missing certain dates
        if from_ and not dat.empty:
            fetch = fetch.union(set(symbols) if not (dat['date'] == from_).any() else
                                set(symbols) - set(dat[dat['date'] == from_]['symbol'].unique()))
        elif count:
            raise NotImplementedError()

        # fetch new data
        df_dict = self.fetch(list(fetch), ['chart']) if len(fetch) else {}

        # join in new data
        for k in df_dict:
            self._cache['DAILY'] = self.merge(dat, df_dict[k], index_fields)

        # reset index to previous
        self._cache['DAILY'].set_index(index_fields, inplace=True)

        # sort on original indexes
        self._cache['DAILY'].sort_index(level=self._cache['DAILY'].index.names[::-1])

        return self._cache['DAILY']

    def fetch(self, symbols, fields):
        print('fetching %s %s' % (symbols, fields))
        df_dict = pyEX.batchDF(symbols, fields, range_='1y')  # TODO port

        for k in df_dict:
            df = df_dict[k]
            df.reset_index(inplace=True)

        return df_dict

    def merge(self, df1, df2, index_fields):
        # merge on new data
        ret = pd.merge(df1, df2, copy=False, how='outer') if not df1.empty else df2

        # remove duplicate records
        ret.drop_duplicates(index_fields, inplace=True)
        return ret
