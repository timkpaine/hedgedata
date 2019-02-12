import pandas as pd
from .define import ALL_FIELDS
from .utils import last_year
import pyEX


class Cache(object):
    def __init__(self, cache_type='NONE'):
        self._cache_type = cache_type.upper()
        self._cache = {k: pd.DataFrame() for k in ALL_FIELDS}
        self._client = pyEX.Client()

    def daily(self,
              symbols,
              from_=pd.Timestamp(last_year())):
        return self._get(symbols, 'DAILY', ['date', 'symbol'], from_, 0, True)

    def _get(self,
             symbols,
             cachename,
             index_fields,
             from_=None,
             count=0,
             dict_=False):
        dat = self._cache[cachename]
        symbols = [_.upper() for _ in symbols]

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
        elif count and not dat.empty:
            counts = dat['symbol'].value_counts() < count
            fetch = fetch.union(set(dat[dat['symbol'].isin(counts[counts])]['symbol'].unique()))

        # fetch new data
        if cachename == 'CASHFLOW':
            df = self._cashflow(list(fetch)) if len(fetch) else pd.DataFrame()
        elif cachename == 'DAILY':
            # fetch new data
            df_dict = self.fetch(list(fetch), ['chart']) if len(fetch) else {}

        if dict_:
            # join in new data
            for k in df_dict:
                if not df_dict[k].empty:
                    self._cache[cachename] = self.merge(dat, df_dict[k], index_fields)
        else:
            # join in new data
            if not df.empty:
                self._cache[cachename] = self.merge(dat, df, index_fields)

        # reset index to previous
        self._cache[cachename].set_index(index_fields, inplace=True)

        # sort on original indexes
        self._cache[cachename].sortlevel()

        return self._cache[cachename].loc[(slice(None), symbols), :]

    def cashflow(self, symbols):
        return self._get(symbols, 'CASHFLOW', ['reportDate', 'symbol'], None, 1)

    def fetch(self, symbols, fields):
        print('fetching %s %s' % (symbols, fields))
        df_dict = pyEX.batchDF(symbols, fields, range_='1y')  # TODO port

        for k in df_dict:
            df = df_dict[k]
            df.reset_index(inplace=True)

        return df_dict

    def _cashflow(self, symbols):
        print('fetching %s %s' % (symbols, 'cashflow'))
        df = pd.concat([self._client.cashFlowDF(symbol) for symbol in symbols])
        df.reset_index(inplace=True)
        return df

    def merge(self, df1, df2, index_fields):
        # merge on new data
        ret = pd.merge(df1, df2, copy=False, how='outer') if not df1.empty else df2

        # remove duplicate records
        ret.drop_duplicates(index_fields, inplace=True)
        return ret
