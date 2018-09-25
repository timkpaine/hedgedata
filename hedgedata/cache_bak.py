import copy
import os
import os.path
import numpy as np
import pandas as pd
import pyEX as p
import requests
import sys
from tqdm import tqdm
from urllib.error import HTTPError
from multiprocessing.pool import ThreadPool
from functools import partial
from datetime import datetime
from .define import FIELDS, OTHERS, ETF_URL, POPULAR_ETFS_URL
from .utils import log, logging, today, yesterday

# a threadpool should be fine as the biggest
# delay is the HTTP request, which should
# release GIL
#
# if porting to process pool, should pipe
# records in between as opposed to pickling
# cache object
_POOL = ThreadPool(len(FIELDS)-1)


def fetch(key, cache, field):
    # fields always lower
    field = field.lower()
    if key not in cache._cache or field not in cache._cache[key] and cache._cache[key]['timestamp'].get(field, yesterday()) < today():
        log.info('fetching %s for %s' % (field, key))
        cache.fetchDF(key, field)
    else:
        log.info('skipping %s for %s' % (field, key))


class Cache(object):
    def __init__(self, tickers=None):
        self._cache = {}
        self._tickers = tickers if tickers is not None and not tickers.empty else p.symbolsDF()
        self._tickers_ts = datetime.now()

        self._others = {}
        self._others_ts = {}

    def tickers(self):
        if self._tickers_ts < today():
            self._tickers = p.symbolsDF()
            self._tickers_ts = datetime.now()
        return self._tickers

    def preload(self, keys, fields):
        for key in keys:
            # tickers always caps
            key = key.upper()

            log.info('Preloading %s' % key)

            while(True):
                try:
                    _POOL.map(partial(fetch, key, self), fields)
                    break
                except requests.exceptions.ConnectionError:
                    pass

    def purge(self, tickers):
        for ticker in tickers:
            self._cache.pop(ticker, None)

    def load(self, dir, preload=False):
        self._dir = dir

        # make if not exists
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)

        # dump tickers for reference
        self._tickers.to_csv(os.path.join(self._dir, 'TICKERS.csv'))

        # get last sync date
        if os.path.exists(os.path.join(self._dir, 'TIMESTAMP')):
            with open(os.path.join(self._dir, 'TIMESTAMP'), 'r') as fp:
                self._sync = datetime.strptime(fp.read(), '%Y/%m/%d-%H:%M:%S')
        else:
            self._sync = yesterday()

        if self._sync < today() and preload:
            fields = list(FIELDS)
            fields.remove('composition')
            for k in os.listdir(self._dir):
                if k.lower().endswith('.csv'):
                    continue
                self.preload([k], fields)

            self.preload([k for k in os.listdir(self._dir) if k.lower().endswith('.csv')], ['composition'])

        for k in os.listdir(self._dir):
            if k in ('TICKERS.CSV', 'TICKERS.csv', 'TIMESTAMP'):
                continue
            if preload:
                if k not in self._cache:
                    self._cache[k] = {}
                    self._cache[k]['timestamp'] = {}

                for f in FIELDS:
                    filename = os.path.join(self._dir, k, f + '.csv')

                    if 'timestamp' not in self._cache[k]:
                        self._cache[k]['timestamp'] = {}
                    if os.path.exists(filename):
                        try:
                            self._cache[k][f] = pd.read_csv(filename, index_col=0)
                            self._cache[k]['timestamp'][f] = datetime.now()
                        except pd.errors.EmptyDataError:
                            log.info('skipping %s for %s' % (f, k))

    def save(self, tickers=None):
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)

        if tickers:
            _iter = tickers
        else:
            _iter = self._cache

        for k in _iter:
            for f in self._cache.get(k, {}):
                if f == 'timestamp':
                    continue

                if not os.path.exists(os.path.join(self._dir, k)):
                    os.makedirs(os.path.join(self._dir, k))
                filename = os.path.join(self._dir, k, f + '.csv')

                if self._check_timestamp(k, f):
                    self.fetch(k, f)
                log.info('writing %s for %s' % (f, k))
                if not self._cache[k][f].empty:
                    self._cache[k][f].to_csv(filename)

        with open(os.path.join(self._dir, 'TIMESTAMP'), 'w') as fp:
            fp.write(datetime.now().strftime('%Y/%m/%d-%H:%M:%S'))

        for k in self._others:
            path = os.path.join(self._dir, k.upper() + '.csv')
            self._others[k].to_csv(path)

    def _check_timestamp(self, key, field):
        if self._cache[key].get('timestamp', {}).get(field, yesterday()) < today():
            return True
        return False

    def _fetch(self, key, field):
        # deprecated
        return self.fetch(key, field)

    def othersDF(self, field, _ret=True):
        if self._others_ts.get(field, yesterday()) < today():
            if field in ('popularEtfs', 'all'):
                self._others['popularEtfs'] = pd.read_html(POPULAR_ETFS_URL)[0]
                self._others_ts['popularEtfs'] = datetime.now()

        if _ret:
            if field == 'all':
                return pd.concat(self._others)
            return pd.concat({field: self._others[field]})

    def fetchDF(self, key, field, _ret=True):
        # tickers always caps
        key = key.upper()

        # fields always lower
        field = field.lower()

        if not (self._tickers['symbol'] == key).any():
            # FIXME
            return pd.DataFrame()

        if key not in self._cache:
            # initialize cache
            self._cache[key] = {}
            self._cache[key]['timestamp'] = {}

        if field in ('financials', 'all'):
            if 'financials' not in self._cache[key] or self._check_timestamp(key, 'financials'):
                try:
                    self._cache[key]['financials'] = p.financialsDF(key)
                except KeyError:
                    self._cache[key]['financials'] = pd.DataFrame()
                self._cache[key]['timestamp']['financials'] = datetime.now()

        if field in ('chart', 'all'):
            if 'chart' not in self._cache[key] or self._check_timestamp(key, 'chart'):
                try:
                    self._cache[key]['chart'] = p.chartDF(key, '1y')
                except KeyError:
                    self._cache[key]['chart'] = pd.DataFrame()

                self._cache[key]['timestamp']['chart'] = datetime.now()

        if field in ('company', 'all'):
            if 'company' not in self._cache[key] or self._check_timestamp(key, 'company'):
                self._cache[key]['company'] = p.companyDF(key)
                self._cache[key]['timestamp']['company'] = datetime.now()

        if field in ('quote', 'all'):
            # always update
            self._cache[key]['quote'] = p.quoteDF(key)

        if field in ('dividends', 'all'):
            if 'dividends' not in self._cache[key] or self._check_timestamp(key, 'dividends'):
                try:
                    self._cache[key]['dividends'] = p.dividendsDF(key)
                except KeyError:
                    self._cache[key]['dividends'] = pd.DataFrame()
                self._cache[key]['timestamp']['dividends'] = datetime.now()

        if field in ('earnings', 'all'):
            if 'earnings' not in self._cache[key] or self._check_timestamp(key, 'earnings'):
                try:
                    self._cache[key]['earnings'] = p.earningsDF(key)
                except KeyError:
                    self._cache[key]['earnings'] = pd.DataFrame()
                self._cache[key]['timestamp']['earnings'] = datetime.now()

        if field in ('news', 'all'):
            if 'news' not in self._cache[key] or self._check_timestamp(key, 'news'):
                try:
                    self._cache[key]['news'] = p.newsDF(key)
                except KeyError:
                    self._cache[key]['news'] = pd.DataFrame()
                self._cache[key]['timestamp']['news'] = datetime.now()

        if field in ('peers', 'all'):
            if 'peers' not in self._cache[key] or self._check_timestamp(key, 'peers'):
                try:
                    peers = p.peersDF(key)
                except KeyError:
                    peers = pd.DataFrame()

                if peers is not None and not peers.empty:
                    peers = peers.replace({np.nan: None})
                    infos = pd.concat([p.companyDF(item) for item in peers['symbol'].values])
                    self._cache[key]['peers'] = infos
                else:
                    self._cache[key]['peers'] = pd.DataFrame()
                self._cache[key]['timestamp']['peers'] = datetime.now()

        if field in ('stats', 'all'):
            if 'stats' not in self._cache[key] or self._check_timestamp(key, 'stats'):
                try:
                    self._cache[key]['stats'] = p.stockStatsDF(key)
                except KeyError:
                    self._cache[key]['stats'] = pd.DataFrame()
                self._cache[key]['timestamp']['stats'] = datetime.now()

        if field in ('composition', 'all'):
            if 'company' not in self._cache[key]:
                self.fetchDF(key, 'company', _ret=False)

            try:
                self._cache[key]['composition'] = pd.read_html(ETF_URL % key, attrs={'id': 'etfs-that-own'})[0]
                self._cache[key]['composition']['% of Total'] = self._cache[key]['composition']['% of Total'].str.rstrip('%').astype(float) / 100.0
                self._cache[key]['composition'].columns = ['Symbol', 'Name', 'Percent']
                self._cache[key]['composition'] = self._cache[key]['composition'][['Symbol', 'Percent', 'Name']]

            except (IndexError, requests.HTTPError, ValueError, HTTPError):
                self._cache[key]['composition'] = pd.DataFrame()

            self._cache[key]['timestamp']['composition'] = datetime.now()

        if _ret:
            # pull data
            if field == 'all':
                ret = copy.deepcopy(self._cache[key])
                del ret['timestamp']
                ret = pd.concat(ret)

            elif field in self._cache[key]:
                # or i have that field
                ret = pd.concat({field: self._cache[key][field]})
            else:
                raise Exception('No ticker provided!')

            return ret

    def fetch(self, key, field):
        # tickers always caps
        key = key.upper()

        # fields always lower
        field = field.lower()

        self.fetchDF(key, field, _ret=False)

        # pull data
        if field == 'all':
            ret = copy.deepcopy(self._cache[key])
            del ret['timestamp']

        elif field in self._cache[key]:
            # or i have that field
            ret = {field: self._cache[key][field]}

        else:
            raise Exception('Should never get here')

        for field in ret:
            if field == 'financials':
                df = ret['financials'].reset_index()
                ret['financials'] = df[-100:].replace({np.nan: None}).to_dict(orient='records')

            if field == 'chart':
                df = ret['chart'].reset_index()[['date', 'open', 'high', 'low', 'close']]
                df['ticker'] = key
                ret['chart'] = df[-100:].replace({np.nan: None}).to_dict(orient='records')

            if field == 'dividends':
                ret['dividends'] = ret['dividends'].replace({np.nan: None}).to_dict(orient='records')

            if field == 'company':
                ret['company'] = ret['company'].replace({np.nan: None})[
                    ['CEO', 'companyName', 'description', 'sector', 'industry', 'issueType', 'exchange', 'website']].reset_index().replace({np.nan: None}).to_dict(orient='records')[0]

            if field == 'quote':
                ret['quote'] = ret['quote'].replace({np.nan: None}).to_dict(orient='records')[0]

            if field == 'earnings':
                ret['earnings'] = ret['earnings'].replace({np.nan: None}).to_dict(orient='records')

            # if field == 'news':
            #     ret['news'] = ret['news'].replace({np.nan: None}).to_dict(orient='records')

            if field == 'news':
                ret['news'] = ret['news'].replace({np.nan: None})
                if not ret['news'].empty:
                    ret['news']['headline'] = '<a href="' + ret['news']['url'] + '">' + ret['news']['headline'] + ' [<strong>' + ret['news']['source'] + '</strong>]' + '</a>'
                    ret['news']['summary'] = '<p>' + ret['news']['summary'] + '</p>'
                    ret['news'] = ret['news'][['headline', 'summary']].to_dict(orient='records')
                else:
                    ret['news'] = {}

            if field == 'peers':
                ret['peers'] = ret['peers'].replace({np.nan: None}).to_dict(orient='records')

            if field == 'stats':
                ret['stats'] = ret['stats'].replace({np.nan: None}).to_dict(orient='records')

            if field == 'composition':
                ret['composition'] = ret['composition'].replace({np.nan: None}).to_dict(orient='records')

            if field == 'options':
                ret['options'] = ret['composition'].replace({np.nan: None}).to_dict(orient='records')

        return ret


def main():
    cache = Cache()

    if '-v' in sys.argv:
        log.setLevel(logging.INFO)

    cache.load('./cache', preload=True)

    fields = list(FIELDS)
    fields.remove('composition')

    df1 = cache.fetchDF('IWN', 'composition')
    df2 = cache.fetchDF('SPY', 'composition')

    symbols = list(set(
        df1['Symbol'].dropna().values.tolist() +
        df2['Symbol'].dropna().values.tolist()))

    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('loading IEX data for symbols:')
    log.critical(symbols)
    log.critical('#')
    log.critical('#')
    log.critical('#')

    # fetch stuff from IEX
    try:
        for item in tqdm(symbols):
            if item in cache._cache:
                cache.preload([item], fields)
                cache.save([item])
                cache.purge([item])
                continue

            log.info('loading %s' % item)
            cache.preload([item], fields)
            cache.save([item])
            cache.purge([item])

    except KeyboardInterrupt:
        cache.save()

    cache.save()

    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('loading other data')
    log.critical('#')
    log.critical('#')
    log.critical('#')

    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('loading ETF compositions')
    log.critical('#')
    log.critical('#')
    log.critical('#')
    # fetch compositions (slower)
    try:
        while(True):
            try:
                _POOL.map(partial(fetch, cache=cache, field='composition'), symbols)
                break
            except requests.exceptions.ConnectionError:
                pass
    except KeyboardInterrupt:
        cache.save()

    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('Complete')
    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('#')
    log.critical('#')

if __name__ == "__main__":
    main()
