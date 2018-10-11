import pyEX as p
import pandas as pd
from datetime import datetime
from .utils import never, last_close, this_week, append, business_days, last_month, yesterday
from .distributor import Distributer
from .backfill import whichBackfill
from .fetch import whichFetch, _fetchComposition
from .log_utils import log
from .validate import SKIP_VALIDATION
from .define import CACHE_FIELDS as FIELDS


def updateTime(field):
    if field in ('DAILY', 'TICK', 'NEWS'):
        return last_close()
    elif field in ('COMPANY', 'DIVIDENDS', 'EARNINGS', 'FINANCIALS', 'PEERS', 'STATS'):
        return this_week()
    elif field in ('QUOTE',):
        return never()


class Data(object):
    def __init__(self, db):
        self.db = db
        self.libraries = {}
        self.distributor = Distributer.default()

        for field in FIELDS:
            dbs = self.db.list_libraries()
            if field not in dbs:
                log.critical('Initializing %s' % field)
                self.db.initialize_library(field)
                if field in ('TICK',):
                    # set 20GB quota
                    self.db.set_quota(field, 21474836480)
            self.libraries[field] = self.db.get_library(field)

    def cache(self, symbols=None, fields=None, delete=False):
        fields = fields or FIELDS
        symbols = symbols or p.symbolsDF().index.values.tolist()

        to_delete, to_fill, to_update = self.initialize(symbols, fields)

        if delete:
            self.delete(to_delete)
        self.backfill(to_fill)
        self.update(to_update)
        self.validate()

    def delete(self, to_delete=None):
        # delete data no longer needed
        for field in to_delete:
            for symbol in to_delete[field]:
                log.critical('Deleting %s from %s' % (symbol, field))
                self.libraries[field].delete(symbol)

    def backfill(self, to_fill):
        # backfill data if necessary
        for field in to_fill:
            log.critical('Backfilling %d items' % len(to_fill[field]))
            for symbol, data in whichBackfill(field)(self.distributor, to_fill[field]):
                log.critical('Filling %s for %s' % (symbol, field))
                data_orig = self.libraries[field].read(symbol).data

                if data_orig.empty:
                    self.libraries[field].write(symbol, data, metadata={'timestamp': datetime.now()})
                else:
                    self.libraries[field].write(symbol, append(data_orig, data), metadata={'timestamp': datetime.now()})

    def update(self, to_update):
        # update data if necessary
        for field in to_update:
            log.critical('Updating %d items' % len(to_update[field]))
            for symbol, data in whichFetch(field)(self.distributor, to_update[field]):
                log.critical('Updating %s for %s' % (symbol, field))

                data_orig = self.libraries[field].read(symbol).data
                if data_orig.empty:
                    self.libraries[field].write(symbol, data, metadata={'timestamp': datetime.now()})
                else:
                    self.libraries[field].write(symbol, append(data_orig, data), metadata={'timestamp': datetime.now()})

    def initialize(self, symbols=None, fields=None):
        '''setup db'''
        fields = fields or FIELDS
        symbols = symbols or p.symbolsDF().index.values.tolist()

        to_fill = {}
        to_update = {}
        to_delete = {}

        # initialize database and collect what to update
        for field in FIELDS:
            if field not in to_fill:
                to_fill[field] = []
            if field not in to_update:
                to_update[field] = []
            if field not in to_delete:
                to_delete[field] = []

            library = self.libraries[field]
            all_symbols = library.list_symbols()

            for symbol in symbols:
                symbol = symbol.upper()
                if symbol not in all_symbols:
                    log.critical('Initializing %s for %s' % (symbol, field))
                    to_fill[field].append(symbol)
                    library.write(symbol.upper(), pd.DataFrame(), metadata={'timestamp': never()})

                else:
                    metadata = library.read_metadata(symbol.upper()).metadata
                    if not metadata or not metadata.get('timestamp'):
                        to_fill[field].append(symbol)
                    elif metadata.get('timestamp', never()) <= never():
                        to_fill[field].append(symbol)
                    elif metadata.get('timestamp', never()) < updateTime(field):
                        to_update[field].append(symbol)

            for symbol in set(all_symbols) - set(symbols):
                    to_delete[field].append(symbol)
        return to_delete, to_fill, to_update

    def validate(self, symbols=None, fields=None):
        '''look for missing data'''
        fields = fields or FIELDS
        symbols = symbols or p.symbolsDF().index.values.tolist()
        to_refill = {}
        self.initialize(symbols, fields)

        for field in FIELDS:
            tick_start_date = None
            to_refill[field] = []

            if (field, '*') in SKIP_VALIDATION:
                continue

            dbs = self.db.list_libraries()
            if field not in dbs:
                log.critical('VALIDATION FAILED %s' % field)
                continue

            lib = self.db.get_library(field)
            all_symbols = lib.list_symbols()

            for symbol in symbols:
                symbol = symbol.upper()
                if (field, symbol) in SKIP_VALIDATION or \
                   ('*', symbol) in SKIP_VALIDATION:
                    continue

                if symbol not in all_symbols:
                    to_refill[field].append(symbol)
                    log.critical('VALIDATION FAILED %s for %s' % (symbol, field))
                    continue

                data = lib.read(symbol).data
                if data.empty:
                    log.critical('VALIDATION FAILED - DATA EMPTY %s for %s' % (symbol, field))
                    to_refill[field].append(symbol)
                    continue

                elif field in ('TICK'):
                    dates = business_days(last_month(), yesterday())
                    for date in dates:
                        if date.date() not in data.index:
                            log.critical('VALIDATION FAILED - DATA MISSING %s for %s : %s' % (symbol, field, date.strftime('%Y%m%d')))
                            to_refill[field].append(symbol)
                            tick_start_date = min(tick_start_date, date.date()) if tick_start_date is not None else date.date()
                            break

                elif field in ('Daily'):
                    dates = business_days(last_month(), yesterday())
                    for date in dates:
                        if date.date() not in data.index:
                            log.critical('VALIDATION FAILED - DATA MISSING %s for %s : %s' % (symbol, field, date.strptime('%Y%m%d')))
                            to_refill[field].append(symbol)
                            tick_start_date = min(tick_start_date, date.date()) if tick_start_date is not None else date.date()
                            break

        # backfill data if necessary
        for field in to_refill:
            log.critical('Backfilling %d items for %s' % (len(to_refill[field]), field))

            for symbol, data in whichBackfill(field)(self.distributor, to_refill[field], from_=tick_start_date):
                if field in ('TICK',):
                    log.critical('Updating %s for %s : %s' % (symbol, field, tick_start_date.strptime('%Y%m%d')))
                else:
                    log.critical('Updating %s for %s' % (symbol, field))

                data_orig = self.libraries[field].read(symbol).data
                if data_orig.empty:
                    self.libraries[field].write(symbol, data, metadata={'timestamp': datetime.now()})
                else:
                    self.libraries[field].write(symbol, append(data_orig, data), metadata={'timestamp': datetime.now()})

    def read(self, symbol, field, fetch=True, fill=False):
        field = field.upper()
        symbol = symbol.upper()

        if field in ('QUOTE'):
            # dont cache, instantaneous
            return p.quoteDF(symbol)

        if field not in self.libraries:
            return pd.DataFrame()

        l = self.libraries[field]

        if not l.has_symbol(symbol):
            return pd.DataFrame()

        df = l.read(symbol).data
        metadata = l.read_metadata(symbol).metadata

        if fetch:
            if df.empty or not metadata or not metadata.get('timestamp') or \
               metadata.get('timestamp', never()) <= never() or \
               metadata.get('timestamp', never()) < updateTime(field):

                if field == 'FINANCIALS':
                    df = p.financialsDF(symbol)
                elif field == 'DAILY':
                    df = p.chartDF(symbol, '5y')
                elif field == 'COMPANY':
                    df = p.companyDF(symbol)
                elif field == 'EARNINGS':
                    df = p.earningsDF(symbol)
                elif field == 'DIVIDENDS':
                    df = p.dividendsDF(symbol)
                elif field == 'NEWS':
                    df = p.newsDF(symbol)
                elif field == 'STATS':
                    df = p.stockStatsDF(symbol)

                elif field == 'COMPOSITION':
                    df = _fetchComposition(symbol)

                elif field == 'PEERS':
                    df = p.peersDF(symbol)

                if fill:
                    l.write(symbol, df, metadata={'timestamp': datetime.now()})

        return df
