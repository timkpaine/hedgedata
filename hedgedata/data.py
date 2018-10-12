import pyEX as p
import pandas as pd
from datetime import datetime
import arctic
from arctic import Arctic
from pymongo.errors import ServerSelectionTimeoutError
from .utils import never, business_days, last_month, yesterday, today
from .data_utils import _getLib, _appendIfNecessary, _updateTime, _skip
from .distributor import Distributer
from .backfill import whichBackfill
from .fetch import whichFetch, refetch
from .log_utils import log, logit
from .define import CACHE_FIELDS as FIELDS


class OfflineLib(object):
    def has_symbol(self, symbol):
        return False


class OfflineDB(object):
    def list_libraries(self):
        return FIELDS

    def initialize_library(self, name):
        return OfflineLib()

    def get_library(self, name):
        return OfflineLib()


class Data(object):
    def __init__(self, dbname, offline=False):
        self.libraries = {}
        self.distributor = Distributer.default()

        if offline:
            self.db = OfflineDB()
            log.critical('WARNING Running in offline mode')
            return

        self.db = Arctic(dbname)

        # initialize databases
        for field in FIELDS:
            try:
                self.libraries[field] = _getLib(self.db, field)
            except (arctic.exceptions.LibraryNotFoundException, ServerSelectionTimeoutError):
                log.critical('Arctic not available, is mongo offline??')
                raise

    def cache(self, symbols=None, fields=None, delete=False):
        fields = fields or FIELDS
        symbols = symbols or p.symbolsDF().index.values.tolist()

        to_delete, to_fill, to_update = self.initialize(symbols, fields)

        if delete:
            # prune data
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
            lib = self.libraries[field]

            for symbol, data in whichBackfill(field)(self.distributor, to_fill[field]):
                log.critical('Filling %s for %s' % (symbol, field))
                data_orig = lib.read(symbol).data
                _appendIfNecessary(lib, symbol, data_orig, data)

    def update(self, to_update):
        # update data if necessary
        for field in to_update:
            log.critical('Updating %d items' % len(to_update[field]))
            lib = self.libraries[field]

            for symbol, data in whichFetch(field)(self.distributor, to_update[field]):
                log.critical('Updating %s for %s' % (symbol, field))
                data_orig = self.libraries[field].read(symbol).data
                _appendIfNecessary(lib, symbol, data_orig, data)

    def initialize(self, symbols=None, fields=None):
        '''setup db'''
        fields = fields or FIELDS
        symbols = symbols or p.symbolsDF().index.values.tolist()

        to_fill = {}
        to_update = {}
        to_delete = {}

        _empty = pd.DataFrame()

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
                    library.write(symbol, _empty, metadata={'timestamp': never()})

                else:
                    metadata = library.read_metadata(symbol.upper()).metadata
                    if not metadata or not metadata.get('timestamp'):
                        to_fill[field].append(symbol)
                    elif metadata.get('timestamp', never()) <= never():
                        to_fill[field].append(symbol)
                    elif metadata.get('timestamp', never()) < _updateTime(field):
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
            tick_start_date = today()
            daily_start_date = today()
            fail_count = 0
            print_fail = False
            dates = business_days(last_month(), yesterday())
            to_refill[field] = []

            if _skip(field):
                continue

            dbs = self.db.list_libraries()
            if field not in dbs:
                log.critical('VALIDATION FAILED %s' % field)
                continue

            lib = self.db.get_library(field)
            all_symbols = lib.list_symbols()

            for symbol in symbols:
                symbol = symbol.upper()

                # if fail count too high, autofail all for speed
                if fail_count > .2 * len(all_symbols):
                    if not print_fail:
                        log.critical('VALIDATION THRESHOLD REACHED for %s' % field)
                        print_fail = True

                    if _skip(field, symbol):
                        continue

                    to_refill[field].append(symbol)
                    if field == 'DAILY':
                        daily_start_date = dates[0]
                    if field == 'TICK':
                        tick_start_date = dates[0]
                    continue

                if _skip(field, symbol):
                    continue

                if symbol not in all_symbols:
                    to_refill[field].append(symbol)
                    log.critical('VALIDATION FAILED %s for %s' % (symbol, field))
                    fail_count += 1
                    continue

                data = lib.read(symbol).data

                if data.empty:
                    log.critical('VALIDATION FAILED - DATA EMPTY %s for %s' % (symbol, field))
                    to_refill[field].append(symbol)
                    fail_count += 1
                    continue

                elif field in ('TICK'):
                    for date in dates:
                        if date not in data.index:
                            log.critical('VALIDATION FAILED - DATA MISSING %s for %s : %s' % (symbol, field, date.strftime('%Y%m%d')))
                            to_refill[field].append(symbol)
                            tick_start_date = min(tick_start_date, date) if tick_start_date is not None else date
                            fail_count += 1
                            break

                elif field in ('Daily'):
                    for date in dates:
                        if date not in data.index:
                            log.critical('VALIDATION FAILED - DATA MISSING %s for %s : %s' % (symbol, field, date.strptime('%Y%m%d')))
                            to_refill[field].append(symbol)
                            daily_start_date = min(daily_start_date, date) if daily_start_date is not None else date
                            fail_count += 1
                            break

        # backfill data if necessary
        for field in to_refill:
            lib = self.libraries[field]

            if field == 'TICK':
                log.critical('Backfilling %d items for %s - %s' % (len(to_refill[field]), field, str(tick_start_date)))
            elif field == 'DAILY':
                log.critical('Backfilling %d items for %s - %s' % (len(to_refill[field]), field, str(daily_start_date)))
            else:
                log.critical('Backfilling %d items for %s' % (len(to_refill[field]), field))

            for symbol, data in whichBackfill(field)(self.distributor, to_refill[field], from_=tick_start_date):
                log.critical('Updating %s for %s' % (symbol, field))

                data_orig = lib.read(symbol).data
                _appendIfNecessary(lib, symbol, data_orig, data)

    def read(self, symbol, field, fetch=True, fill=False):
        field = field.upper()
        symbol = symbol.upper()

        if field in ('QUOTE'):
            # dont cache, instantaneous
            return p.quoteDF(symbol)
        elif field in ('COMPOSITION'):
            return refetch(field, symbol)

        if field not in self.libraries and not fetch:
            return pd.DataFrame()

        l = _getLib(self.db, field)

        if not l.has_symbol(symbol):
            if not fetch:
                return pd.DataFrame()
            df = pd.DataFrame()
        else:
            df = l.read(symbol).data
            metadata = l.read_metadata(symbol).metadata

        if fetch:
            if df.empty or not metadata or not metadata.get('timestamp') or \
               metadata.get('timestamp', never()) <= never() or \
               metadata.get('timestamp', never()) < _updateTime(field):

                df = refetch(field, symbol)
                if fill:
                    l.write(symbol, df, metadata={'timestamp': datetime.now()})
        return df
