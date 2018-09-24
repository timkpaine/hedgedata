import pyEX as p
import pandas as pd
import logging
from datetime import datetime
from .utils import never, last_close, this_week, append
from .distributor import Distributer
from .backfill import whichBackfill
from .fetch import whichFetch

log = logging.getLogger('')

FIELDS = ['DAILY',
          'TICK',
          'STATS',
          # 'QUOTE',
          'PEERS',
          'NEWS',
          'FINANCIALS',
          'EARNINGS',
          'DIVIDENDS',
          'COMPANY']


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

    def cache(self, symbols=None, delete=False):
        symbols = symbols or p.symbolsDF()['symbol'].values.tolist()

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

            dbs = self.db.list_libraries()
            if field not in dbs:
                log.critical('Initializing %s' % field)
                self.db.initialize_library(field)

            self.libraries[field] = self.db.get_library(field)
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

        # delete data no longer needed
        if delete:
            for field in to_delete:
                for symbol in to_delete[field]:
                    log.critical('Deleting %s from %s' % (symbol, field))
                    self.libraries[field].delete(symbol)

        # backfill data if necessary
        for field in to_fill:
            log.critical('Updating %d items' % len(to_fill[field]))
            for symbol, data in whichBackfill(field)(self.distributor, to_fill[field]):
                log.critical('Filling %s for %s' % (symbol, field))

                data_orig = self.libraries[field].read(symbol).data

                if data_orig.empty:
                    self.libraries[field].write(symbol, data, metadata={'timestamp': datetime.now()})

                else:
                    self.libraries[field].write(symbol, append(data_orig, data), metadata={'timestamp': datetime.now()})

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
