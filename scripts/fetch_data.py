#!/usr/local/bin/python3
import os
import os.path
import pandas as pd
from hedgedata.fetch import whichFetch
from hedgedata.backfill import whichBackfill
from hedgedata.data import FIELDS
from hedgedata.distributor import Distributer
from hedgedata.log_utils import log
from hedgedata import symbols as all_symbols
from hedgedata import sp500_constituents

_DISTRIBUTOR = Distributer.default()


def defaults(field):
    if field == 'DAILY':
        return ['KEY', 'date']
    elif field == 'TICK':
        return ['KEY', 'date', 'minute']
    elif field == 'STATS':
        return ['KEY']
    elif field == 'PEERS':
        return ['KEY', 'peer']
    elif field == 'NEWS':
        return ['KEY', 'datetime']
    elif field == 'FINANCIALS':
        return ['KEY', 'reportDate']
    elif field == 'EARNINGS':
        return ['KEY', 'EPSReportDate']
    elif field == 'DIVIDENDS':
        return ['KEY', 'exDate']
    elif field == 'COMPANY':
        return ['KEY']
    else:
        print(field)
        raise NotImplemented


def backfillData(symbols, fields, output='cache'):
    if not os.path.exists('cache'):
        os.makedirs('cache')

    for field in fields:
        if os.path.exists(os.path.join('cache', field) + '.csv'):
            data_orig = pd.read_csv(os.path.join('cache', field) + '.csv')
            for k in ('date', 'datetime', 'reportDate', 'EPSReportDate', 'exDate'):
                if k in data_orig.columns:
                    data_orig[k] = pd.to_datetime(data_orig[k])

        else:
            data_orig = pd.DataFrame()

        for symbol, data in whichBackfill(field)(_DISTRIBUTOR, symbols):
            if data.empty:
                log.critical('Skipping %s for %s' % (symbol, field))
                continue

            log.critical('Filling %s for %s' % (symbol, field))
            data.reset_index(inplace=True)

            if field == 'PEERS':
                data = data[['peer']]

            data['KEY'] = symbol
            data_orig = pd.concat([data_orig, data]) if not data_orig.empty else data

        if not data_orig.empty:
            data_orig.set_index(defaults(field), inplace=True)
            data_orig[~data_orig.index.duplicated(keep='first')].to_csv(os.path.join('cache', field) + '.csv')


if __name__ == '__main__':
    backfillData(sp500_constituents(), FIELDS)
