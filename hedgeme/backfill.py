import pyEX as p
from datetime import timedelta
from .utils import six_months, last_close
from .fetch import fetch, \
                   fetchStats as backfillStats, \
                   fetchPeers as backfillPeers, \
                   fetchNews as backfillNews, \
                   fetchFinancials as backfillFinancials, \
                   fetchEarnings as backfillEarnings, \
                   fetchDividends as backfillDividends, \
                   fetchCompany as backfillCompany


def whichBackfill(field):
    if field == 'DAILY':
        return backfillDaily
    elif field == 'TICK':
        return backfillMinute
    elif field == 'STATS':
        return backfillStats
    elif field == 'PEERS':
        return backfillPeers
    elif field == 'NEWS':
        return backfillNews
    elif field == 'FINANCIALS':
        return backfillFinancials
    elif field == 'EARNINGS':
        return backfillEarnings
    elif field == 'DIVIDENDS':
        return backfillDividends
    elif field == 'COMPANY':
        return backfillCompany
    else:
        raise NotImplemented


def backfillDaily(distributor, symbols, timeframe='5y'):
    return fetch(distributor, p.chartDF, {'timeframe': timeframe}, symbols)


def backfillMinute(distributor, symbols, _from=six_months()):
    while _from < last_close():
        for symbol, data in distributor.distribute(p.chartDF, {'date': _from, 'timeframe': None}, symbols):
            yield symbol, data
            _from += timedelta(days=1)
