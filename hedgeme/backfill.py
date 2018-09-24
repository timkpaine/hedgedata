import pyEX as p
from datetime import timedelta
from functools import lru_cache
from .utils import three_months, last_close
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
    if len(symbols) > 0:
        return fetch(distributor, p.chartDF, {'timeframe': timeframe}, symbols)
    return []


@lru_cache(None)
def _getRange(_from):
    dates = []
    while _from < last_close():
        dates.append(_from)
        _from += timedelta(days=1)
    return dates


def backfillMinute(distributor, symbols, _from=three_months()):
    dates = _getRange(_from)
    if len(symbols) > 0:
        if len(dates) > len(symbols):
            # make dates the iterable
            for symbol in symbols:
                for date, data in distributor.distribute(p.chartDF, {}, [(symbol, None, date) for date in dates], starmap=True):
                    yield symbol, data
        else:
            # make symbols the iterable
            for date in dates:
                for symbol, data in distributor.distribute(p.chartDF, {'date': date, 'timeframe': None}, symbols):
                    print(date, symbol, data)
                    yield symbol, data
