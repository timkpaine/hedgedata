import pyEX as p
from datetime import timedelta
from functools import lru_cache
from .utils import last_month, last_close
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


def backfillDaily(distributor, symbols, timeframe='5y', **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.chartDF, {'timeframe': '5y'}, symbols)
    return []


@lru_cache(None)
def _getRange(_from):
    dates = []
    while _from < last_close():
        dates.append(_from)
        _from += timedelta(days=1)
    return dates


def backfillMinute(distributor, symbols, _from=last_month(), **kwargs):
    dates = _getRange(_from)
    if len(symbols) > 0:
        if len(dates) > len(symbols):
            # make dates the iterable
            for symbol in symbols:
                for date, data in distributor.distribute(p.chartDF, {}, [(symbol, '1d', date) for date in dates], starmap=True):
                    yield symbol, data
        else:
            # make symbols the iterable
            for date in dates:
                for symbol, data in distributor.distribute(p.chartDF, {'date': date, 'timeframe': '1d'}, symbols):
                    yield symbol, data
