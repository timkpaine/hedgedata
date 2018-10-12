import pyEX as p
import pandas as pd
import requests
from urllib.error import HTTPError
from .define import POPULAR_ETFS_URL, ETF_URL


def fetch(fields, symbols):
    pass


def refetch(field, symbol):
    if field == 'TICK':
        return p.chartDF(symbol, '1d')
    if field == 'FINANCIALS':
        return p.financialsDF(symbol)
    elif field == 'DAILY':
        return p.chartDF(symbol, '5y')
    elif field == 'COMPANY':
        return p.companyDF(symbol)
    elif field == 'EARNINGS':
        return p.earningsDF(symbol)
    elif field == 'DIVIDENDS':
        return p.dividendsDF(symbol)
    elif field == 'NEWS':
        return p.newsDF(symbol)
    elif field == 'STATS':
        return p.stockStatsDF(symbol)
    elif field == 'COMPOSITION':
        return _fetchComposition(symbol)
    elif field == 'PEERS':
        return p.peersDF(symbol)
    raise NotImplementedError('%s - %s' % (field, symbol))


def whichFetch(field):
    if field == 'DAILY':
        return fetchDaily
    elif field == 'TICK':
        return fetchMinute
    elif field == 'STATS':
        return fetchStats
    elif field == 'PEERS':
        return fetchPeers
    elif field == 'NEWS':
        return fetchNews
    elif field == 'FINANCIALS':
        return fetchFinancials
    elif field == 'EARNINGS':
        return fetchEarnings
    elif field == 'DIVIDENDS':
        return fetchDividends
    elif field == 'COMPANY':
        return fetchCompany
    else:
        raise NotImplementedError('%s' % field)


def _fetch(distributor, foo, foo_kwargs, symbols):
    if len(symbols) > 0:
        for symbol, data in distributor.distribute(foo, foo_kwargs or {}, symbols):
            yield symbol, data


def fetchDaily(distributor, symbols, timeframe='1m', **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.chartDF, {'timeframe': timeframe}, symbols)
    return []


def fetchMinute(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.chartDF, {'timeframe': '1d'}, symbols)
    return []


def fetchStats(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.stockStatsDF, {}, symbols)
    return []


def fetchPeers(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.peersDF, {}, symbols)
    return []


def fetchNews(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.newsDF, {}, symbols)
    return []


def fetchFinancials(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.financialsDF, {}, symbols)
    return []


def fetchEarnings(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.earningsDF, {}, symbols)
    return []


def fetchDividends(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.dividendsDF, {}, symbols)
    return []


def fetchCompany(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return _fetch(distributor, p.companyDF, {}, symbols)
    return []


def _fetchComposition(symbol):
    try:
        comp = pd.read_html(ETF_URL % symbol, attrs={'id': 'etfs-that-own'})[0]
        comp['% of Total'] = comp['% of Total'].str.rstrip('%').astype(float) / 100.0
        comp.columns = ['Symbol', 'Name', 'Percent']
        comp = comp[['Symbol', 'Percent', 'Name']]
        return comp

    except (IndexError, requests.HTTPError, ValueError, HTTPError):
        return pd.DataFrame()


def fetchComposition(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        for symbol, data in distributor.distribute(_fetchComposition, foo_kwargs or {}, symbols):
            yield symbol, data
    return []


def fetchPopularEtfs(**kwargs):
    return pd.read_html(POPULAR_ETFS_URL)[0]
