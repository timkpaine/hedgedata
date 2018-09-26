import pyEX as p


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
        raise NotImplemented


def fetch(distributor, foo, foo_kwargs, symbols):
    if len(symbols) > 0:
        for symbol, data in distributor.distribute(foo, {}, symbols):
            yield symbol, data


def fetchDaily(distributor, symbols, timeframe='1m', **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.chartDF, {'timeframe': timeframe}, symbols)
    return []


def fetchMinute(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.chartDF, {'timeframe': '1d'}, symbols)
    return []


def fetchStats(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.stockStatsDF, {}, symbols)
    return []


def fetchPeers(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.peersDF, {}, symbols)
    return []


def fetchNews(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.newsDF, {}, symbols)
    return []


def fetchFinancials(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.financialsDF, {}, symbols)
    return []


def fetchEarnings(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.earningsDF, {}, symbols)
    return []


def fetchDividends(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.dividendsDF, {}, symbols)
    return []


def fetchCompany(distributor, symbols, **kwargs):
    if len(symbols) > 0:
        return fetch(distributor, p.companyDF, {}, symbols)
    return []
