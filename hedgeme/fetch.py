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
    for symbol, data in distributor.distribute(foo, {}, symbols):
        yield symbol, data


def fetchDaily(distributor, symbols, timeframe='1m'):
    return fetch(distributor, p.chartDF, {'timeframe': timeframe}, symbols)


def fetchMinute(distributor, symbols):
    return fetch(distributor, p.chartDF, {'timeframe': '1d'}, symbols)


def fetchStats(distributor, symbols):
    return fetch(distributor, p.stockStatsDF, {}, symbols)


def fetchPeers(distributor, symbols):
    return fetch(distributor, p.peersDF, {}, symbols)


def fetchNews(distributor, symbols):
    return fetch(distributor, p.newsDF, {}, symbols)


def fetchFinancials(distributor, symbols):
    return fetch(distributor, p.financialsDF, {}, symbols)


def fetchEarnings(distributor, symbols):
    return fetch(distributor, p.earningsDF, {}, symbols)


def fetchDividends(distributor, symbols):
    return fetch(distributor, p.dividendsDF, {}, symbols)


def fetchCompany(distributor, symbols):
    return fetch(distributor, p.companyDF, {}, symbols)
