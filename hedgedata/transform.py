import numpy as np
import pandas as pd
from functools import lru_cache


@lru_cache(20)
def whichTransform(field):
    if field == 'DAILY':
        return transformDaily
    elif field == 'TICK':
        return transformMinute
    elif field == 'STATS':
        return transformStats
    elif field == 'PEERS':
        return transformPeers
    elif field == 'NEWS':
        return transformNews
    elif field == 'FINANCIALS':
        return transformFinancials
    elif field == 'EARNINGS':
        return transformEarnings
    elif field == 'DIVIDENDS':
        return transformDividends
    elif field == 'COMPANY':
        return transformCompany
    elif field == 'QUOTE':
        return transformQuote
    elif field == 'COMPOSITION':
        return transformComposition
    else:
        raise NotImplementedError('field: %s' % field)


@lru_cache(100)
def _stripDt(df):
    for col in df.select_dtypes(np.datetime64):
        df[col] = df[col].astype(str)


@lru_cache(100)
def transformDaily(df, symbol, data):
    if df.empty:
        return {}
    df = df.reset_index()
    _stripDt(df)
    df['ticker'] = symbol
    return df[['date', 'ticker', 'open', 'high', 'low', 'close']][-100:].replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformMinute(df, symbol, data):
    if df.empty:
        return {}
    df = df.reset_index()
    _stripDt(df)
    df['ticker'] = symbol
    return df[['date', 'ticker', 'open', 'high', 'low', 'close']][-100:].replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformFinancials(df, symbol, data):
    if df.empty:
        return {}
    df = df.reset_index()
    _stripDt(df)
    return df[-100:].replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformDividends(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformCompany(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None})[
        ['CEO', 'companyName', 'description', 'sector', 'industry', 'issueType', 'exchange', 'website']].reset_index().replace({np.nan: None}).to_dict(orient='records')[0]


@lru_cache(100)
def transformQuote(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None}).to_dict(orient='records')[0]


@lru_cache(100)
def transformEarnings(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformNews(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    if not df.empty:
        df['headline'] = '<a href="' + df['url'] + '">' + df['headline'] + ' [<strong>' + df['source'] + '</strong>]' + '</a>'
        df['summary'] = '<p>' + df['summary'] + '</p>'
        return df[['headline', 'summary']].to_dict(orient='records')
    else:
        return {}


@lru_cache(100)
def transformPeers(df, symbol, data):
    if df is not None and not df.empty:
        df = df.replace({np.nan: None})
        infos = pd.concat([data.read(item, 'COMPANY') for item in df.index.values])
    else:
        infos = pd.DataFrame()
    return infos.replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformStats(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None}).to_dict(orient='records')


@lru_cache(100)
def transformComposition(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None}).to_dict(orient='records')
