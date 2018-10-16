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


def _stripDt(df):
    for col in df.select_dtypes(np.datetime64):
        df[col] = df[col].astype(str)


def transformDaily(df, symbol, data):
    if df.empty:
        return {}
    df = df.reset_index()
    _stripDt(df)
    df['ticker'] = symbol
    df = df[['date', 'ticker', 'open', 'high', 'low', 'close']][-100:].replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformMinute(df, symbol, data):
    if df.empty:
        return {}
    df = df.reset_index()
    _stripDt(df)
    df['ticker'] = symbol
    df = df[['date', 'ticker', 'open', 'high', 'low', 'close']][-100:].replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformFinancials(df, symbol, data):
    if df.empty:
        return {}
    df = df.reset_index()
    _stripDt(df)
    df = df[-100:].replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformDividends(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformCompany(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    return df.replace({np.nan: None})[
        ['CEO', 'companyName', 'description', 'sector', 'industry', 'issueType', 'exchange', 'website']].reset_index().replace({np.nan: None}).to_dict(orient='records')[0]


def transformQuote(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    return df.to_dict(orient="record")[0]


def transformEarnings(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformNews(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    if not df.empty:
        df['headline'] = '<a href="' + df['url'] + '">' + df['headline'] + ' [<strong>' + df['source'] + '</strong>]' + '</a>'
        df['summary'] = '<p>' + df['summary'] + '</p>'
        df = df[['headline', 'summary']]
        return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}
    else:
        return {}


def transformPeers(df, symbol, data):
    if df is not None and not df.empty:
        df = df.replace({np.nan: None})
        infos = pd.concat([data.read(item, 'COMPANY') for item in df.index.values])
    else:
        infos = pd.DataFrame()
    df = infos.replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformStats(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}


def transformComposition(df, symbol, data):
    if df.empty:
        return {}
    _stripDt(df)
    df = df.replace({np.nan: None})
    return {k: v.values.tolist() for k, v in df.to_dict(orient="series").items()}
