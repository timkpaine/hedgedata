import pandas as pd
from functools import lru_cache
from .define import ETF_URL
from .utils import symbols_map


@lru_cache(None)
def composition(key):
    comp = pd.read_html(ETF_URL % key, attrs={'id': 'etfs-that-own'})[0]
    comp['% of Total'] = comp['% of Total'].str.rstrip('%').astype(float) / 100.0
    comp.columns = ['Symbol', 'Name', 'Percent']

    comp['Symbol'].apply(lambda x: symbols_map().get(x, x))
    return comp[['Symbol', 'Percent', 'Name']]


def constituents(key):
    return composition(key)['Symbol'].dropna().values.tolist()


def spy():
    '''spy'''
    return composition('spy')


def spy_constituents():
    '''spy'''
    return constituents('spy')


def sp500():
    '''just use spy'''
    return composition('spy')


def sp500_constituents():
    '''just use spy'''
    return constituents('spy')


def djia():
    '''just use dia'''
    return composition('DIA')


def djia_constituents():
    '''just use dia'''
    return constituents('DIA')


def qqq():
    '''qqq'''
    return composition('qqq')


def qqq_constituents():
    '''qqq'''
    return constituents('qqq')


def nasdaq():
    '''just use qqq'''
    return composition('qqq')


def nasdaq_constituents():
    return constituents('qqq')


def russell1000():
    return composition('IWB')


def russell1000_constituents():
    return constituents('IWB')


def russell2000():
    return composition('IWM')


def russell2000_constituents():
    return constituents('IWM')


def russell3000():
    return composition('IWV')


def russell3000_constituents():
    return constituents('IWV')
