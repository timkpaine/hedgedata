import pandas as pd
from functools import lru_cache
from .define import ETF_URL, POPULAR_ETFS_URL


@lru_cache(None)
def composition(key):
    comp = pd.read_html(ETF_URL % key, attrs={'id': 'etfs-that-own'})[0]
    comp['% of Total'] = comp['% of Total'].str.rstrip('%').astype(float) / 100.0
    comp.columns = ['Symbol', 'Name', 'Percent']
    comp[['Symbol', 'Percent', 'Name']]
    return comp


def spy():
    return composition('spy')


def constituents(key):
    return composition(key)['Symbol'].values.tolist()
