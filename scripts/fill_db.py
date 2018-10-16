#!/usr/local/bin/python3
from hedgedata import Data, symbols

d = Data('localhost')
d.cache(symbols())
