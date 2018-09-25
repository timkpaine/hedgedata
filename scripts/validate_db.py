#!/usr/local/bin/python3
from hedgedata import Data
from arctic import Arctic

a = Arctic('localhost')

d = Data(a)
d.validate()
