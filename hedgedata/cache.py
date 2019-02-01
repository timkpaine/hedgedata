import pyEX
from functools import lru_cache
from .data import Data
from .define import ALL_FIELDS
from .transform import whichTransform


class Cache(object):
    def __init__(self, host='localhost', offline=False):
        