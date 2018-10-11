import pandas as pd
import pyEX as p
import string
from trading_calendars import get_calendar
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from functools import lru_cache


_TRANSLATOR = str.maketrans('', '', string.punctuation)
_OVERRIDES = {
    'PCLN': 'BKNG'
}


@lru_cache(1)
def symbols():
    return p.symbolsList()


@lru_cache(1)
def symbols_map():
    ret = {}
    for x in symbols():
        ret[x] = x
        new_x = x.translate(_TRANSLATOR)
        if new_x not in ret:
            ret[new_x] = x
    for k, v in _OVERRIDES.items():
        ret[k] = v
    return ret


@lru_cache(1)
def today():
    '''today starts at 4pm the previous close'''
    today = date.today()
    return datetime(year=today.year, month=today.month, day=today.day)


@lru_cache(1)
def this_week():
    '''start of week'''
    return today() - timedelta(days=datetime.today().isoweekday() % 7)


@lru_cache(1)
def last_close():
    '''last close'''
    today = date.today()
    close = datetime(year=today.year, month=today.month, day=today.day, hour=16)

    if datetime.now().hour < 16:
        close -= timedelta(days=1)
        if close.weekday() == 5:  # saturday
            return close - timedelta(days=1)
        elif close.weekday() == 6:  # sunday
            return close - timedelta(days=2)
        return close
    return close


@lru_cache(1)
def yesterday():
    '''yesterday is anytime before the previous 4pm close'''
    today = date.today()

    if today.weekday() == 0:  # monday
        return datetime(year=today.year, month=today.month, day=today.day) - timedelta(days=3)
    elif today.weekday() == 6:  # sunday
        return datetime(year=today.year, month=today.month, day=today.day) - timedelta(days=2)
    return datetime(year=today.year, month=today.month, day=today.day) - timedelta(days=1)


@lru_cache(1)
def last_month():
    '''last_month is one month before today'''
    today = date.today()
    last_month = datetime(year=today.year, month=today.month, day=today.day) - relativedelta(months=1)

    if last_month.weekday() == 5:
        last_month -= timedelta(days=1)
    elif last_month.weekday() == 6:
        last_month -= timedelta(days=2)
    return last_month


@lru_cache(1)
def six_months():
    '''six_months is six months before today'''
    today = date.today()
    six_months = datetime(year=today.year, month=today.month, day=today.day) - relativedelta(months=6)

    if six_months.weekday() == 5:
        six_months -= timedelta(days=1)
    elif six_months.weekday() == 6:
        six_months -= timedelta(days=2)
    return six_months


@lru_cache(1)
def three_months():
    '''three_months is three months before today'''
    today = date.today()
    six_months = datetime(year=today.year, month=today.month, day=today.day) - relativedelta(months=3)

    if six_months.weekday() == 5:
        six_months -= timedelta(days=1)
    elif six_months.weekday() == 6:
        six_months -= timedelta(days=2)
    return six_months


@lru_cache(1)
def never():
    '''long long time ago'''
    return datetime(year=1, month=1, day=1)


def append(df1, df2):
    merged = pd.concat([df1, df2])
    return merged[~merged.index.duplicated(keep='first')]


@lru_cache(1)
def holidays():
    return get_calendar('NYSE').regular_holidays.holidays().to_pydatetime().tolist()


@lru_cache(None)
def business_days(start, end=last_close()):
    ret = []
    while start < last_close():
        if start not in holidays() and start.weekday() != 6 and start.weekday() != 5:
            ret.append(start)
        start += timedelta(days=1)
    return ret
