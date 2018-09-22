try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

import requests
import tornado
import ujson

from datetime import datetime, timedelta, date
from functools import lru_cache
from .log_utils import LOG as log


def parse_args(argv):
    args = []
    kwargs = {}
    for arg in argv:
        if '--' not in arg and '-' not in arg:
            log.debug('ignoring argument: %s', arg)
            continue
        if '=' in arg:
            k, v = arg.replace('-', '').split('=')
            kwargs[k] = v
        else:
            args.append(arg.replace('-', ''))
    return args, kwargs


def parse_body(req, **fields):
    try:
        data = tornado.escape.json_decode(req.body)
    except ValueError:
        data = {}
    return data


def safe_get(path, *args, **kwargs):
    try:
        log.debug('GET: %s' % path)
        resp = requests.get(path, *args, **kwargs).text
        # log.debug('GET_RESPONSE: %s' % resp)
        return ujson.loads(resp)
    except ConnectionRefusedError:
        return {}


def safe_post(path, *args, **kwargs):
    try:
        log.debug('POST: %s' % path)
        resp = requests.post(path, *args, **kwargs).text
        # log.debug('POST_RESPONSE: %s' % resp)
        return ujson.loads(resp)
    except ConnectionRefusedError:
        return {}


def safe_post_cookies(path, *args, **kwargs):
    try:
        log.debug('POST: %s' % path)
        resp = requests.post(path, *args, **kwargs)
        # log.debug('POST_RESPONSE: %s' % resp.text)
        return ujson.loads(resp.text), resp.cookies
    except ConnectionRefusedError:
        return {}, None


def construct_path(host, method):
    return urljoin(host, method)


@lru_cache(1)
def today():
    '''today starts at 4pm the previous close'''
    today = date.today()
    return datetime(year=today.year, month=today.month, day=today.day)


@lru_cache(1)
def yesterday():
    '''yesterday is anytime before the previous 4pm close'''
    today = date.today()
    return datetime(year=today.year, month=today.month, day=today.day) - timedelta(days=1)
