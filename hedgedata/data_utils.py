from datetime import datetime
from .log_utils import log
from .utils import never, last_close, this_week, append
from .validate import SKIP_VALIDATION

EXTRA_CAPACITY = ('TICK',)


def _updateTime(field):
    if field in ('DAILY', 'TICK', 'NEWS'):
        return last_close()
    elif field in ('COMPANY', 'DIVIDENDS', 'EARNINGS', 'FINANCIALS', 'PEERS', 'STATS'):
        return this_week()
    elif field in ('QUOTE',):
        return never()


def _appendIfNecessary(lib, symbol, data_new):
    data_orig = lib.read(symbol).data
    if data_orig.empty:
        lib.write(symbol, data_new, metadata={'timestamp': datetime.now()})
    else:
        lib.write(symbol, append(data_orig, data_new), metadata={'timestamp': datetime.now()})


def _skip(field, symbol=None):
    if symbol is None:
        if (field, '*') in SKIP_VALIDATION:
            return True
        return False
    if (field, symbol) in SKIP_VALIDATION or \
       ('*', symbol) in SKIP_VALIDATION:
        return True
    return False


def _write(lib, symbol, data, dateoverride=None):
    if dateoverride:
        date = dateoverride
    else:
        date = datetime.now()
    lib.write(symbol, data, metadata={'timestamp': date})


def _getLib(db, libname):
    if libname not in db.list_libraries():
        log.critical('Initializing %s' % libname)
        db.initialize_library(libname)
        if libname in EXTRA_CAPACITY:
            db.set_quota(libname, 21474836480)
    return db.get_library(libname)
