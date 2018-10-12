import pandas as pd
import traceback
from functools import partial
from multiprocessing.pool import ThreadPool
from .log_utils import log


def chunks(l, n):
    '''MISH'''
    if len(l) > 0:
        for i in range(0, len(l), n):
            yield l[i:i + n]


class Distributer(object):
    def __init__(self, kind, chunkSize=20):
        self.kind = kind
        self.chunk_size = chunkSize

    def distribute(self, function, function_kwargs, iterable, starmap=False, skip_if_error=True, max_attempts=3):
        if self.kind == 'thread':
            self.pool = ThreadPool(self.chunk_size)
        else:
            raise NotImplemented

        if isinstance(self.pool, ThreadPool):
            for chunk in chunks(iterable, self.chunk_size):
                attempts = 0
                individually = False

                while attempts < max_attempts:
                    try:
                        if not individually:
                            if starmap:
                                ret = self.pool.starmap(function, chunk)
                            else:
                                ret = self.pool.map(partial(function, **function_kwargs), chunk)
                            attempts = max_attempts
                        else:
                            ret = []
                            for item in chunk:
                                try:
                                    if starmap:
                                        val = function(*item)
                                    else:
                                        val = partial(function, **function_kwargs)(item)
                                    ret.append(val)

                                except Exception as e:
                                    log.critical(e)
                                    log.critical(''.join(traceback.format_exception(None, e, e.__traceback__)))
                                    attempts += 1
                                    if attempts >= max_attempts:
                                        if skip_if_error:
                                            ret.append(pd.DataFrame())
                                        else:
                                            raise e

                    except Exception as e:
                        log.error(e)
                        attempts += 1
                        if attempts >= max_attempts:
                            individually = True
                            attempts = 0

                for i, item in enumerate(ret):
                    yield (chunk[i], item)

        else:
            raise NotImplemented

        self.pool.stop()

    def stop(self):
        self.pool.stop()

    @staticmethod
    def default():
        return Distributer('thread')
