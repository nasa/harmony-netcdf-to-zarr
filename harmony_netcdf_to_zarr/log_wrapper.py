"""Simple wrapper to add a log to harmony."""

from time import time

from harmony.util import config
from harmony.logging import build_logger

c = config(validate=False)
c = c._replace(text_logger=True, app_name="netcdf-to-zarr")
logger = build_logger(c, name='NetCDF-to-Zarr', stream=None)
logger.propagate = False


def get_logger():
    """Return a logger to use in NetCDF-to-Zarr."""
    return logger


def log_elapsed(func):
    """Wrap function with a logging timer."""
    def wrap_func(*args, **kwargs):
        funcname = func.__name__ if func.__name__ else 'unknown function'

        t1 = time()
        logger.info(f'Entered {funcname}')
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.info(f'{funcname} excepted in {(time()-t1):.4f}s')
            raise e
        t2 = time()
        logger.info(f'Function {funcname} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func
