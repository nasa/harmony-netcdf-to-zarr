from time import time
from harmony.util import config
from harmony.logging import build_logger

c = config(validate=False)
c = c._replace(text_logger=True, app_name="netcdf-to-zarr")
logger = build_logger(c, name='NetCDF-to-Zarr', stream=None)
logger.propagate = False

def get_logger():
    return logger

def log_elapsed(func):
    def wrap_func(*args, **kwargs):
        t1 = time()
        logger.info(f'Entered {func.__name__!r}')
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.info(f'{func.__name!r} excepted in {(time()-t1):.4fs}')
            raise e
        t2 = time()
        logger.info(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func
