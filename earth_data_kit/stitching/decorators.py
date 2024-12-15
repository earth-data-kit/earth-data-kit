import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def log_time(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.debug("{} ran in {}s".format(func.__name__, end - start))
        return result

    return wrapper


def log_init(func):
    """This decorator prints that the function has started executing"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug("Running function {}".format(func.__name__))
        result = func(*args, **kwargs)
        return result

    return wrapper


def deprecated(func):
    """This decorator prints that the function has started been deprecated"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.warn(
            "Function {} will be deprecated.".format(func.__name__)
        )
        result = func(*args, **kwargs)
        return result

    return wrapper
