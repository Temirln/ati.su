import inspect
import logging
import time
from datetime import datetime, timedelta

MAX_RETRIES = 10


def execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        print("Execution Time: {:.3f} s".format(end_time - start_time))

    return wrapper


def logger(log_level=logging.DEBUG) -> logging:
    logger_name = inspect.stack()[1][3]
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s : %(message)s"
    )

    fh = logging.FileHandler(
        "./ati.su/custom_utils/logger.log", mode="a", encoding="UTF-8"
    )

    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
