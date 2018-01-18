import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import time

from . import config

logger = logging.getLogger('Agent')


def _configure_loggers():
    """
    Configure the root logger so that all log messages (including 3rd party libraries) are logged to file using
    the same format. Additionally configure the Agent logger to write to the console. Logs are rolled over at
    00h00 on Mondays.
    """
    rootlogger = logging.getLogger()
    rootlogger.setLevel(config.LOG_LEVEL)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(filename)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    filehandler = TimedRotatingFileHandler(os.path.join(config.LOG_DIR, 'agent.log'), when='W0', atTime=time(0))
    consolehandler = logging.StreamHandler()
    filehandler.setFormatter(formatter)
    consolehandler.setFormatter(formatter)
    rootlogger.addHandler(filehandler)
    logger.addHandler(consolehandler)

_configure_loggers()
