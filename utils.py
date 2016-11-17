# coding: utf-8
from __future__ import print_function
import sys
from time import sleep
import logging

import colorlog


def flush_print(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def wait(seconds):
    sec = 0
    while sec < seconds:
        flush_print('.', end='')
        sec += 1
        sleep(1)
    print()


class UnicodeException(Exception):
    def __init__(self, message=''):
        if isinstance(message, unicode):
            super(UnicodeException, self).__init__(message.encode('utf-8'))
            self.message = message
        elif isinstance(message, str):
            super(UnicodeException, self).__init__(message)
            self.message = message.decode('utf-8')
        else:
            raise TypeError

    def __unicode__(self):
        return self.message


def get_logger(name, loglevel='INFO', logfile='log.log'):
    INFO_OK = 15
    logging.addLevelName(INFO_OK, "INFO_OK")
    def info_ok(self, message, *args, **kwargs):
        if self.isEnabledFor(INFO_OK):
            self._log(INFO_OK, message, args, kwargs)
    logging.Logger.info_ok = info_ok
    logging.INFO_OK = INFO_OK
    logger = colorlog.getLogger(name)
    logger.setLevel(getattr(logging, loglevel))
    consoleHandler = colorlog.StreamHandler()
    consoleHandler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s[%(levelname)s] %(message)s',
        log_colors = {
            'DEBUG': 'cyan',
            'INFO_OK': 'green',
            'INFO': 'white',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red'
        }
    ))
    fileHandler = logging.FileHandler(logfile)
    fileHandler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)-15s: %(message)s'))
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    return logger

