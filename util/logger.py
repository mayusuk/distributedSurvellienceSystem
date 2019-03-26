import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os


class Logger:

    def __init__(self):
        self.FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
        self.LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__name__)), "log")

    def __get_console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.FORMATTER)
        return console_handler

    def __get_file_handler(self):
        file_handler = TimedRotatingFileHandler(self.LOG_FILE, when='MIDNIGHT')
        file_handler.setFormatter(self.FORMATTER)
        return file_handler

    def get_logger(self,logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG) # better to have too much log than not enough
        logger.addHandler(self.__get_console_handler())
        logger.addHandler(self.__get_file_handler())
        # with this pattern, it's rarely necessary to propagate the error up to parent
        logger.propagate = False
        return logger