"""
A logging wrapper module for VRP Pipelines
"""
import json
import logging
import os
import traceback
from datetime import datetime

import json_logging
from pythonjsonlogger import jsonlogger


class CustomJSONLog(logging.Formatter):
    """
    Customized logger
    """

    @classmethod
    def parse_exc_info(cls, exc_cls, exc_obj, exc_traceback):
        """
        Helper method to parse exceptions.
        It returns a dict with type, args and traceback of an exception.
        param exc_cls - The type of the exception.
        param exc_obj - The instance of the exception.
        param exc_traceback - The traceback object of the exception
        return - Returns a dict with the following keys [type, args, trace]
        """
        return {
            'type': exc_cls.__module__ + "." + exc_cls.__name__,
            'args': ', '.join(
                exc_obj.args),
            'trace': ''.join(
                traceback.format_exception(
                    exc_cls,
                    exc_obj,
                    exc_traceback))}

    def format(self, record):
        json_log_object = {"@timestamp": datetime.utcnow().isoformat(),
                           "level": record.levelname,
                           "caller": record.filename + '::' + record.funcName,
                           "logger": record.name,
                           "thread_name": record.threadName,
                           }
        try:
            json_log_object["message"] = record.getMessage()
            if record.stack_info:
                json_log_object['stack_info'] = record.stack_info
            if record.exc_info:
                json_log_object['exc'] = self.parse_exc_info(*record.exc_info)
        except Exception as exc:
            json_log_object['logger_exception'] = self.parse_exc_info(
                exc.__class__, exc, exc.__traceback__)
        return json.dumps(json_log_object, skipkeys=True)


def full_logger(
        name: str,
        log_level: str = 'INFO',
        extra: dict = None,
        root_logger=None,
        file_name=None):
    """
    Helper method to build a logger.
    It returns a JSON enabled logger with kubernetes and elastic compliant logging format
    name - The logger name
    param name - Name of the logger
    param log_level - Level of logger, Default INFO else value picked from environment (LOG_LEVEL)
    param extra - any desired extra fields to add to default logging
    return - Logger instance
    """

    logging.basicConfig(filename=file_name)
    json_logging.init_non_web(enable_json=True, custom_formatter=CustomJSONLog)
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    json_logging.config_root_logger()
    logger.setLevel(os.getenv('LOG_LEVEL', default=logging.INFO))
    logger.addHandler(logging.StreamHandler())
    logger.propagate = True
    return logger


def simple_logger(log_level=None):
    """Returns a simple logger instance"""
    if log_level is None:
        log_level = 'INFO'  # Default to Info
    logger = logging.getLogger('')
    log_handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter()
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
