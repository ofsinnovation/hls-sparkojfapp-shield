import logging
from logging.handlers import RotatingFileHandler
import os

logger = logging.getLogger('ihealthservices')
logger.setLevel(logging.DEBUG)

log_dir = os.path.join(os.getcwd(), 'logs')
log_file_name = 'ihealth-services.log'
print('Trying to load the following config file ===> ')
print(os.path.realpath(log_dir + os.sep + log_file_name))

# create file handler which logs even debug messages
fh = RotatingFileHandler(os.path.realpath(log_dir + os.sep + log_file_name))

fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


def info(message):
    if logger.isEnabledFor(logging.INFO):
        logger.info(message)


def error(message):
    if logger.isEnabledFor(logging.ERROR):
        logger.error(message)


def debug(message):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(message)


