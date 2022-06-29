import configparser
from dynaconf import settings
import logging.config

logging.config.fileConfig(settings.LOGGER.LOG_CONF_FILE, defaults={'logfilename': settings.LOGGER.LOG_FILE}, disable_existing_loggers=False)

log = logging.getLogger("simpleTask")
log.propagate = False
