import configparser
import logging
import os
import pkg_resources

from .exceptions import ConfigError


DB_HOST = None
DB_NAME = None
DB_USER = None
DB_PASS = None

LOG_DIR = None
LOG_LEVEL = None

CMS_URL = None


_defaults = {
    'DBHost': 'localhost',
    'DBName': 'agentdb',
    'DBUser': 'agent',
    'DBPass': 'agent',
    'LogDir': pkg_resources.resource_filename(__name__, '../log'),
    'LogLevel': 'INFO',
    'CMSUrl': 'http://localhost:8080/Plone',
}


def read_config(filename):

    def getvalue(key):
        return config.get('Agent', key, fallback=_defaults[key])

    global DB_HOST
    global DB_NAME
    global DB_USER
    global DB_PASS

    global LOG_DIR
    global LOG_LEVEL

    global CMS_URL

    if not os.path.isfile(filename):
        raise ConfigError("Configuration file {} not found".format(filename))

    config = configparser.ConfigParser()
    try:
        config.read(filename)

        DB_HOST = getvalue('DBHost')
        DB_NAME = getvalue('DBName')
        DB_USER = getvalue('DBUser')
        DB_PASS = getvalue('DBPass')

        LOG_DIR = getvalue('LogDir')
        if not os.path.isabs(LOG_DIR):
            raise ConfigError("LogDir must be an absolute path")
        LOG_LEVEL = int(logging.getLevelName(getvalue('LogLevel')))

        CMS_URL = getvalue('CMSUrl')

    except Exception as e:
        raise ConfigError("Error in configuration file {}".format(filename)) from e
