import pkg_resources

from . import config

config.read_config(pkg_resources.resource_filename(__name__, '../config/agent.ini'))
