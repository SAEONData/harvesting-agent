class AgentError(Exception):
    """Base class for all Agent errors."""


class ConfigError(AgentError):
    """Raised when there is a configuration file error."""


class CMSError(AgentError):
    """Raised when an error occurs in a call to the CMS."""


class HarvestingError(AgentError):
    """Raised when an error occurs during harvesting."""
