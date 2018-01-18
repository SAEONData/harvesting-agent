from .opendapcollector import NetCDFCollector
from .exceptions import HarvestingError


_registered_collector_classes = (
    NetCDFCollector,
)


class CollectorFactory:

    @staticmethod
    def create_collector(protocol, schema, url, username, password):
        """
        Find a matching Collector class for the given protocol and schema, and create an instance of it.
        :return: a Collector instance
        """
        for cls in _registered_collector_classes:
            if protocol in cls.supported_protocols() and schema in cls.supported_schemas():
                return cls(protocol, schema, url, username, password)

        raise HarvestingError("No Collector class found that supports protocol '{}' and schema '{}'".
                              format(protocol, schema))
