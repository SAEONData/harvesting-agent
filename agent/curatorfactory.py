from .datacitecurator import DataCiteCurator
from .exceptions import HarvestingError


_registered_curator_classes = (
    DataCiteCurator,
)


class CuratorFactory:

    @staticmethod
    def create_curator(schema, default_values, supplementary_values, granularity_dict,
                       repository_url, search_url, commit_url, username, password, institution):
        """
        Find a matching Curator class for the given schema, and create an instance of it.
        :return: a Curator instance
        """
        for cls in _registered_curator_classes:
            if schema in cls.supported_schemas():
                return cls(schema, default_values, supplementary_values, granularity_dict,
                           repository_url, search_url, commit_url, username, password, institution)

        raise HarvestingError("No Curator class found that supports schema '{}'".format(schema))
