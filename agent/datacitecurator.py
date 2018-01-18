from .curator import Curator
from .datacitemetadata import DataCiteMetadata


class DataCiteCurator(Curator):

    @classmethod
    def supported_schemas(cls):
        """
        Get the supported metadata schemas for this Collector class.
        :return: iterable of supported schema strings
        """
        return ('DataCite',)

    @classmethod
    def create_metadata(cls, metadata_dict):
        """
        Create an appropriate concrete instance of Metadata.
        :param metadata_dict: dictionary of metadata values
        :return: a Metadata instance
        """
        return DataCiteMetadata(metadata_dict)
