from .exceptions import HarvestingError
from .gdalwrapper import create_point, create_box


class Metadata:
    """
    Abstract base class encapsulating a metadata dictionary.
    """

    def __init__(self, metadata_dict):
        """
        Initialize a Metadata instance.
        :param metadata_dict: dictionary of metadata values
        """
        self.metadata_dict = metadata_dict

    def merge(self, new_metadata):
        """
        Update this metadata instance with new information.
        NB: Of all the metadata values retrieved from a new record, only those related to granularity are
        effectively merged in. Other kinds of metadata values are implicitly assumed to be unchanged.
        :param new_metadata: metadata instance originating from a new record in the remote datastore
        """
        collected_dates = self.get_collected_dates()
        new_collected_dates = new_metadata.get_collected_dates()
        self.set_collected_dates([
            min(collected_dates[0], new_collected_dates[0]),
            max(collected_dates[1], new_collected_dates[1]),
        ])

        update_geolocations = False
        geolocations = self.get_geolocations()
        new_geolocations = new_metadata.get_geolocations()
        for new_geolocation in new_geolocations:
            if new_geolocation not in geolocations:
                geolocations += [new_geolocation]
                update_geolocations = True
        if update_geolocations:
            self.set_geolocations(geolocations)

    def set_source_identifiers(self, datasource_id, record_id):
        """
        Set the 'DatasourceID' and 'RecordID' alternate identifiers in the metadata dictionary.
        :param datasource_id: globally unique id of the originating datasource
        :param record_id: unique id of the metadata record with respect to its datasource
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def get_metadata_id(self):
        """
        Get the primary identifier and its type from the metadata dictionary.
        :return: tuple(id, idtype)
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def get_datasource_id(self):
        """
        Get the alternate identifier of type 'DatasourceID' from the metadata dictionary,
        which indicates the origin of the metadata.
        :return: string
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def get_value(self, key):
        """
        Get the value of the metadata element identified by 'key'.
        :param key: a top-level DataCite element name
        :return: the value for key; a string or a nested list/dict
        """
        value = self.metadata_dict.get(key, None)
        if value is None:
            raise HarvestingError("Metadata contains no '{}' element".format(key))
        return value

    def get_collected_dates(self):
        """
        Get the "collected" dates from the metadata dictionary. This should be a start/end pair, but if it happens to
        be a single datetime string, we expand it to be a pair.
        :return: 2-element list of datetimes
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def set_collected_dates(self, collected_dates):
        """
        Update the "collected" dates in the metadata dictionary.
        :param collected_dates: 2-element list of datetimes
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def get_geolocations(self):
        """
        Get the geolocations from the metadata in a usable form.
        :return: list of dicts in one of two forms:
            [
                {
                    'type': 'point',
                    'place': place_str,
                    'lat': Decimal(lat),
                    'lon': Decimal(lon),
                },
                {
                    'type': 'box',
                    'place': place_str,
                    'lat1': Decimal(lat1),
                    'lon1': Decimal(lon1),
                    'lat2': Decimal(lat2),
                    'lon2': Decimal(lon2),
                }
            ]
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def set_geolocations(self, geolocations):
        """
        Update the geolocations in the metadata dictionary.
        :param geolocations: list of dict of the same form as returned by get_geolocations()
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def get_location_geometries(self):
        """
        Get the location geometries from the metadata dictionary.
        An exception is raised if the metadata contains no locations.
        :return: list of OGR geometries (points and/or polygons)
        """
        geometries = []
        try:
            for geolocation in self.get_geolocations():
                if geolocation['type'] == 'point':
                    point = create_point(geolocation['lon'], geolocation['lat'])
                    geometries += [point]
                elif geolocation['type'] == 'box':
                    box = create_box(geolocation['lon1'], geolocation['lat1'],
                                     geolocation['lon2'], geolocation['lat2'])
                    geometries += [box]

        except Exception as e:
            raise HarvestingError("Error parsing geolocation info from metadata") from e

        if not geometries:
            raise HarvestingError("Metadata contains no geolocation info")

        return geometries
