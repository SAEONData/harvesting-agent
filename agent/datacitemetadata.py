from decimal import Decimal

from isodate import parse_datetime
from isodate.isoerror import ISO8601Error

from .metadata import Metadata
from .exceptions import HarvestingError


class DataCiteMetadata(Metadata):

    def set_source_identifiers(self, datasource_id, record_id):
        """
        Set the 'DatasourceID' and 'RecordID' alternate identifiers in the metadata dictionary.
        :param datasource_id: globally unique id of the originating datasource
        :param record_id: unique id of the metadata record with respect to its datasource
        """
        if 'alternateIdentifiers' not in self.metadata_dict:
            self.metadata_dict['alternateIdentifiers'] = []

        datasource_element = next((alt_id for alt_id in self.metadata_dict['alternateIdentifiers']
                                   if alt_id['alternateIdentiferType'] == 'DatasourceID'), None)
        if datasource_element is not None:
            datasource_element['alternateIdentifier'] = datasource_id
        else:
            self.metadata_dict['alternateIdentifiers'] += [{
                'alternateIdentifier': datasource_id,
                'alternateIdentiferType': 'DatasourceID'
            }]

        record_element = next((alt_id for alt_id in self.metadata_dict['alternateIdentifiers']
                               if alt_id['alternateIdentiferType'] == 'RecordID'), None)
        if record_element is not None:
            record_element['alternateIdentifier'] = record_id
        else:
            self.metadata_dict['alternateIdentifiers'] += [{
                'alternateIdentifier': record_id,
                'alternateIdentiferType': 'RecordID'
            }]

    def get_metadata_id(self):
        """
        Get the primary identifier and its type from the metadata dictionary.
        :return: tuple(id, idtype)
        """
        id_ = self.metadata_dict.get('identifier', None)
        if type(id_) == dict:
            return id_['identifier'], id_['identifierType']
        else:
            return '', 'DOI'

    def get_datasource_id(self):
        """
        Get the alternate identifier of type 'DatasourceID' from the metadata dictionary,
        which indicates the origin of the metadata.
        :return: string
        """
        alt_ids = self.metadata_dict.get('alternateIdentifiers', [])
        datasource_id = next((alt_id['alternateIdentifier'] for alt_id in alt_ids
                              if alt_id['alternateIdentiferType'] == 'DatasourceID'), None)
        if datasource_id is None:
            raise HarvestingError("Metadata contains no alternate identifier of type 'DatasourceID'")
        return datasource_id

    def get_collected_dates(self):
        """
        Get the "collected" dates from the metadata dictionary. This should be a start/end pair, but if it happens to
        be a single datetime string, we expand it to be a pair.
        :return: 2-element list of datetimes
        """
        collected_date_records = [daterec['date'] for daterec in self.metadata_dict.get('dates', [])
                                  if daterec['dateType'] == 'Collected']
        if not collected_date_records:
            raise HarvestingError("Metadata contains no collected date element")
        if len(collected_date_records) > 1:
            raise HarvestingError("Metadata contains too many collected date elements")

        collected_dates = collected_date_records[0].split('/')
        if len(collected_dates) > 2:
            raise HarvestingError("Collected date element contains too many parts")
        try:
            result = [parse_datetime(collected_date) for collected_date in collected_dates]
            if len(result) == 1:
                result += [result[0]]
            return result
        except ISO8601Error as e:
            raise HarvestingError("Error parsing collected date from metadata") from e

    def set_collected_dates(self, collected_dates):
        """
        Update the "collected" dates in the metadata dictionary.
        :param collected_dates: 2-element list of datetimes
        """
        assert len(collected_dates) == 2, "collected_dates must be a 2-element list"
        collected_date_record = next((daterec for daterec in self.metadata_dict.get('dates', [])
                                      if daterec['dateType'] == 'Collected'), None)
        assert collected_date_record, "WTF happened to the collected date"

        collected_date_strings = [collected_date.isoformat() for collected_date in collected_dates]
        collected_date_record['date'] = '/'.join(collected_date_strings)

    def get_geolocations(self):
        """
        Get the geolocations from the metadata in a usable form.
        :return: list of dicts
        """
        geolocations = []
        for geolocation in self.metadata_dict.get('geoLocations', []):
            place = geolocation.get('geoLocationPlace', None)
            if 'geoLocationPoint' in geolocation:
                lat, lon = geolocation['geoLocationPoint'].split()
                geolocations += [{
                    'type': 'point',
                    'place': place,
                    'lat': Decimal(lat),
                    'lon': Decimal(lon),
                }]
            if 'geoLocationBox' in geolocation:
                lat1, lon1, lat2, lon2 = geolocation['geoLocationBox'].split()
                geolocations += [{
                    'type': 'box',
                    'place': place,
                    'lat1': Decimal(lat1),
                    'lon1': Decimal(lon1),
                    'lat2': Decimal(lat2),
                    'lon2': Decimal(lon2),
                }]
        return geolocations

    def set_geolocations(self, geolocations):
        """
        Update the geolocations in the metadata dictionary.
        :param geolocations: list of dict of the same form as returned by get_geolocations()
        """
        elements = []
        for geolocation in geolocations:
            if geolocation['type'] == 'point':
                element = {
                    'geoLocationPoint': '{} {}'.format(geolocation['lat'], geolocation['lon'])
                }
            elif geolocation['type'] == 'box':
                element = {
                    'geoLocationBox': '{} {} {} {}'.format(geolocation['lat1'], geolocation['lon1'],
                                                           geolocation['lat2'], geolocation['lon2'])
                }
            else:
                continue
            if geolocation['place']:
                element['geoLocationPlace'] = geolocation['place']
            elements += [element]

        self.metadata_dict['geoLocations'] = elements
