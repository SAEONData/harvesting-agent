import re
import json

import requests
from requests.exceptions import RequestException

from .granularity import Granularity
from .granule import Granule
from .exceptions import HarvestingError


class Curator:
    """
    Commits metadata records to a repository.
    """
    
    def __init__(self, schema, default_values, supplementary_values, granularity_dict,
                 repository_url, search_url, commit_url, username, password, institution):
        self.schema = schema
        self.default_values = default_values
        self.supplementary_values = supplementary_values
        self.granularity = Granularity(granularity_dict)
        self.repository_url = repository_url
        self.search_url = search_url
        self.commit_url = commit_url
        self.username = username
        self.password = password
        self.institution = institution

    @classmethod
    def supported_schemas(cls):
        """
        Get the supported metadata schemas for this Curator class.
        :return: iterable of supported schema strings
        """
        return ()

    @classmethod
    def create_metadata(cls, metadata_dict):
        """
        Create an appropriate concrete instance of Metadata.
        :param metadata_dict: dictionary of metadata values
        :return: a Metadata instance
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def commit_metadata(self, metadata_dict, datasource_id, record_id):
        """
        Create or update a metadata record in the repository.
        :param metadata_dict: dictionary of metadata values
        :param datasource_id: globally unique id of the originating datasource
        :param record_id: unique id of the metadata record with respect to its datasource
        :return: unique id of the created/updated metadata in the repository
        """
        new_metadata = self.create_metadata(metadata_dict)
        new_metadata.set_source_identifiers(datasource_id, record_id)
        self._apply_default_supplementary_values(new_metadata)

        granule = Granule(self.granularity, new_metadata)
        candidate_matches = self._find_candidate_matches(granule)
        existing_metadata = self._find_spatial_match(granule, candidate_matches)

        if existing_metadata is not None:
            existing_metadata.merge(new_metadata)
            return self._submit_metadata(existing_metadata, 'update')
        else:
            return self._submit_metadata(new_metadata, 'create')

    def _apply_default_supplementary_values(self, metadata):
        """
        Update a metadata record with default and supplementary values.
        :param metadata: Metadata instance to be updated
        """
        metadata_dict = metadata.metadata_dict
        for key, value in self.default_values.items():
            if key not in metadata_dict:
                metadata_dict[key] = value

        for key, value in self.supplementary_values.items():
            if key in metadata_dict:
                # only update if both source and supplementary values are lists
                if type(metadata_dict[key]) == list == type(value):
                    metadata_dict[key] += value
            else:
                metadata_dict[key] = value

    def _find_candidate_matches(self, granule):
        """
        Find existing metadata records which fall within the given granule.
        Since spatial filtering cannot be performed by the Plone jsonContent API,
        the result here is a list of candidate metadata instances, which must be
        spatially filtered locally.
        :param granule: the Granule encapsulating the match conditions
        :return: list of Metadata instances
        """
        params = {
            'types': 'Metadata',
            'depth': -1,
            'datasource_id': granule.datasource_id,
            '__ac_name': self.username,
            '__ac_password': self.password
        }

        if granule.values is not None:
            params['metadata_values'] = json.dumps(granule.values)
        if granule.starttime is not None and granule.endtime is not None:
            params['collected_date'] = '{}|{}'.format(granule.starttime, granule.endtime)

        # Something like the following could be used if spatial filtering were available via the search API:
        # if granule.polygon is not None:
        #     geojson = granule.polygon.ExportToJson()
        #     params['bounding_polygon'] = geojson

        try:
            response = requests.get(self.search_url, params)
            response.raise_for_status()
            records = response.json()
            return [self.create_metadata(metadata_rec['jsonData']) for metadata_rec in records]

        except RequestException as e:
            raise HarvestingError("Error requesting {}".format(self.search_url)) from e
        except (ValueError, TypeError, KeyError) as e:
            raise HarvestingError("Invalid response from {}".format(self.search_url)) from e

    def _find_spatial_match(self, granule, metadata_list):
        """
        Find the metadata instance in the given list that falls within the spatial boundary of the given granule.
        :param granule: the Granule encapsulating the match conditions
        :param metadata_list: list of Metadata instances
        :return: a Metadata instance, or None if not found
        """
        if granule.polygon is None:
            matches = metadata_list
        else:
            matches = []
            for metadata in metadata_list:
                try:
                    locations = metadata.get_location_geometries()
                except HarvestingError:
                    continue
                for location in locations:
                    if not location.Within(granule.polygon):
                        continue
                matches += [metadata]

        if not matches:
            return None
        if len(matches) > 1:
            raise HarvestingError("Multiple matching metadata records found")
        return matches[0]

    def _submit_metadata(self, metadata, method):
        """
        Create or update a metadata record in the repository.
        :param metadata: Metadata instance
        :param method: 'create' | 'update'
        :return: unique id of the created/updated metadata
        """
        def parse_uuid(message):
            match = re.search(r'\buuid=(\w+)\b', message)
            if match is not None:
                return match.group(1)
            else:
                return None

        id_, idtype = metadata.get_metadata_id()
        payload = {'json': json.dumps({
            'json': metadata.metadata_dict,
            'schema': self.schema,
            'mode': 'manual',
            'PID': id_,
            'typePID': idtype,
            'repository': {
                'URL': self.repository_url,
                'username': self.username,
                'password': self.password,
                'institution': self.institution,
            },
        })}
        try:
            if method == 'create':
                response = requests.post(self.commit_url, data=payload)
            elif method == 'update':
                response = requests.put(self.commit_url, data=payload)
            else:
                assert False, "Invalid method"

            response.raise_for_status()
            result = response.json()

            if result['success']:
                return parse_uuid(result['message'])
            else:
                verb = method[:-1] + 'ing'
                raise HarvestingError("Error {} metadata record in repository: {}".format(verb, result['message']))

        except RequestException as e:
            raise HarvestingError("Error {}'ing to {}".format(e.request.method, self.commit_url)) from e
        except ValueError as e:
            raise HarvestingError("Invalid response from {}".format(self.commit_url)) from e
