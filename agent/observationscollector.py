from traceback import format_exc
from pprint import pformat

import requests
from requests.exceptions import RequestException

from .collector import Collector
from .exceptions import HarvestingError
from .log import logger


class ObservationsCollector(Collector):

    @classmethod
    def supported_protocols(cls):
        """
        Get the supported transport protocols for this Collector class.
        :return: iterable of supported protocol strings
        """
        return ('ObservationsAPI',)

    @classmethod
    def supported_schemas(cls):
        """
        Get the supported metadata schemas for this Collector class.
        :return: iterable of supported schema strings
        """
        return ('DataCite',)

    def fetch_records(self, since=None, limit=None):
        """
        Fetch metadata records from the Observations DB web API.
        :param since: (optional) get only records with timestamp >= since
        :param limit: (optional) maximum number of records to fetch (not yet implemented)
        :return: list of dict {
                     uid: uniquely identifies the record in the Observations DB,
                     timestamp: timestamp of the record,
                     metadata: metadata dictionary (None if status != 'Success'),
                     status: 'Success' | 'Error',
                     error: message if status == 'Error'
                 }
        """
        records = []
        response_list = self._get_records(starttime=since)
        for response in response_list:
            try:
                records += [self._parse_response(response)]
            except Exception as e:
                logger.exception(e)

        return records

    def fetch_metadata(self, record_uid):
        """
        Fetch the metadata for a specific record.
        :param record_uid: uniquely identifies the record in the Observations DB
        :return: dict {
                     uid: record_uid,
                     timestamp: timestamp of the record,
                     metadata: metadata dictionary (None if status == 'Error'),
                     status: 'Success' | 'Error',
                     error: message if status == 'Error'
                 }
        """
        response = self._get_records(uid=record_uid)
        return self._parse_response(response)

    def _get_records(self, uid=None, starttime=None, endtime=None):
        """
        Make a request to the web API.
        :param uid: (optional) get only the record with this uid
        :param starttime: (optional) get only records with timestamp >= starttime
        :param endtime: (optional) get only records with timestamp <= endtime
        :return: deserialized JSON response dict
        """
        url = self.url + '/Metadata'
        if uid is not None:
            url += '/{}'.format(uid)
        if starttime is not None:
            url += '/{}'.format(starttime.isoformat())
        if endtime is not None:
            url += '/{}'.format(endtime.isoformat())
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            raise HarvestingError("Error requesting {}".format(url)) from e
        except ValueError as e:
            raise HarvestingError("Invalid response from {}".format(url)) from e

    def _parse_response(self, response):
        """
        Convert a response record from the web API into a collector record.
        :param response: dict representing a single record from a web API result set
        :return: dict of the same form as returned by fetch_metadata()
        """
        uid = response.pop('id', None)
        timestamp = response.pop('timestamp', None)
        if uid is not None:
            try:
                record = {
                    'uid': uid,
                    'timestamp': timestamp,
                    'metadata': self._construct_metadata(response),
                    'status': 'Success',
                    'error': None,
                }
                logger.debug("Fetched metadata for %s from %s", response['id'], self.url)
            except Exception as e:
                record = {
                    'uid': uid,
                    'timestamp': timestamp,
                    'metadata': None,
                    'status': 'Error',
                    'error': format_exc(),
                }
                logger.exception("Error fetching metadata for %s from %s: %s", response['id'], self.url, e)

            return record
        else:
            raise HarvestingError("Invalid record received from {}: {}".format(self.url, pformat(response)))

    def _construct_metadata(self, response):
        """
        Construct a DataCite metadata dictionary from a web API response record.
        :param response: dict representing a single record from a web API result set
        :return: dict
        """
        dc = {}
        return dc
