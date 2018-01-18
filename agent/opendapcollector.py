from urllib.parse import urljoin
from traceback import format_exc

import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from pydap.parsers.das import parse_das
from dateutil.parser import parse

from .collector import Collector
from .exceptions import HarvestingError
from .log import logger


class OPeNDAPCollector(Collector):

    def fetch_records(self, since=None, limit=None):
        """
        Get all filenames from the datasource.
        See base class for complete method documentation.
        :param since: ignored as file timestamps are not available via OPeNDAP
        :param limit: (optional) maximum number of records to fetch
        """
        records = []
        response_text = self._request('')
        soup = BeautifulSoup(response_text, 'html.parser')
        exts = self._valid_file_extensions()
        tags = soup.find_all('a', itemprop='contentUrl',
                             href=lambda url: next((True for ext in exts if url.lower().endswith(ext + '.das')), False))
        filelist = sorted(set([tag['href'][:-4] for tag in tags]))
        for n, filename in enumerate(filelist, 1):
            records += [{
                'uid': filename,
                'timestamp': None,
                'metadata': None,
                'status': 'Pending',
                'error': None,
            }]
            if limit and n == limit:
                break
        logger.debug("Found %d data files at %s", len(records), self.url)
        return records

    def fetch_metadata(self, record_uid):
        """
        Fetch the metadata for a single NetCDF file.
        See base class for complete method documentation.
        """
        try:
            record = {
                'uid': record_uid,
                'timestamp': None,
                'metadata': self._get_datacite(record_uid),
                'status': 'Success',
                'error': None,
            }
            logger.debug("Fetched metadata for %s from %s", record_uid, self.url)
        except Exception as e:
            record = {
                'uid': record_uid,
                'timestamp': None,
                'metadata': None,
                'status': 'Error',
                'error': format_exc(),
            }
            logger.exception("Error fetching metadata for %s from %s: %s", record_uid, self.url, e)

        return record

    def _request(self, path):
        """
        Make a request to the datasource.
        :param path: path to server resource (absolute or relative)
        :return: response text
        """
        url = urljoin(self.url, path)
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except RequestException as e:
            raise HarvestingError("Error requesting {}".format(url)) from e

    @classmethod
    def _valid_file_extensions(cls):
        """
        Get a list of valid datafile extensions for this Collector class.
        :return: iterable of lowercase strings including period e.g. ('.nc',)
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def _get_das(self, filename):
        """
        Get the DAS (Dataset Attribute Structure) for a dataset.
        :param filename: datafile name (e.g. datafile.nc)
        :return: dict
        """
        response_text = self._request(filename + '.das')
        try:
            return parse_das(response_text)
        except Exception as e:
            raise HarvestingError("Error parsing Data Attribute Structure for {}".format(filename)) from e

    def _get_datacite(self, filename):
        """
        Get the DataCite metadata for a NetCDF file.
        :param filename: datafile name (e.g. datafile.nc)
        :return: dict
        """
        raise NotImplementedError("Method must be overridden in descendants")


class NetCDFCollector(OPeNDAPCollector):

    @classmethod
    def supported_protocols(cls):
        """
        Get the supported transport protocols for this Collector class.
        :return: iterable of supported protocol strings
        """
        return ('OPeNDAP-NetCDF',)

    @classmethod
    def supported_schemas(cls):
        """
        Get the supported metadata schemas for this Collector class.
        :return: iterable of supported schema strings
        """
        return ('DataCite',)

    @classmethod
    def _valid_file_extensions(cls):
        """
        Get a list of valid datafile extensions for this Collector class.
        :return: iterable of lowercase strings including period e.g. ('.nc',)
        """
        return ('.nc',)

    def _get_datacite(self, filename):
        """
        Get the DataCite metadata for a NetCDF file.
        :param filename: datafile name (e.g. datafile.nc)
        :return: dict
        """
        assert self.schema == 'DataCite', "This method only works for DataCite"

        def ncval(field):
            value = nc.get(field, None)
            if type(value) is str:
                value = value.strip()
            return value

        def nctime(field):
            value = ncval(field)
            if not value:
                return None
            try:
                return parse(value)
            except (ValueError, OverflowError) as e:
                raise HarvestingError("Invalid date/time for {}".format(field)) from e

        def nclist(field, separator, relatedcount=None, relatedfield=None):
            if field in nc:
                values = [value.strip() for value in nc[field].split(separator)]
                if relatedcount is not None and len(values) != relatedcount:
                    raise HarvestingError("{} and {} must contain the same number of values separated by {}".
                                         format(relatedfield, field, separator))
                return values
            else:
                return []

        das = self._get_das(filename)
        try:
            nc = das['NC_GLOBAL']
        except KeyError:
            raise HarvestingError("'NC_GLOBAL' not found in Data Attribute Structure")

        id_ = ncval('id')
        title = ncval('title')
        summary = ncval('summary')
        keywords = nclist('keywords', ',')
        creator_names = nclist('creator_name', ';')
        creator_institutions = nclist('creator_institution', ';', len(creator_names), 'creator_name')
        contributor_names = nclist('contributor_name', ';')
        contributor_institutions = nclist('contributor_institution', ';', len(contributor_names), 'contributor_name')
        publisher_name = ncval('publisher_name')
        date_issued = nctime('date_issued')
        time_coverage_start = nctime('time_coverage_start')
        time_coverage_end = nctime('time_coverage_end')
        geospatial_lat_min = ncval('geospatial_lat_min')
        geospatial_lon_min = ncval('geospatial_lon_min')
        geospatial_lat_max = ncval('geospatial_lat_max')
        geospatial_lon_max = ncval('geospatial_lon_max')

        dc = {
            'resourceType': 'NetCDF',
            'resourceTypeGeneral': 'Dataset',
            'additionalFields': {
                'onlineResources': [{
                    'href': urljoin(self.url, filename)
                }]
            }
        }

        if id_:
            dc['alternateIdentifiers'] = [{
                'alternateIdentifier': id_,
                'alternateIdentiferType': 'DatasetID'
            }]

        if title:
            dc['titles'] = [{
                'title': title
            }]

        if summary:
            dc['description'] = [{
                'description': summary,
                'descriptionType': 'Abstract'
            }]

        if keywords:
            dc['subjects'] = [{
                'subject': keyword
            } for keyword in keywords]

        if creator_names:
            dc['creators'] = [{
                'creatorName': creator_name
            } for creator_name in creator_names]

            for i, creator_institution in enumerate(creator_institutions):
                dc['creators'][i]['affiliation'] = creator_institution

        if contributor_names:
            dc['contributors'] = [{
                'contributorName': contributor_name
            } for contributor_name in contributor_names]

            for i, contributor_institution in enumerate(contributor_institutions):
                dc['contributors'][i]['affiliation'] = contributor_institution

        if publisher_name:
            dc['publisher'] = publisher_name

        if date_issued:
            dc['publicationYear'] = date_issued.year

        if time_coverage_start and time_coverage_end:
            dc['dates'] = [{
                'date': '{}/{}'.format(time_coverage_start.isoformat(), time_coverage_end.isoformat()),
                'dateType': 'Collected'
            }]

        if geospatial_lat_min and geospatial_lon_min and geospatial_lat_max and geospatial_lon_max:
            dc['geoLocations'] = [{
                'geoLocationBox': '{} {} {} {}'.format(
                    geospatial_lat_min, geospatial_lon_min, geospatial_lat_max, geospatial_lon_max)
            }]

        return dc
