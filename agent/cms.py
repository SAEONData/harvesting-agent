from urllib.parse import urljoin

import requests
from requests.exceptions import RequestException

from .exceptions import CMSError


class CMS:
    """
    Represents the Content Management System.
    """

    def __init__(self, server_url, auth_user, auth_pass):
        if not server_url.endswith('/'):
            server_url += '/'
        self.server_url = server_url
        self.auth_user = auth_user
        self.auth_pass = auth_pass

    def get_harvester_config(self, harvester_uid):
        """
        Get the config for a harvester.
        :param harvester_uid: harvester uid in Plone
        :return: dict of harvester attributes
        """
        dictlist = self._request('jsonContent', types='Harvester', uid=harvester_uid)
        if type(dictlist) != list or len(dictlist) != 1 or type(dictlist[0]) != dict:
            raise CMSError("Cannot find a harvester with uid {}".format(harvester_uid))
        return dictlist[0]

    def _request(self, path, **kwargs):
        """
        Make a request to the server.
        :param path: path to a server resource (absolute or relative)
        :param kwargs: query params
        :return: a dict or list which is the deserialized JSON response
        """
        url = urljoin(self.server_url, path)
        try:
            params = kwargs
            params['__ac_name'] = self.auth_user
            params['__ac_password'] = self.auth_pass
            response = requests.get(url, params)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            raise CMSError("Error requesting {}".format(url)) from e
        except ValueError as e:
            raise CMSError("Invalid response from {}".format(url)) from e
