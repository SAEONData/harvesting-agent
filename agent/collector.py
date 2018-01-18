class Collector:
    """
    Fetches metadata records from a datasource.
    """

    def __init__(self, protocol, schema, url, username, password):
        if not url.endswith('/'):
            url += '/'
        self.protocol = protocol
        self.schema = schema
        self.url = url
        self.username = username
        self.password = password

    @classmethod
    def supported_protocols(cls):
        """
        Get the supported transport protocols for this Collector class.
        :return: iterable of supported protocol strings
        """
        return ()

    @classmethod
    def supported_schemas(cls):
        """
        Get the supported metadata schemas for this Collector class.
        :return: iterable of supported schema strings
        """
        return ()

    def fetch_records(self, since=None, limit=None):
        """
        Fetch metadata records from the datasource.
        :param since: (optional) get only records added to the remote datastore since this time.
                      This should be considered a performance hint; it won't necessarily work
                      in all concrete Collector implementations.
        :param limit: (optional) maximum number of records to fetch
        :return: list of dict {
                     uid: uniquely identifies the object in the remote datastore,
                     timestamp: timestamp of the remote object (if available),
                     metadata: metadata dictionary (empty if status != 'Success'),
                     status: 'Pending' (metadata must be explicitly fetched later) | 'Success' | 'Error',
                     error: message if status == 'Error'
                 }
        """
        raise NotImplementedError("Method must be overridden in descendants")

    def fetch_metadata(self, record_uid):
        """
        Fetch the metadata for a specific record.
        :param record_uid: uniquely identifies the file/record in the remote datastore
        :return: dict {
                     uid: record_uid,
                     timestamp: timestamp of the remote object (if available),
                     metadata: metadata dictionary (empty if status == 'Error'),
                     status: 'Success' | 'Error',
                     error: message if status == 'Error'
                 }
        """
        raise NotImplementedError("Method must be overridden in descendants")
