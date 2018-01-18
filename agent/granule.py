class Granule:
    """
    Represents a mutually exclusive "unit" of granularity with specific boundary and key values.
    """

    def __init__(self, granularity, metadata):
        """
        Instantiate a granule of the specified granularity suitable for the given metadata record.
        :param granularity: a Granularity instance
        :param metadata: a Metadata instance
        """
        self.datasource_id = metadata.get_datasource_id()
        self.values = None
        self.starttime = None
        self.endtime = None
        self.polygon = None

        if granularity.keys is not None:
            self.values = {}
            for key in granularity.keys:
                self.values[key] = metadata.get_value(key)

        if granularity.period is not None:
            collected_dates = metadata.get_collected_dates()
            self.starttime, self.endtime = granularity.get_timeframe(collected_dates)

        if granularity.polygonsource is not None:
            locations = metadata.get_location_geometries()
            self.polygon = granularity.get_polygon(locations)
