from datetime import datetime, timedelta

from .exceptions import HarvestingError
from .gdalwrapper import PolygonSource
from .log import logger


class Granularity:
    """
    Represents the granularity specification for a metadata collection.
    """

    def __init__(self, granularity_dict):
        """
        Create a Granularity instance from a dictionary.
        """
        self.keys = granularity_dict.get('key_fields', None)
        self.period = granularity_dict.get('temporal_extent', None)
        self.geojson_url = granularity_dict.get('spatial_extent', None)

        if self.keys is not None and type(self.keys) != list:
            raise ValueError("Invalid granularity 'key_fields' element; expecting list")
        if self.period is not None and self.period not in ('hour', 'day', 'month', 'year'):
            raise ValueError("Invalid value for granularity 'temporal_extent' element")

        if self.geojson_url:
            logger.debug("Loading polygons from %s", self.geojson_url)
            self.polygonsource = PolygonSource(self.geojson_url)
        else:
            self.polygonsource = None

    def get_timeframe(self, times):
        """
        Get the time frame (of length period) that contains the given datetime values.
        Note that the end time is equal to the start time of the next granularity period.
        :param times: list of datetimes
        :return: tuple(starttime, endtime)
        """
        assert self.period, "Granularity period has not been set"
        assert times, "No datetime values given"

        start = None
        end = None

        for time in times:
            if self.period == 'hour':
                starttime = datetime(time.year, time.month, time.day, time.hour)
                endtime = starttime + timedelta(hours=1)
            elif self.period == 'day':
                starttime = datetime(time.year, time.month, time.day)
                endtime = starttime + timedelta(days=1)
            elif self.period == 'month':
                starttime = datetime(time.year, time.month)
                endyear = time.year
                endmonth = time.month + 1
                if endmonth == 13:
                    endyear += 1
                    endmonth = 1
                endtime = datetime(endyear, endmonth)
            elif self.period == 'year':
                starttime = datetime(time.year)
                endtime = datetime(time.year + 1)
            else:
                assert False

            if (start is not None and start != starttime) or (end is not None and end != endtime):
                raise HarvestingError("Metadata dates span multiple granularity periods")
            start = starttime
            end = endtime

        return start, end

    def get_polygon(self, locations):
        """
        Get the bounding polygon for the given set of locations.
        :param locations: list of OGR geometries
        :return: OGR polygon geometry
        """
        assert self.polygonsource, "Granularity polygonsource has not been set"
        return self.polygonsource.get_polygon(locations)
