from osgeo import gdal, ogr

gdal.UseExceptions()


def create_point(lon, lat):
    """
    Return an OGR point representing the specified geographic location.
    :param lon: longitude
    :param lat: latitude
    :return: OGR geometry
    """
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(lon, lat)
    return point


def create_box(lon1, lat1, lon2, lat2):
    """
    Return an OGR polygon representing the specified geographic rectangle.
    :param lon1: west longitude
    :param lat1: south latitude
    :param lon2: east longitude
    :param lat2: north latitude
    :return: OGR geometry
    """
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(lon1, lat1)
    ring.AddPoint(lon2, lat1)
    ring.AddPoint(lon2, lat2)
    ring.AddPoint(lon1, lat2)
    ring.AddPoint(lon1, lat1)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


class PolygonSource:

    def __init__(self, geojson_url):
        """
        Instantiate this class, loading all polygon data from the specified remote location.
        :param geojson_url: URL providing GeoJSON-format polygons
        """
        self._datasource = ogr.GetDriverByName('GeoJSON').Open(geojson_url)
        # we need to hold onto references to these objects to prevent GDAL from cleaning them up,
        # otherwise seg faults will ensue when polygons returned from the methods below are accessed
        self._layer = self._datasource.GetLayer()
        self._features = set()

    def get_polygons(self, locations):
        """
        Get the bounding polygon for each of the given locations.
        :param locations: list of OGR geometries (points and/or boxes)
        :return: dict with location-polygon pairs
        """
        if not locations:
            return {}

        result = dict.fromkeys(locations)

        found = 0
        for feature in self._layer:
            polygon = feature.GetGeometryRef()
            if polygon.GetGeometryType() != ogr.wkbPolygon:
                continue
            for location in locations:
                if result[location] is not None:
                    continue
                if location.Within(polygon):
                    result[location] = polygon
                    self._features |= {feature}
                    found += 1
            if found == len(locations):
                break

        return result

    def get_polygon(self, locations):
        """
        Get the bounding polygon for the given set of locations.
        An exception is raised if exactly one polygon cannot be found.
        :param locations: list of OGR geometries (points and/or boxes)
        :return: OGR polygon geometry
        """
        assert locations, "No location(s) given"
        location = locations[0]

        for feature in self._layer:
            polygon = feature.GetGeometryRef()
            if polygon.GetGeometryType() != ogr.wkbPolygon:
                continue
            if location.Within(polygon):
                for _, other_location in enumerate(locations, 1):
                    if not other_location.Within(polygon):
                        raise Exception("Locations do not all fall within one polygon")
                self._features |= {feature}
                return polygon

        raise Exception("Could not find bounding polygon for the given location(s)")
