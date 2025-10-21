import logging
from osgeo import gdal, osr
import numpy as np

logger = logging.getLogger(__name__)


def transform_coordinates(x, y, source_epsg, target_epsg):
    """
    Transform coordinates from one projection to another.

    Parameters:
        x (float or numpy.ndarray): X coordinate(s) in the source projection
        y (float or numpy.ndarray): Y coordinate(s) in the source projection
        source_epsg (int): EPSG code of the source projection
        target_epsg (int): EPSG code of the target projection

    Returns:
        tuple: (x_transformed, y_transformed) coordinates in the target projection
    """
    if source_epsg == target_epsg:
        return float(x), float(y)
    # Create source and target spatial reference systems
    source_srs = osr.SpatialReference()
    source_srs.ImportFromEPSG(source_epsg)

    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(target_epsg)

    # This is needed for the transform to work correctly or else returned points will be y, x (lat, lon) instead of x, y (lon, lat)
    # https://gis.stackexchange.com/questions/421771/ogr-coordinatetransformation-appears-to-be-inverting-xy-coordinates
    target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    # Create coordinate transformation
    transform = osr.CoordinateTransformation(source_srs, target_srs)

    # Handle both single coordinates and arrays
    if isinstance(x, np.ndarray) and isinstance(y, np.ndarray):
        # For arrays, process in batches
        x_transformed = np.zeros_like(x)
        y_transformed = np.zeros_like(y)

        for i in range(len(x)):
            point = transform.TransformPoint(float(x[i]), float(y[i]))
            x_transformed[i] = point[0]
            y_transformed[i] = point[1]

        return x_transformed, y_transformed
    else:
        # For single coordinates
        point = transform.TransformPoint(float(x), float(y))
        return point[0], point[1]


def transform_bbox(xmin, ymin, xmax, ymax, source_epsg, target_epsg):
    """
    Transform a bounding box from one projection to another.

    Parameters:
        xmin, ymin, xmax, ymax (float): Coordinates of the bounding box in source projection
        source_epsg (int): EPSG code of the source projection
        target_epsg (int): EPSG code of the target projection

    Returns:
        tuple: (xmin, ymin, xmax, ymax) of the bounding box in target projection
    """
    # Transform the corners
    x_corners = [xmin, xmax, xmax, xmin]
    y_corners = [ymin, ymin, ymax, ymax]

    x_transformed = []
    y_transformed = []

    for i in range(4):
        x_t, y_t = transform_coordinates(
            x_corners[i], y_corners[i], source_epsg, target_epsg
        )
        x_transformed.append(x_t)
        y_transformed.append(y_t)

    # Get the min/max of the transformed coordinates
    return (
        min(x_transformed),
        min(y_transformed),
        max(x_transformed),
        max(y_transformed),
    )
