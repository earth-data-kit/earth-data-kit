from osgeo import osr


def transform_coordinates(x, y, source_epsg, target_epsg):
    """
    Transform coordinates from one projection to another.

    Args:
        x (float): X coordinate in source projection.
        y (float): Y coordinate in source projection.
        source_epsg (int): EPSG code of the source projection.
        target_epsg (int): EPSG code of the target projection.

    Returns:
        (float, float): Transformed X, Y coordinates in the target projection.
    """
    # Create source spatial reference
    source_srs = osr.SpatialReference()
    source_srs.ImportFromEPSG(source_epsg)

    # Create target spatial reference
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(target_epsg)

    # Create the transformation object
    transform = osr.CoordinateTransformation(source_srs, target_srs)

    # Transform the point
    x_out, y_out, _ = transform.TransformPoint(x, y)
    return x_out, y_out


# Example usage
if __name__ == "__main__":
    # Coordinates in EPSG:4326 (WGS 84)
    x, y = -71.873, 177.431  # Longitude, Latitude (Rome, Italy)

    source_epsg = 4326
    target_epsg = 32760

    transformed_x, transformed_y = transform_coordinates(x, y, source_epsg, target_epsg)
    print(
        f"Transformed Coordinates (EPSG:{target_epsg}): {transformed_x}, {transformed_y}"
    )
