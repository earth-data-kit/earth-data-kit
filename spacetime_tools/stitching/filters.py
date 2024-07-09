import logging
import copy
import re
import geopandas as gpd
import json
import fiona
import pandas as pd
import shapely
from shapely import Polygon
from rasterio import Affine
from ast import literal_eval
import numpy as np
from shapely.affinity import affine_transform
import shapely.geometry
import pyproj
import spacetime_tools.stitching.helpers as helpers

pd.set_option("display.max_columns", None)

fiona.drvsupport.supported_drivers[
    "kml"
] = "rw"  # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers[
    "KML"
] = "rw"  # enable KML support which is disabled by default

logger = logging.getLogger(__name__)


def resolve_time_filters(source, date_range):
    # * Handles the below time format codes. For formats without trailing zeros check the stackoverflow link
    # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    # https://stackoverflow.com/questions/904928/python-strftime-date-without-leading-0
    if not (date_range and len(date_range) == 2):
        return [source]
    return list(
        set(
            pd.date_range(
                start=date_range[0], end=date_range[1], inclusive="both"
            ).strftime(source)
        )
    )


def resolve_space_filters(patterns, grid_fp, matcher, bbox):
    if (not grid_fp) or (not matcher):
        # Won't be able to filter so simply return patterns
        return patterns

    # TODO: Add stuff about other file types and driver
    grid_df = gpd.read_file(grid_fp, driver="kml", bbox=bbox)
    space_vars = []
    for grid in grid_df.itertuples():
        space_vars.append(matcher(grid))

    pl = []
    for p in patterns:
        matches = re.findall(r"({.[^}]*})", p)
        # Now we replace matches and with all space_variables
        for var in space_vars:
            tmp_p = copy.copy(p)
            for m in matches:
                tmp_p = tmp_p.replace(m, var[m.replace("{", "").replace("}", "")])
            pl.append(tmp_p)

    return pl


def polygonise_2Dcells(df_row):
    return Polygon(
        [
            (df_row.x_min, df_row.y_min),
            (df_row.x_max, df_row.y_min),
            (df_row.x_max, df_row.y_max),
            (df_row.x_min, df_row.y_max),
        ]
    )


def filter_inventory(file_path, source, bbox, date_range):
    df = pd.read_csv(file_path)

    if date_range:
        # Filter temporally
        pass
    if bbox:
        # * Below function assumes the projection is gonna be same which can be
        # * usually true for a single set of tiles
        # Filter spatially

        # Getting the projection from first row
        projection = df["projection"].iloc[0]
        # Add the extent geo-series and set projection
        extent = gpd.GeoSeries(
            df.apply(polygonise_2Dcells, axis=1),
            crs=pyproj.CRS.from_user_input(projection),
        )
        reprojected_extent = extent.to_crs(epsg=4326)

        # Get polygon from user's bbox
        bbox = shapely.geometry.box(*bbox, ccw=True)

        # Perform intersection and filtering
        intersects = reprojected_extent.intersects(bbox)
        df = df[intersects == True]

    filtered_inventory = f"{helpers.get_tmp_dir()}/filtered-inventory.csv"
    df.to_csv(filtered_inventory, index=False, header=True)

    return filtered_inventory
