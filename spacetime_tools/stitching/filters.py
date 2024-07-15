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
import spacetime_tools.stitching.decorators as decorators

pd.set_option("display.max_columns", None)

fiona.drvsupport.supported_drivers[
    "kml"
] = "rw"  # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers[
    "KML"
] = "rw"  # enable KML support which is disabled by default

logger = logging.getLogger(__name__)


@decorators.timed
@decorators.log_init
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


@decorators.timed
@decorators.log_init
def resolve_space_filters(patterns, grid_fp, matcher, bbox):
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


@decorators.timed
@decorators.log_init
def filter_inventory(file_path, source, bbox, date_range):
    df = pd.read_csv(file_path)
