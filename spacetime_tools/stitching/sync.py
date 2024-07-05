import spacetime_tools.stitching.time_filters as time_filters
import spacetime_tools.stitching.space_filters as space_filters
import logging
import fiona
from spacetime_tools.stitching.engines import s3
import os
import spacetime_tools.stitching.helpers as helpers

fiona.drvsupport.supported_drivers[
    "kml"
] = "rw"  # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers[
    "KML"
] = "rw"  # enable KML support which is disabled by default

logger = logging.getLogger(__name__)
engines_supported = ["s3"]


def sync(
    engine="s3",
    pattern=None,
    grid_fp=None,
    matcher=None,
    bbox=None,
    date_range=None,
    engine_opts={},
):
    """Syncs data from a remote rasters set to filesystem"""
    if engine not in engines_supported:
        raise Exception("only s3 engine is supported")

    if not pattern:
        raise Exception("pattern in required")

    # Resolving time filtering
    pattern_list = time_filters.resolve_time_filters(pattern, date_range)

    # Resolving space filtering
    pattern_list = space_filters.resolve_space_filters(
        pattern_list, grid_fp, matcher, bbox
    )
    logger.info(f"{len(pattern_list)} patterns to search")

    # Making sure tmp dir exists
    helpers.make_sure_dir_exists(f"{os.getcwd()}/tmp")

    # Apply the wildcard search and list all objects we want to download
    if engine == "s3":
        s3.create_inventory(pattern_list, engine_opts)
        s3.sync_inventory(pattern_list, engine_opts)
