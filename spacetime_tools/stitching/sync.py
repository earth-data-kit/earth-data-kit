import spacetime_tools.stitching.time_filters as time_filters
import spacetime_tools.stitching.space_filters as space_filters
import logging
import fiona

fiona.drvsupport.supported_drivers['kml'] = 'rw' # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['KML'] = 'rw' # enable KML support which is disabled by default

logger = logging.getLogger(__name__)
engines_supported = ["s3"]

def sync(
    engine="s3",
    pattern=None,
    grid_fp=None,
    matcher=None,
    options={},
    bbox=None,
    date_range=None,
):
    """Syncs data from a remote rasters set to filesystem"""
    if engine not in engines_supported:
        raise Exception("only s3 engine is supported")

    if not pattern:
        raise Exception("pattern in required")

    # Resolving time filtering
    pattern_list = time_filters.resolve_time_filters(pattern, date_range)

    # Resolving space filtering
    pattern_list = space_filters.resolve_space_filters(pattern_list, grid_fp, matcher, bbox)
    logger.info(f"{len(pattern_list)} patterns to search")
    # First gets the pattern and creates the pattern list to search
    # It's basically custom time wildcards resolution, eg: ddd, DD, etc.
    # Once that is done we get the entire list based on time dimension
    # Then we resolve spatial wildcards or other variables,
    # that is done only if user supplies grid_fp and matcher function, if not we go ahead and
    # apply the wildcard search and list all objects we want to download
    # Once that is done we perform get metadata and projection details of all the files matched using gdal.Open
    # if method == "wildcard":
    #     inventory_file = s3.list_objects(pattern, options)
