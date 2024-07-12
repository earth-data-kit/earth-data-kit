import spacetime_tools.stitching.filters as filters
import logging
import spacetime_tools.stitching.discover as discover
import spacetime_tools.stitching.sync as sync
import spacetime_tools.stitching.decorators as decorators
import spacetime_tools.stitching.stitch as stitch

logger = logging.getLogger(__name__)


def sync_and_stitch(
    engine="s3",
    source="s3://modis-pds",
    bbox=None,
    date_range=None,
    grid_fp=None,
    matcher=None,
    dest=None,
):
    # Gets the pattern list as input and explodes it with different combinations across time
    patterns = filters.resolve_time_filters(source, date_range)

    # Explodes along space dimension, atleast tries to using a grid file, if present.
    # If a grid file is present then the user also needs to provide the column and a small matching function
    # If a grid file is not present then we don't explodes using space dimension and filter based on metadata later
    if bbox and grid_fp and matcher:
        patterns = filters.resolve_space_filters(patterns, grid_fp, matcher, bbox)

    # Then we run discovery functions, first minimally. Minimally means just the unix-like metadata.
    # size, file_path, last_modified. This will output an inventory which is nothing but a list of files we need to download
    inventory_file = discover.file_discovery(engine, patterns)

    # Runs on remote and gets gdal-info of all files in the inventory, inventory filepath is required
    expanded_inventory_file = discover.spatial_discovery(engine, inventory_file)

    # Then we filter files which we need, this is done spatially and time-based if possible
    filtered_inventory_file = filters.filter_inventory(
        expanded_inventory_file, source, bbox, date_range
    )

    # Then we will download all the data
    local_inventory_file = sync.sync(engine, filtered_inventory_file)

    # After the above if condition data is downloaded locally and is ready to be stitched
    stitch.stitch(local_inventory_file, source, date_range, dest)
