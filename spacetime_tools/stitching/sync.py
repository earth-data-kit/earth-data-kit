import logging
import fiona
from spacetime_tools.stitching.engines import s3
import pandas as pd
import spacetime_tools.stitching.helpers as helpers

fiona.drvsupport.supported_drivers[
    "kml"
] = "rw"  # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers[
    "KML"
] = "rw"  # enable KML support which is disabled by default

logger = logging.getLogger(__name__)
engines_supported = ["s3"]


def sync(filtered_inventory_file):
    df = pd.read_csv(filtered_inventory_file)
    logger.info(df)
    # Apply the wildcard search and list all objects we want to download
    # if engine == "s3":
    #     s3.create_inventory(pattern_list, engine_opts)
    #     s3.sync_inventory(pattern_list, engine_opts)
