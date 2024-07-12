import logging
import fiona
from spacetime_tools.stitching import decorators
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


@decorators.timed
def sync(engine, filtered_inventory_file):
    df = pd.read_csv(filtered_inventory_file)
    if engine == "s3":
        return s3.sync_inventory(df)

    raise Exception(f"{engine} engine not supported")
