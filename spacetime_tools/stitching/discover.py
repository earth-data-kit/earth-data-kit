from osgeo import gdal

gdal.UseExceptions()
import pandas as pd
import time
import json
import logging
import glob
import concurrent.futures
import spacetime_tools.stitching.helpers as helpers
import spacetime_tools.stitching.engines.s3 as s3
import spacetime_tools.stitching.decorators as decorators

logger = logging.getLogger(__name__)


@decorators.timed
@decorators.log_init
def spatial_discovery(engine, inventory_file):
    if engine not in ["s3"]:
        raise Exception(f"{engine} engine not supported")

    if engine == "s3":
        df = pd.read_json(inventory_file, lines=True)
        df["gdal_path"] = df["key"].str.replace("s3://", "/vsis3/")

    futures = []
    st = time.time()
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=helpers.get_max_workers()
    ) as executor:
        for row in df.itertuples():
            futures.append(executor.submit(read_metadata, row.gdal_path, row.Index))
        executor.shutdown(wait=True)

        results = []
        for future in futures:
            result = future.result()
            results = results + result

        inventory = pd.DataFrame(results)
        inventory["engine_path"] = df["key"]
        inventory["size"] = df["size"]
        inventory["last_modified"] = df["last_modified"]

        full_inventory = f"{helpers.get_tmp_dir()}/full-inventory.csv"
        inventory.to_csv(full_inventory, header=True, index=False)
        return full_inventory
