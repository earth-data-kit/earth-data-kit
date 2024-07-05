from osgeo import gdal

gdal.UseExceptions()
import pandas as pd
import time
import json
import logging
import glob
import concurrent.futures
import spacetime_tools.stitching.helpers as helpers

logger = logging.getLogger(__name__)


def discover(fp):
    """Capable of discovering the dataset by analysing the files metadata.
    Things it extracts from the metadata -
    1. CRS/Projection
    2. Extent of each file
    3. Resolution of each file
    4. Band details
    """
    st = time.time()

    tiles = glob.glob(fp)
    logger.info("Discovering %s tiles", len(tiles))
    futures = []
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=helpers.get_max_workers()
    ) as executor:
        for tile_fp in tiles:
            futures.append(executor.submit(discover_tile, tile_fp))
        executor.shutdown(wait=True)

        results = []
        for future in futures:
            result = future.result()
            results = results + result

        logger.info("Time taken %s", time.time() - st)
        # i == inventory
        i_df = pd.DataFrame(results)
        uniq_bands = (
            i_df.groupby(by=["description", "x_res", "y_res", "dtype"])
            .size()
            .reset_index(name="tile_count")
        )
        return i_df


def discover_tile(file_path):
    st = time.time()
    ds = gdal.Open(file_path)
    geotransform = ds.GetGeoTransform()

    bands = []
    band_count = ds.RasterCount
    for i in range(1, band_count + 1):
        band = ds.GetRasterBand(i)
        bands.append(
            {
                "description": band.GetDescription(),
                "geotransform": geotransform,
                "file_path": file_path,
                "dtype": band.DataType,
                "x_size": band.XSize,
                "y_size": band.YSize,
                # TODO: Round so that random floating point errors don't come
                "x_res": int(geotransform[1]),
                "y_res": int(geotransform[5]),
            }
        )

    return bands
