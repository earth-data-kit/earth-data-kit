import concurrent.futures
import os
import logging
from osgeo import ogr, gdal
import pandas as pd
from earth_data_kit.stitching.classes.tile import Tile
import json
import earth_data_kit.utilities as utilities
import earth_data_kit.stitching.engines.commons as commons
import earth_data_kit.utilities.helpers as helpers
from tqdm import tqdm
import earth_data_kit.stitching.decorators as decorators
import datetime

logger = logging.getLogger(__name__)


class EarthEngine:
    def __init__(self) -> None:
        self.name = "earth_engine"

    def _get_parent_tiles(self, source, time_opts, space_opts):
        ds = ogr.GetDriverByName("EEDA").Open(
            f"EEDA:projects/earthengine-public/assets/{source}"
        )

        layer = ds.GetLayer()
        layer.SetSpatialFilterRect(
            space_opts["bbox"][0],
            space_opts["bbox"][1],
            space_opts["bbox"][2],
            space_opts["bbox"][3],
        )

        if time_opts and "start" in time_opts and "end" in time_opts:
            layer.SetAttributeFilter(
                f"startTime >= '{time_opts['start']}' and endTime <= '{time_opts['end'] - datetime.timedelta(seconds=1)}Z'"
            )
        tiles = []
        for feature in layer:
            tiles.append([feature["gdal_dataset"], feature["id"], feature["startTime"]])

        df = pd.DataFrame(tiles, columns=pd.Index(["gdal_path", "engine_path", "date"]))
        df["tile_name"] = df["gdal_path"].str.split("/").str[-1]
        df["date"] = pd.to_datetime(df["date"], format="ISO8601")

        return df

    def scan(self, source, time_opts, space_opts, tmp_base_dir, band_locator):
        df = self._get_parent_tiles(source, time_opts, space_opts)
        df["date"] = (
            df["date"].dt.tz_convert("UTC")
            if df["date"].dt.tz is not None
            else df["date"].dt.tz_localize("UTC")
        )
        return df

    def _sync_one(self, gdal_path, sync_idx, output_path, overwrite=False):
        if not overwrite and os.path.exists(output_path):
            return

        with tqdm(
            total=100,
            desc=f"{sync_idx+1}. Downloading {os.path.basename(gdal_path)}",
            unit="%",
        ) as pbar:

            def progress_callback(complete, message, data):
                pbar.update(int(complete * 100) - pbar.n)
                return 1  # Return 1 to continue the operation

            gdal.Translate(
                output_path,
                gdal_path,
                format="GTiff",
                callback=progress_callback,
                creationOptions=[
                    "NUM_THREADS=ALL_CPUS",
                    "TILED=YES",
                    "BIGTIFF=YES",
                    "COMPRESS=ZSTD",
                    "SPARSE_OK=TRUE",
                ],
            )

    def sync(self, df, tmp_base_dir, overwrite=False):
        # Iterate over the dataframe to get GDAL paths that need syncing
        gdal_paths = []
        for band_tile in df.itertuples():
            gdal_paths.append(band_tile.tile.gdal_path)

        gdal_paths = list(set(gdal_paths))
        helpers.make_sure_dir_exists(f"{tmp_base_dir}/raw-data")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            futures = []
            sync_idx = 0
            for gdal_path in gdal_paths:
                output_path = f"{tmp_base_dir}/raw-data/{gdal_path.split('/')[-1]}.tif"
                futures.append(
                    executor.submit(
                        self._sync_one, gdal_path, sync_idx, output_path, overwrite
                    )
                )
                sync_idx += 1

            # Wait for all futures to complete
            for future in futures:
                future.result()

        # Update gdal_path in dataframe with local paths
        for band_tile in df.itertuples():
            output_path = (
                f"{tmp_base_dir}/raw-data/{band_tile.tile.gdal_path.split('/')[-1]}.tif"
            )
            df.at[band_tile.Index, "tile"].gdal_path = output_path

        return df
