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

logger = logging.getLogger(__name__)


class EarthEngine:
    def __init__(self) -> None:
        self.name = "earth_engine"
        self.app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT")

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
                f"startTime >= '{time_opts['start']}' and endTime <= '{time_opts['end']}'"
            )
        tiles = []
        for feature in layer:
            tiles.append([feature["gdal_dataset"], feature["id"], feature["startTime"]])

        df = pd.DataFrame(tiles, columns=["gdal_path", "engine_path", "date"])
        df["tile_name"] = df["gdal_path"].str.split("/").str[-1]
        df["date"] = pd.to_datetime(df["date"], format="ISO8601")

        return df

    def _aggregate_temporally(self, df, resolution=1):
        # Floor dates to nearest resolution interval (in days)
        df["date"] = df["date"].dt.floor(f"{resolution}D")
        return df

    def _get_subdatasets(self, df):
        # Get all subdatasets
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=utilities.helpers.get_threadpool_workers()
        ) as executor:
            futures = [
                executor.submit(utilities.geo.get_subdatasets, tile.gdal_path)
                for tile in df.itertuples()
            ]
            subdataset_paths = [
                future.result()
                for future in tqdm(futures, desc="Getting subdatasets", unit="tile")
            ]
        df["subdataset_paths"] = subdataset_paths
        subdataset_paths = [path for sublist in subdataset_paths for path in sublist]
        return subdataset_paths

    def _expand_catalog(self, catalog_df):
        expanded_catalog_df = []
        for row in catalog_df:
            parent_gdal_path = ":".join(row["gdal_path"].split(":")[0:2])
            for band in row["bands"]:
                # Create a copy of the row without the bands field
                new_row = {k: v for k, v in row.items() if k != "bands"}
                # Add single band with source_idx 1
                new_row["bands"] = [{**band, "source_idx": 1}]
                new_row["gdal_path"] = f"{parent_gdal_path}:{band['description']}"
                new_row["engine_path"] = f"{parent_gdal_path}:{band['description']}"
                expanded_catalog_df.append(new_row)
        return expanded_catalog_df

    def scan(self, source, time_opts, space_opts, tmp_base_dir):
        df = self._get_parent_tiles(source, time_opts, space_opts)

        # Check if time_opts has resolution set to daily
        if (
            time_opts
            and "resolution" in time_opts
            and time_opts["resolution"] == "daily"
        ):
            # Set the time part of the date to 00:00:00
            df = self._aggregate_temporally(df)

        subdataset_paths = self._get_subdatasets(df)

        catalog_df = []

        if len(subdataset_paths) > 0:
            # Get metadata for all subdatasets
            subdatasets_metadata = commons.get_tiles_metadata(subdataset_paths)
            k = 0
            for row in df.itertuples():
                for subdataset_path in row.subdataset_paths:
                    subdataset_metadata = subdatasets_metadata[k]
                    k += 1
                    catalog_df.append(
                        {
                            "gdal_path": subdataset_path,
                            "engine_path": subdataset_path,
                            "date": row.date,
                            "tile_name": row.tile_name,
                            "geo_transform": subdataset_metadata["geo_transform"],
                            "projection": subdataset_metadata["projection"],
                            "x_size": subdataset_metadata["x_size"],
                            "y_size": subdataset_metadata["y_size"],
                            "crs": subdataset_metadata["crs"],
                            "length_unit": subdataset_metadata["length_unit"],
                            "bands": subdataset_metadata["bands"],
                        }
                    )

        else:
            # Get metadata for all parent tiles
            parent_tiles_metadata = commons.get_tiles_metadata(df["gdal_path"].tolist())
            k = 0
            for row in df.itertuples():
                tile_metadata = parent_tiles_metadata[k]
                k += 1
                catalog_df.append(
                    {
                        "gdal_path": row.gdal_path,
                        "engine_path": row.engine_path,
                        "date": row.date,
                        "tile_name": row.tile_name,
                        "geo_transform": tile_metadata["geo_transform"],
                        "projection": tile_metadata["projection"],
                        "x_size": tile_metadata["x_size"],
                        "y_size": tile_metadata["y_size"],
                        "crs": tile_metadata["crs"],
                        "length_unit": tile_metadata["length_unit"],
                        "bands": tile_metadata["bands"],
                    }
                )

        catalog_df = self._expand_catalog(catalog_df)

        # Passing array of jsons in a dataframe "bands" column
        tiles = Tile.from_df(pd.DataFrame(catalog_df))
        return tiles

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
