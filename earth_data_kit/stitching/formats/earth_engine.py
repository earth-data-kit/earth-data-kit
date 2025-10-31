import pandas as pd
import earth_data_kit.stitching.engines.commons as commons
from earth_data_kit.stitching.classes.tile import Tile
from earth_data_kit.utilities import geo, helpers
import concurrent.futures
from tqdm import tqdm


class EarthEngineAdapter:
    def __init__(self):
        self.name = "Earth Engine"

    def _get_subdatasets(self, df):
        # Get all subdatasets
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            futures = [
                executor.submit(geo.get_subdatasets, tile.gdal_path)
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

    def create_tiles(self, scan_df, band_locator="description"):
        subdataset_paths = self._get_subdatasets(scan_df)

        catalog_df = []

        if len(subdataset_paths) > 0:
            # Get metadata for all subdatasets
            subdatasets_metadata = commons.get_tiles_metadata(
                subdataset_paths, band_locator
            )
            k = 0
            for row in scan_df.itertuples():
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
            parent_tiles_metadata = commons.get_tiles_metadata(
                scan_df["gdal_path"].tolist(), band_locator
            )
            k = 0
            for row in scan_df.itertuples():
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
