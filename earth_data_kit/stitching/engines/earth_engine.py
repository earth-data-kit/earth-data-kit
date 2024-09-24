import concurrent.futures
import os
import logging
from osgeo import ogr
import pandas as pd
import earth_data_kit.stitching.helpers as helpers
import json

logger = logging.getLogger(__name__)


class EarthEngine:
    def __init__(self) -> None:
        self.app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT")

    def create_inventory(self, source, time_opts, space_opts, tmp_base_dir):
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

        layer.SetAttributeFilter(
            f"startTime >= '{time_opts['start']}' and endTime <= '{time_opts['end']}'"
        )
        tiles = []
        for feature in layer:
            tiles.append([feature["gdal_dataset"], feature["id"]])

        df = pd.DataFrame(tiles, columns=["gdal_path", "engine_path"])
        return df[["gdal_path", "engine_path"]]

    def sync_inventory(self, df, tmp_base_dir):
        base_path = f"{tmp_base_dir}/raw"

        # We create one file per band as EE can have different datatypes for bands.
        # Not exactly sure what format they are using
        # Using process pool as when using threads only one gdal_translate program was running
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=helpers.get_processpool_workers()
        ) as executor:
            for row in df.itertuples():
                bands = json.loads(row.bands)
                single_band_fps = []
                for b_idx in range(len(bands)):
                    single_band_fp = (
                        f'{base_path}/{row.engine_path}-{bands[b_idx]["band_idx"]}.tif'
                    )

                    cmd = f'GOOGLE_APPLICATION_CREDENTIALS={os.getenv("GOOGLE_APPLICATION_CREDENTIALS")} gdal_translate -b {bands[b_idx]["band_idx"]} {row.gdal_path} {single_band_fp} -co SPARSE_OK=TRUE -co BIGTIFF=YES -co NUM_THREADS=ALL_CPUS'
                    base_folder = "/".join(single_band_fp.split("/")[:-1])
                    helpers.make_sure_dir_exists(base_folder)

                    single_band_fps.append(single_band_fp)
                    executor.submit(os.system, cmd)

                # We combine all the single bands tif to a vrt as rest of the system is written considering files with multiple bands rather than single band tifs. Something to think later
                local_path = f"{base_path}/{row.engine_path}.vrt"
                cmd = f"gdalbuildvrt -separate {local_path} {' '.join(single_band_fps)}"
                os.system(cmd)
                df.at[row.Index, "local_path"] = local_path
            executor.shutdown(wait=True)

        return df
