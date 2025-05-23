import concurrent.futures
import os
import logging
from osgeo import ogr
import pandas as pd

logger = logging.getLogger(__name__)


class EarthEngine:
    def __init__(self) -> None:
        self.name = "earth_engine"
        self.app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT")

    def scan(self, source, time_opts, space_opts, tmp_base_dir):
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
        df["date"] = pd.to_datetime(df["date"])

        # Check if time_opts has resolution set to daily
        if (
            time_opts
            and "resolution" in time_opts
            and time_opts["resolution"] == "daily"
        ):
            # Set the time part of the date to 00:00:00
            df["date"] = df["date"].dt.normalize()
        return df[["gdal_path", "engine_path", "date", "tile_name"]]
