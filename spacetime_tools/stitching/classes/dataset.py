import pandas as pd
import geopandas as gpd
import logging
import re
import copy
from spacetime_tools.stitching import helpers
import spacetime_tools.stitching.constants as constants
import spacetime_tools.stitching.engines.s3 as s3
import concurrent.futures
import spacetime_tools.stitching.classes.tile as tile

logger = logging.getLogger(__name__)


class DataSet:
    def __init__(self, id, engine, source) -> None:
        if engine not in constants.engines_supported:
            raise Exception(f"{engine} not supported")

        self.id = id
        self.engine = engine
        self.source = source
        self.patterns = []
        self.tiles = []

    def set_timebounds(self, start, end):
        self.patterns = self.patterns + list(
            set(
                pd.date_range(start=start, end=end, inclusive="both").strftime(
                    self.source
                )
            )
        )

    def set_spacebounds(self, bbox, grid_file, matcher):
        self.bbox = bbox

        if grid_file.endswith(".kml") or grid_file.endswith(".KML"):
            grid_df = gpd.read_file(grid_file, driver="kml", bbox=bbox)
            space_vars = []
            for grid in grid_df.itertuples():
                space_vars.append(matcher(grid))

            patterns = self.patterns

            new_patterns = []
            for p in patterns:
                matches = re.findall(r"({.[^}]*})", p)
                # Now we replace matches and with all space_variables
                for var in space_vars:
                    tmp_p = copy.copy(p)
                    for m in matches:
                        tmp_p = tmp_p.replace(
                            m, var[m.replace("{", "").replace("}", "")]
                        )
                    new_patterns.append(tmp_p)

            self.patterns = new_patterns
        else:
            raise Exception("drivers other than kml are not supported")

    def find_tiles(self):
        df = pd.DataFrame()

        if self.engine == "s3":
            df = s3.create_inventory(self.patterns, self.get_ds_tmp_path())

        futures = []
        tiles = []
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=helpers.get_max_workers()
        ) as executor:
            for row in df.itertuples():
                t = self.create_tile(row.engine_path, row.gdal_path, row.size)
                tiles.append(t)
                futures.append(executor.submit(t.get_metadata))
            executor.shutdown(wait=True)

            results = []
            for idx in range(len(futures)):
                future = futures[idx]
                result = future.result()
                tiles[idx].set_metadata(result)

            self.tiles = tiles

    def create_tile(self, engine_path, gdal_path, size):
        t = tile.Tile(engine_path, gdal_path, size)
        return t

    def sync(self):
        self.find_tiles()

        if self.engine == "s3":
            df = pd.DataFrame([t.__dict__ for t in self.tiles])
            s3.sync_inventory(df, self.get_ds_tmp_path())

    def get_ds_tmp_path(self):
        path = f"{helpers.get_tmp_dir()}/{self.id}"
        helpers.make_sure_dir_exists(path)
        return path

    def stitch(self, driver, destination):
        pass
