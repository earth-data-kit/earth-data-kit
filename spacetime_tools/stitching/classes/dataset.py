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
import shapely.geometry
import pyproj

logger = logging.getLogger(__name__)


class DataSet:
    def __init__(self, id, engine, source, overwrite) -> None:
        if engine not in constants.engines_supported:
            raise Exception(f"{engine} not supported")

        self.id = id
        self.engine = engine
        self.source = source
        self.patterns = []
        self.tiles = []
        self.complete_inventory = f"{self.get_ds_tmp_path()}/complete-inventory.csv"
        self.filtered_inventory = f"{self.get_ds_tmp_path()}/filtered-inventory.csv"
        self.local_inventory = f"{self.get_ds_tmp_path()}/local-inventory.csv"
        if overwrite:
            helpers.empty_dir(self.get_ds_tmp_path())

    def set_timebounds(self, start, end):
        self.start = start
        self.end = end

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
                t = tile.Tile(row.engine_path, row.gdal_path, row.size)
                tiles.append(t)
                futures.append(executor.submit(t.get_metadata))
            executor.shutdown(wait=True)

            results = []
            for idx in range(len(futures)):
                future = futures[idx]
                result = future.result()
                tiles[idx].set_metadata(result)

            df = tile.Tile.to_df(tiles)
            df.to_csv(f"{self.complete_inventory}", header=True, index=False)

    def filter_tiles(self):
        df = pd.read_csv(f"{self.complete_inventory}")

        if self.start and self.end:
            # Not filtering based on date_range as we are already pin-pointing the files to download
            pass

        if self.bbox:
            # * Below function assumes the projection is gonna be same which can be
            # * usually true for a single set of tiles
            # Filter spatially

            # Getting the projection from first row
            projection = df["projection"].iloc[0]
            # Add the extent geo-series and set projection
            extent = gpd.GeoSeries(
                df.apply(helpers.polygonise_2Dcells, axis=1),
                crs=pyproj.CRS.from_user_input(projection),
            )
            reprojected_extent = extent.to_crs(epsg=4326)

            # Get polygon from user's bbox
            bbox = shapely.geometry.box(*self.bbox, ccw=True)

            # Perform intersection and filtering
            intersects = reprojected_extent.intersects(bbox)
            df = df[intersects == True]

        filtered_inventory = f"{self.filtered_inventory}"
        df.to_csv(filtered_inventory, index=False, header=True)

    def sync(self):
        self.find_tiles()
        self.filter_tiles()

        # Reading the filtered inventory
        df = pd.read_csv(self.filtered_inventory)

        if self.engine == "s3":
            # Syncing files to local
            df = s3.sync_inventory(df, self.get_ds_tmp_path())
            df.to_csv(f"{self.local_inventory}", index=False, header=True)

    def get_ds_tmp_path(self):
        path = f"{helpers.get_tmp_dir()}/{self.id}"
        helpers.make_sure_dir_exists(path)
        return path

    def stitch(self):
        df = pd.read_csv(self.local_inventory)
        tiles = tile.Tile.as_tiles(df)
