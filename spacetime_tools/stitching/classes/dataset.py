import pandas as pd
import geopandas as gpd
import logging
import re
import copy

logger = logging.getLogger(__name__)


class DataSet:
    def __init__(self, engine, source) -> None:
        self.engine = engine
        self.source = source
        self.patterns = []

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
        self.tiles = []
        pass

    def sync(self):
        pass

    def stitch(self, driver, destination):
        pass
