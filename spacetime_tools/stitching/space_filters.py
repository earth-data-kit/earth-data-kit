import logging
import geopandas as gpd
import re
import copy

logger = logging.getLogger(__name__)


def resolve_space_filters(pattern_list, grid_fp, matcher, bbox):
    if (not grid_fp) or (not matcher):
        # Won't be able to filter so simply return pattern_list
        return pattern_list

    # TODO: Add stuff about file types and driver
    grid_df = gpd.read_file(grid_fp, driver="kml", bbox=bbox)
    space_vars = []
    for grid in grid_df.itertuples():
        space_vars.append(matcher(grid))

    pl = []

    for p in pattern_list:
        matches = re.findall(r"(!.[^!]*!)", p)
        # Now we replace matches and with all space_variables
        for var in space_vars:
            tmp_p = copy.copy(p)
            for m in matches:
                tmp_p = tmp_p.replace(m, var[m.replace("!", "")])
            pl.append(tmp_p)

    return pl
