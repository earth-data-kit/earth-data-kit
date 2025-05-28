import os
import pandas as pd
import logging
import earth_data_kit.utilities.helpers as helpers
import pathlib
import Levenshtein as levenshtein
import geopandas as gpd
import re
import shapely
import copy
from earth_data_kit.stitching import decorators
import earth_data_kit.stitching.engines.commons as commons
from earth_data_kit.stitching.classes.tile import Tile
import json

logger = logging.getLogger(__name__)


class S3:
    def __init__(self) -> None:
        self.name = "s3"
        no_sign_flag = os.getenv("AWS_NO_SIGN_REQUEST")
        request_payer_flag = os.getenv("AWS_REQUEST_PAYER")
        profile_flag = os.getenv("AWS_PROFILE")
        json_flag = "--json"
        if no_sign_flag and (no_sign_flag.upper() == "YES"):
            no_sign_flag = "--no-sign-request"
        else:
            no_sign_flag = ""

        if (request_payer_flag) and (request_payer_flag.upper() == "requester"):
            request_payer_flag = f"--request-payer requester"
        else:
            request_payer_flag = ""

        if profile_flag:
            profile_flag = f"--profile {profile_flag}"
        else:
            profile_flag = ""

        self.base_cmd = (
            f"s5cmd {no_sign_flag} {request_payer_flag} {profile_flag} {json_flag}"
        )

    def _expand_time(self, df, source, time_opts):
        if isinstance(source, list):
            # If source is a list, we don't need to expand time as user has provided direct path to multiple files
            return df
        # Expanding for time dimension
        if not time_opts:
            # If time options don't exist, return df unchanged
            return df
        if "start" not in time_opts or "end" not in time_opts:
            # If time options are incomplete, log a warning and return df unchanged
            logger.warning(
                "Incomplete time options provided. Both 'start' and 'end' are required for time expansion."
            )
            return df

        start = time_opts["start"]
        end = time_opts["end"]
        # TODO: Test if inclusive="both" is correct. We are making end_date as exclusive in set_timebounds
        df["date"] = pd.date_range(start=start, end=end, inclusive="both")
        df["search_path"] = df["date"].dt.strftime(source)
        return df

    def _expand_space(self, df, source, space_opts):
        if isinstance(source, list):
            # If source is a list, we don't need to expand space as user has provided direct path to multiple files
            return df
        matches = re.findall(r"({.[^}]*})", source)
        space_vars = []
        for m in matches:
            space_vars.append(m.replace("{", "").replace("}", ""))

        if len(space_vars) == 0:
            # No space variables found, return df unchanged
            return df

        if len(space_vars) > 0 and "grid_dataframe" not in space_opts:
            raise Exception("Spatial variables found but no grid_dataframe provided")

        grid_df = space_opts["grid_dataframe"]

        for var in space_vars:
            if var not in grid_df.columns:
                raise Exception(f"Spatial variable {var} not found in grid_dataframe")

        bbox = shapely.geometry.box(*space_opts["bbox"], ccw=True)  # type: ignore

        grid_df = grid_df[grid_df.intersects(bbox)]

        new_patterns = []
        for path in df.itertuples():
            for _, grid in grid_df.iterrows():
                tmp_p = copy.copy(path.search_path)
                for var in space_vars:
                    tmp_p = tmp_p.replace("{" + var + "}", grid[var])
                new_patterns.append([path.date, tmp_p])

        new_patterns_df = pd.DataFrame(new_patterns, columns=["date", "search_path"])

        return new_patterns_df

    def get_patterns(self, source, time_opts, space_opts):
        patterns_df = pd.DataFrame()

        patterns_df = self._expand_time(patterns_df, source, time_opts)

        patterns_df = self._expand_space(patterns_df, source, space_opts)

        # If expansion failed we send source as it is
        if patterns_df.empty:
            logger.warning(
                "Expansion failed. Will search according to source directly."
            )
            if isinstance(source, list):
                patterns_df = pd.DataFrame({"search_path": source})
            else:
                patterns_df = pd.DataFrame({"search_path": [source]})

        return patterns_df

    def scan(self, source, time_opts, space_opts, tmp_base_dir):
        patterns_df = self.get_patterns(source, time_opts, space_opts)

        ls_cmds_fp = f"{tmp_base_dir}/ls_commands.txt"
        inventory_file_path = f"{tmp_base_dir}/inventory.csv"
        if "date" not in patterns_df.columns:
            patterns_df["date"] = None

        # go-lib expects paths in unix style
        patterns_df["unix_path"] = patterns_df["search_path"].str.replace("s3://", "/")

        patterns_df[["unix_path"]].to_csv(ls_cmds_fp, index=False, header=False)
        lib_path = helpers.get_shared_lib_path()

        ls_cmd = f"{lib_path} {ls_cmds_fp} {inventory_file_path}"
        os.system(ls_cmd)

        inv_df = pd.read_csv(inventory_file_path, names=["key"])

        # Fixing output from go-lib
        inv_df["key"] = "s3://" + inv_df["key"].str[1:]

        for in_row in inv_df.itertuples():
            max_score = -99
            max_score_idx = -1
            for out_row in patterns_df.itertuples():
                s = levenshtein.ratio(in_row.key, out_row.search_path)  # type: ignore
                if s > max_score:
                    max_score = s
                    max_score_idx = out_row.Index

            inv_df.at[in_row.Index, "date"] = patterns_df["date"][max_score_idx]
            inv_df.at[in_row.Index, "search_path"] = patterns_df["search_path"][
                max_score_idx
            ]
            inv_df.at[in_row.Index, "unix_path"] = patterns_df["unix_path"][
                max_score_idx
            ]
            inv_df.at[in_row.Index, "tile_name"] = ".".join(
                inv_df.at[in_row.Index, "key"].split("/")[-1].split(".")[:-1]
            )

        # Adding gdal_path
        inv_df["gdal_path"] = inv_df["key"].str.replace("s3://", "/vsis3/")
        inv_df["engine_path"] = inv_df["key"]

        # Removing extra files created
        os.remove(inventory_file_path)
        os.remove(ls_cmds_fp)

        # TODO: Add code to handle daily resolution
        metadata = commons.get_tiles_metadata(inv_df["engine_path"].tolist())
        for idx in range(len(metadata)):
            inv_df.at[idx, "geo_transform"] = metadata[idx]["geo_transform"]
            inv_df.at[idx, "projection"] = metadata[idx]["projection"]
            inv_df.at[idx, "x_size"] = metadata[idx]["x_size"]
            inv_df.at[idx, "y_size"] = metadata[idx]["y_size"]
            inv_df.at[idx, "crs"] = metadata[idx]["crs"]
            inv_df.at[idx, "length_unit"] = metadata[idx]["length_unit"]
            # Passing array of jsons in a dataframe "bands" column
            inv_df.at[idx, "bands"] = metadata[idx]["bands"]

        tiles = Tile.from_df(inv_df)
        return tiles
