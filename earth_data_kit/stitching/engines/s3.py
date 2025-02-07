import os
import pandas as pd
import logging
import earth_data_kit.stitching.helpers as helpers
import pathlib
import Levenshtein as levenshtein
import geopandas as gpd
import re
import copy

logger = logging.getLogger(__name__)


class S3:
    def __init__(self) -> None:
        self.name = "S3"
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

    def get_patterns(self, source, time_opts, space_opts):
        patterns_df = pd.DataFrame()

        # Expanding for time dimension
        start = time_opts["start"]
        end = time_opts["end"]
        patterns_df["date"] = pd.date_range(start=start, end=end, inclusive="both")
        patterns_df["search_path"] = patterns_df["date"].dt.strftime(source)

        # Expanding for space dimension
        bbox = space_opts["bbox"]
        if ("grid_file" not in space_opts) or (
            ("grid_file" in space_opts) and (space_opts["grid_file"] is None)
        ):
            # Doing nothing if grid_file is not passed
            return patterns_df

        grid_file = space_opts["grid_file"]
        matcher = space_opts["matcher"]
        if grid_file.endswith(".kml") or grid_file.endswith(".KML"):
            grid_df = gpd.read_file(grid_file, driver="kml", bbox=bbox)
            space_vars = []
            for grid in grid_df.itertuples():
                space_vars.append(matcher(grid))

            new_patterns = []
            for row in patterns_df.itertuples():
                matches = re.findall(r"({.[^}]*})", row.search_path)  # type: ignore
                # Now we replace matches and with all space_variables
                for var in space_vars:
                    tmp_p = copy.copy(row.search_path)
                    for m in matches:
                        tmp_p = tmp_p.replace(  # type: ignore
                            m, var[m.replace("{", "").replace("}", "")]
                        )
                    new_patterns.append([row.date, tmp_p])
            new_patterns_df = pd.DataFrame(
                new_patterns, columns=["date", "search_path"]
            )
            return new_patterns_df
        else:
            raise Exception("drivers other than kml are not supported")

    def scan(self, source, time_opts, space_opts, tmp_base_dir):
        patterns_df = self.get_patterns(source, time_opts, space_opts)
        ls_cmds_fp = f"{tmp_base_dir}/ls_commands.txt"
        inventory_file_path = f"{tmp_base_dir}/inventory.csv"

        # go-lib expects paths in unix style
        patterns_df["unix_path"] = patterns_df["search_path"].str.replace("s3://", "/")

        patterns_df[["unix_path"]].to_csv(ls_cmds_fp, index=False, header=False)
        lib_path = os.path.join(
            pathlib.Path(__file__).parent.resolve(),
            "..",
            "shared_libs",
            "builds",
            "go-lib",
        )

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

        return inv_df[["date", "engine_path", "gdal_path", "tile_name"]]
