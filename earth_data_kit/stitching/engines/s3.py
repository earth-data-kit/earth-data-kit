import os
from tqdm import tqdm
from osgeo import gdal
import subprocess
import io
import pandas as pd
import logging
import earth_data_kit.utilities.helpers as helpers
import earth_data_kit.utilities.geo as geo
import pathlib
import Levenshtein as levenshtein
import geopandas as gpd
import re
import shapely
import copy
import earth_data_kit as edk
from earth_data_kit.stitching import decorators
import earth_data_kit.stitching.engines.commons as commons
import json
from tenacity import retry, stop_after_attempt, wait_fixed
import concurrent.futures

logger = logging.getLogger(__name__)


class S3:
    def __init__(self) -> None:
        self.name = "s3"
        no_sign_flag = os.getenv("AWS_NO_SIGN_REQUEST")
        request_payer_flag = os.getenv("AWS_REQUEST_PAYER")
        profile_flag = os.getenv("AWS_PROFILE")

        no_sign_flag_env = os.getenv("AWS_NO_SIGN_REQUEST")
        request_payer_env = os.getenv("AWS_REQUEST_PAYER")
        profile_env = os.getenv("AWS_PROFILE")

        if no_sign_flag_env and (no_sign_flag_env.upper() in ["YES", "TRUE", "1"]):
            self.no_sign_flag = "--no-sign-request"
        else:
            self.no_sign_flag = ""

        if request_payer_env and (request_payer_env.lower() == "requester"):
            self.request_payer_flag = "--request-payer requester"
        else:
            self.request_payer_flag = ""

        if profile_env:
            self.profile_flag = f"--profile {profile_env}"
        else:
            self.profile_flag = ""

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

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True)
    def _scan_s3_via_s5cmd(self, path):
        ls_cmd = f"{edk.S5CMD_PATH} {self.no_sign_flag} {self.request_payer_flag} {self.profile_flag} --json ls '{path}'"
        proc = subprocess.Popen(
            ls_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            if "no object found" not in stderr.decode("utf-8").lower():
                raise Exception(
                    f"Error scanning S3 path {path}: {stderr.decode('utf-8')}"
                )
            return pd.DataFrame(columns=["key"])
        try:
            stdout = stdout.decode("utf-8")
            df = pd.read_json(io.StringIO(stdout), lines=True)
            if "key" in df.columns:
                return df[["key"]]
            else:
                return pd.DataFrame(columns=["key"])
        except Exception as e:
            logger.error(f"Error scanning S3 path {path}: {e} {stdout}")
            return pd.DataFrame(columns=["key"])

    def scan(self, source, time_opts, space_opts, tmp_base_dir, band_locator):
        patterns_df = self.get_patterns(source, time_opts, space_opts)
        if "date" not in patterns_df.columns:
            patterns_df["date"] = None

        def scan_pattern(row):
            result_df = self._scan_s3_via_s5cmd(row.search_path)
            if hasattr(row, "date"):
                result_df["date"] = row.date
            return result_df

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            futures = [
                executor.submit(scan_pattern, row) for row in patterns_df.itertuples()
            ]
            scan_results = []
            for f in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Scanning S3 patterns",
                unit="path",
            ):
                result_df = f.result()
                scan_results.append(result_df)
        if scan_results:
            inv_df = pd.concat(scan_results, ignore_index=True)
        else:
            inv_df = pd.DataFrame(columns=["key"])

        # Adding gdal_path
        inv_df["gdal_path"] = inv_df["key"].str.replace("s3://", "/vsis3/")
        inv_df["engine_path"] = inv_df["key"]
        inv_df["tile_name"] = inv_df["key"].str.split("/").str[-1]
        inv_df.drop(columns=["key"], inplace=True)

        # Convert datetime[ns] to datetime[ns, UTC]
        inv_df["date"] = inv_df["date"].dt.tz_localize("UTC")
        return inv_df

    def sync(self, df, tmp_base_dir, overwrite=False):
        # Iterate over the dataframe to get GDAL paths that need syncing
        cmds = []
        for band_tile in df.itertuples():
            try:
                logger.debug(f"Trying to open local file {band_tile.tile.gdal_path}")
                gdal.Open(
                    f"{tmp_base_dir}/raw-data/{band_tile.tile.gdal_path.replace('/vsis3/', '')}"
                )
                # File exists and is valid, no need to sync, unless overwrite is True
                if overwrite:
                    cmd = f"cp --sp {band_tile.tile.gdal_path.replace('/vsis3/', 's3://')} {tmp_base_dir}/raw-data/{band_tile.tile.gdal_path.replace('/vsis3/', '')}"
                    cmds.append(cmd)
            except Exception as e:
                # Error getting metadata, file will be synced
                cmd = f"cp --sp {band_tile.tile.gdal_path.replace('/vsis3/', 's3://')} {tmp_base_dir}/raw-data/{band_tile.tile.gdal_path.replace('/vsis3/', '')}"
                cmds.append(cmd)

        cmds = list(set(cmds))
        helpers.make_sure_dir_exists(f"{tmp_base_dir}/raw-data")
        logger.info(f"Syncing {len(cmds)} files")

        pd.DataFrame(cmds).to_csv(
            f"{tmp_base_dir}/sync_cmds.txt", index=False, header=False
        )
        os.system(f"{edk.S5CMD_PATH} run {tmp_base_dir}/sync_cmds.txt")
        os.remove(f"{tmp_base_dir}/sync_cmds.txt")

        # Update gdal_path in dataframe with local paths
        for band_tile in df.itertuples():
            output_path = f"{tmp_base_dir}/raw-data/{band_tile.tile.gdal_path.replace('/vsis3/', '')}"
            band_tile.tile.gdal_path = output_path

        return df
