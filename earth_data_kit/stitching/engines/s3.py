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

        def _extract_tokens(template: str):
            """Return normalized tokens like '%Y', '%m', '%d', '%H', '%M', ..."""
            # Match %-directives with optional padding/flags:
            #  - Flags: -, _, 0, ^, # (common across platforms), and optional ':' (for %:z)
            #  - Directive letter: [A-Za-z]
            #  - '%%' is a literal percent, we'll skip it later
            _TOKEN_RE = re.compile(r"%(?:%|[:]?[-_0^#]?[A-Za-z])")
            tokens = []
            for m in _TOKEN_RE.finditer(template):
                t = m.group(0)
                if t == "%%":
                    continue  # literal '%'
                # Normalize: strip optional leading ':' and flag chars, keep the letter.
                # Examples: '%-d' -> '%d', '%_m' -> '%m', '%#H' -> '%H', '%:z' -> '%z'
                letter = t[-1]
                tokens.append("%" + letter)
            return tokens

        def smallest_unit(template: str) -> str:
            """
            Return the smallest granularity implied by the template,
            capped at: 'minute' > 'hour' > 'day' > 'year'.
            """
            tset = set(_extract_tokens(template))
            if not tset:
                raise ValueError("No recognized strftime directives in template.")

            # Tokens that imply at least MINUTE (we cap seconds/micros/time to minute)
            minute_like = {
                "%M",
                "%S",
                "%f",
                "%X",
                "%c",
            }  # %c usually includes time; cap to minute
            # Tokens that imply HOUR (12/24h)
            hour_like = {"%H", "%I", "%p"}
            # Tokens that imply at least DAY (weekday/day/month/week-of-year/date/ISO-weekday)
            day_like = {
                "%d",
                "%e",
                "%j",
                "%a",
                "%A",
                "%w",
                "%m",
                "%b",
                "%B",
                "%U",
                "%W",
                "%V",
                "%u",
                "%x",  # locale date
            }
            # Tokens that are YEAR-level only
            year_like = {"%Y", "%y", "%G"}  # include ISO year

            # Timezone tokens (do not affect stepping granularity)
            tz_like = {"%z", "%Z"}  # (%:z normalizes to %z above)

            # If any minute-or-finer token appears, choose 'minute'
            if tset & minute_like:
                return "minute"
            # Else if any hour token appears, choose 'hour'
            if tset & hour_like:
                return "hour"
            # Else if any day-ish token appears, choose 'day'
            if tset & day_like:
                return "day"
            # Else if only year-ish tokens appear, choose 'year'
            if (tset - tz_like) <= year_like and (tset - tz_like):
                return "year"

            # Fallbacks:
            if tset & year_like:
                return "year"
            raise ValueError(
                "Template has directives, but none that imply granularity up to minute/year."
            )

        # --- expansion with pandas date_range ---
        def _align_start(ts: pd.Timestamp, unit: str) -> pd.Timestamp:
            if unit == "minute":
                return ts.floor("T")
            if unit == "hour":
                return ts.floor("h")
            if unit == "day":
                return ts.floor("D")
            # year: align to Jan 1 00:00:00 (start-of-year)
            return pd.Timestamp(year=ts.year, month=1, day=1, tz=ts.tz).floor("D")

        def _align_end(ts: pd.Timestamp, unit: str) -> pd.Timestamp:
            """
            Align the end timestamp up to the next boundary of the unit.
            Example:
            2023-06-01 12:34:56 with unit='hour' → 2023-06-01 13:00:00
            """
            if unit == "minute":
                return ts.ceil("T")
            if unit == "hour":
                return ts.ceil("h")
            if unit == "day":
                return ts.ceil("D")
            if unit == "year":
                return ts.ceil("Y")
            raise ValueError(f"Unsupported unit: {unit}")

        def _freq_for(unit: str) -> str:
            # YS = Year-Start (Jan 1). T = minute
            return {"minute": "T", "hour": "h", "day": "D", "year": "YS"}[unit]

        s = pd.to_datetime(start)
        e = pd.to_datetime(end)

        unit = smallest_unit(source)
        s = _align_start(s, unit)
        e = _align_end(e, unit)

        rng = pd.date_range(
            start=s, end=e, freq=_freq_for(unit), inclusive="left"
        )  # inclusive on left; includes end if aligned

        df["date"] = rng
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
        # TODO: Use engine_path to download from S3 instead of gdal_path.
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
