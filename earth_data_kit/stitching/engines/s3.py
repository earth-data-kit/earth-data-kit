import os
import datetime
from urllib.parse import urlparse
from osgeo import gdal
import pandas as pd
import logging
import earth_data_kit.utilities.helpers as helpers
import re
import shapely
import copy
import earth_data_kit as edk

logger = logging.getLogger(__name__)


class S3:
    def __init__(self) -> None:
        self.name = "s3"

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
                return ts.floor("min")
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
                return ts.ceil("min")
            if unit == "hour":
                return ts.ceil("h")
            if unit == "day":
                return ts.ceil("D")
            if unit == "year":
                return ts.ceil("Y")
            raise ValueError(f"Unsupported unit: {unit}")

        def _freq_for(unit: str) -> str:
            # YS = Year-Start (Jan 1). T = minute
            return {"minute": "min", "hour": "h", "day": "D", "year": "YS"}[unit]

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

        new_patterns_df = pd.DataFrame(
            new_patterns, columns=pd.Index(["date", "search_path"])
        )

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

    # TODO: Use s5cmd run commands.txt as its much faster than ThreadPoolExecutor.
    # TODO: Also add functionality to do search based on year but
    # date creation based on the full wildcard pattern supplied by the user. We might have to filter the paths later
    def scan(self, source, time_opts, space_opts, tmp_base_dir, band_locator):
        patterns_df = self.get_patterns(source, time_opts, space_opts)
        if "date" not in patterns_df.columns:
            patterns_df["date"] = None

        s = "ls " + patterns_df["search_path"]
        s.to_csv(f"{tmp_base_dir}/commands.txt", index=False, header=False)

        # Running s5cmd and outputting everything to output.json
        os.system(
            f"{edk.S5CMD_PATH} {self.no_sign_flag} {self.request_payer_flag} {self.profile_flag} --json run {tmp_base_dir}/commands.txt > {tmp_base_dir}/output.json"
        )

        # Reading output.json
        df = pd.read_json(f"{tmp_base_dir}/output.json", lines=True)

        files = []
        for row in df.itertuples():
            file = []
            k = row.key
            file.append(k)
            file.append(k.replace("s3://", "/vsis3/"))
            file.append(k.split("/")[-1])
            dt = extract_date_components(k, source)
            file.append(dt)
            files.append(file)

        df = pd.DataFrame(
            files, columns=pd.Index(["engine_path", "gdal_path", "tile_name", "date"])
        )
        df["date"] = df["date"].dt.tz_localize("UTC")

        return df

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


def create_regex_template(template):
    mts = (
        template.replace("*", "(.*)")
        .replace("%Y", r"(?P<year>\d{4})")
        .replace("%-m", r"(?P<month>\d{1,2})")
        .replace("%m", r"(?P<month>\d{2})")
        .replace("%d", r"(?P<day>\d{2})")
    )
    return mts


def create_parts(path):
    parts = urlparse(path).path.strip("/").split("/")
    return parts


def contains_time_component(template):
    comps = ["%Y", "%-m", "%m", "%d"]
    for c in comps:
        if c in template:
            return True


def extract_date_components(test_str, template_str):
    template_parts = create_parts(template_str)
    test_parts = create_parts(test_str)
    year, month, day, hour, min, sec = 1970, 1, 1, 0, 0, 0
    for i in range(len(template_parts)):
        if contains_time_component(template_parts[i]):
            mts = create_regex_template(template_parts[i])
            res = re.search(mts, test_parts[i])
            if res:
                groups = res.groupdict()
                if "year" in groups:
                    year = int(groups["year"])
                if "month" in groups:
                    month = int(groups["month"])
                if "day" in groups:
                    day = int(groups["day"])

    return datetime.datetime(year, month, day, hour, min, sec)
