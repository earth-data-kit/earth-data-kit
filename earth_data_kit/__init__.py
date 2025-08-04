import os

S5CMD_PATH = os.environ.get("S5CMD_PATH", "/usr/bin/s5cmd")
os.environ["GDAL_HTTP_TCP_KEEPALIVE"] = "YES"
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
os.environ["GDAL_MAX_DATASET_POOL_SIZE"] = "1024"

from earth_data_kit import stitching
from earth_data_kit import xarray_boosted
from earth_data_kit import utilities
import pandas as pd
import logging
import subprocess

__version__ = "0.1.3.dev20250802"

logger = logging.getLogger(__name__)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

if LOG_LEVEL == "DEBUG":
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", None)  # or 199

logging.basicConfig(level=LOG_LEVEL)

logging.addLevelName(logging.INFO, "info")
logging.addLevelName(logging.ERROR, "error")
logging.addLevelName(logging.DEBUG, "debug")
logging.addLevelName(logging.WARNING, "warning")
logging.addLevelName(logging.CRITICAL, "critical")


logging.getLogger().handlers.clear()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "[%(asctime)s] - [%(name)s:%(lineno)d] - [%(levelname)s] - %(message)s"
)

handler.setFormatter(formatter)
logger.addHandler(handler)


def get_s5cmd_version():
    result = subprocess.run(
        [S5CMD_PATH, "version"], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def get_gdal_version():
    import subprocess

    result = subprocess.run(
        ["gdalinfo", "--version"], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()
