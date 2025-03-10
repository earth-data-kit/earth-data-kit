from earth_data_kit import stitching
from earth_data_kit import xarray_boosted
from earth_data_kit import utilities
import pandas as pd
import logging
import os

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
logging.addLevelName(logging.CRITICAL, "critical")

logging.getLogger().handlers.clear()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "[%(asctime)s] - [%(name)s:%(lineno)d] - [%(levelname)s] - %(message)s"
)

handler.setFormatter(formatter)
logger.addHandler(handler)
