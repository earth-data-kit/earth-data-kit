from earth_data_kit.stitching import *

import logging
import os

logger = logging.getLogger(__name__)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

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
