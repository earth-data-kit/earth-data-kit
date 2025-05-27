import concurrent.futures
import logging
import earth_data_kit.utilities as utilities
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_tiles_metadata(gdal_paths):
    # Concurrently fetch metadata and construct Tile objects
    tiles_md = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=utilities.helpers.get_threadpool_workers()
    ) as executor:
        # Submit all tasks and store futures
        futures = [
            executor.submit(utilities.geo.get_metadata, gdal_path)
            for gdal_path in gdal_paths
        ]

        # Process completed futures with progress bar
        for future in tqdm(futures, desc="Getting metadata", unit="tile"):
            try:
                tiles_md.append(future.result())
            except Exception as e:
                logger.error(f"Error Getting metadata of tile: {e}")

    return tiles_md
