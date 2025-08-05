import concurrent.futures
import logging
import earth_data_kit.utilities as utilities
from tqdm import tqdm
import pandas as pd

logger = logging.getLogger(__name__)


def get_tiles_metadata(gdal_paths, band_locator):
    # Concurrently fetch metadata and construct Tile objects
    tiles_md = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=utilities.helpers.get_threadpool_workers()
    ) as executor:
        # Submit all tasks and store futures
        futures = [
            executor.submit(utilities.geo.get_metadata, gdal_path, band_locator)
            for gdal_path in gdal_paths
        ]

        # Process completed futures with progress bar
        for i, future in enumerate(tqdm(futures, desc="Getting metadata", unit="tile")):
            try:
                tiles_md.append(future.result())
            except Exception as e:
                logger.error(
                    f"Error Getting metadata of tile: {e}. GDAL Path: {gdal_paths[i]}"
                )
                tiles_md.append(None)

    return tiles_md


def aggregate_temporally(df, start, end, resolution):
    # Create date range from start to end with specified resolution, ensuring UTC timezone
    date_range = pd.date_range(start=start, end=end, freq=f"{resolution}", tz="UTC")
    # Create a new column to store the aggregated dates
    df["aggregated_date"] = None

    # Iterate through each date in the range
    for date in date_range:
        # Find all rows where the date falls within this interval
        mask = (df["date"] >= date) & (df["date"] < date + pd.Timedelta(resolution))
        # Update the aggregated_date for matching rows
        df.loc[mask, "aggregated_date"] = date

    # Replace the original date column with aggregated dates
    df["date"] = df["aggregated_date"]
    df = df.drop("aggregated_date", axis=1)

    return df
