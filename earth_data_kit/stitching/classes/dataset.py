import pandas as pd
import ast
import geopandas as gpd
import logging
from osgeo import osr
import os
import earth_data_kit.utilities.helpers as helpers
import earth_data_kit.utilities.geo as geo
import earth_data_kit.stitching.constants as constants
import earth_data_kit.stitching.decorators as decorators
import earth_data_kit.stitching.engines.earth_engine as earth_engine
import earth_data_kit.stitching.engines.s3 as s3
import concurrent.futures
from earth_data_kit.stitching.classes.tile import Tile
import shapely
import numpy as np
import fiona
import json
import xarray as xr
from osgeo import gdal
from tqdm import tqdm
from datetime import datetime
from xml.etree import ElementTree as ET

fiona.drvsupport.supported_drivers["kml"] = "rw"  # type: ignore
fiona.drvsupport.supported_drivers["KML"] = "rw"  # type: ignore


logger = logging.getLogger(__name__)


class Dataset:
    """
    The Dataset class is the main class implemented by the stitching module. It acts as a dataset wrapper and maps to a single remote dataset. A remote dataset can contain multiple files.
    """

    def __init__(self, name, source, engine, clean=True) -> None:
        """
        Initializes a new dataset instance.

        Args:
            name (str): Unique identifier for the dataset.
            source (str): Source identifier (S3 URI or Earth Engine collection ID).
            engine (str): Data source engine - ``s3`` or ``earth_engine``.
            clean (bool, optional): Whether to clean temporary files before processing. Defaults to True.

        Raises:
            Exception: If the provided engine is not supported.

        Example:
            >>> from earth_data_kit.stitching.classes.dataset import Dataset
            >>> ds = Dataset("example_dataset", "LANDSAT/LC08/C01/T1_SR", "earth_engine")
            >>> # Or with S3
            >>> ds = Dataset("example_dataset", "s3://your-bucket/path", "s3")
        """
        if engine not in constants.ENGINES_SUPPORTED:
            raise Exception(f"{engine} not supported")

        self.name = name
        self.time_opts = {}
        self.space_opts = {}
        if engine == "s3":
            self.engine = s3.S3()
        if engine == "earth_engine":
            self.engine = earth_engine.EarthEngine()
        self.source = source
        self.src_options = {}
        self.target_options = {}
        self.catalog_path = f"{self.__get_ds_tmp_path__()}/catalog.csv"
        if clean:
            helpers.delete_dir(f"{self.__get_ds_tmp_path__()}")

    def __str__(self):
        """
        Return a string representation of the Dataset instance.

        This representation includes:
          - name: the unique identifier for the dataset.
          - source: the source identifier accordingly.
          - engine: the name of the engine handling the dataset.
          - time_opts: a tuple indicating the start and end time options.
          - space_opts: the spatial bounding box if it is set.
        """
        s = (
            "edk.stitching.Dataset\n"
            "\tname: {}\n"
            "\tsource: {}\n"
            "\tengine: {}\n"
            "\ttime_opts: {}\n"
            "\tspace_opts: {}".format(
                self.name,
                self.source,
                self.engine.name,
                (self.time_opts.get("start", None), self.time_opts.get("end", None)),
                self.space_opts.get("bbox", None),
            )
        )
        return s

    def set_timebounds(self, start, end, resolution=None):
        """
        Sets time bounds for which we want to download the data.

        Args:
            start (datetime): Start date.
            end (datetime): End date, inclusive.
            resolution (str, optional): Temporal resolution for combining images.
                                       Options include 'daily'.

        Example:
            >>> import datetime
            >>> from earth_data_kit.stitching import Dataset
            >>> ds = Dataset("example_dataset", "LANDSAT/LC08/C01/T1_SR", "earth_engine", clean=True)
            >>> ds.set_timebounds(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 12, 31))
        """
        self.time_opts = {"start": start, "end": end, "resolution": resolution}

    def set_src_options(self, options):
        """
        Sets options for the source dataset.

        This method allows setting various options for the source dataset,
        including source nodata values that can be used during processing.

        Args:
            options (dict): A dictionary containing source options.
                Can include '-srcnodata' which may be either:
                - A single value to be applied to all bands
                - An array of values, one for each band in the dataset

        Example:
            >>> ds = Dataset("example", "path/to/data", "file_system")
            >>> # Set a single nodata value for all bands
            >>> ds.set_src_options({"-srcnodata": -9999})
            >>> # Or set different nodata values for each band
            >>> ds.set_src_options({"-srcnodata": [-9999, 0, 255]})
        """
        self.src_options = options

    def set_target_options(self, options):
        """
        Sets options for the target dataset.

        This method allows setting various GDAL options for the output dataset,
        which influence the gdalwarp process during VRT creation.

        Args:
            options (dict): A dictionary containing GDAL options.
                Common options include:
                - '-t_srs': Target spatial reference system (projection)
                - '-tr': Target resolution (x y, in target projection units)
                - '-r': Resampling method (nearest, bilinear, cubic, etc.)

        Example:
            >>> ds = Dataset("example", "path/to/data", "file_system")
            >>> # Set target projection, resolution and resampling method
            >>> ds.set_target_options({"-t_srs": "EPSG:3857", "-tr": "30 30", "-r": "bilinear"})
        """
        self.target_options = options

    def set_spacebounds(self, bbox, grid_dataframe=None):
        """
        Configure spatial constraints for the dataset using a bounding box and, optionally, a grid dataframe.

        This method sets up the spatial filtering parameters by specifying a bounding box defined by
        four coordinates in EPSG:4326. Additionally, if a grid dataframe is provided, the method will
        utilize it to accurately pinpoint the scene files to download based on the spatial variables
        in the source path.

        Args:
            bbox (tuple[float, float, float, float]): A tuple of four coordinates in the order
                (min_longitude, min_latitude, max_longitude, max_latitude)/(xmin, ymin, xmax, ymax) defining the spatial extent.
            grid_dataframe (geopandas.GeoDataFrame, optional): A GeoDataFrame containing grid cells with columns that match
                the spatial variables in the source path (e.g., 'h', 'v' for MODIS grid). Each row should have a geometry
                column defining the spatial extent of the grid cell.

        Example:
            >>> import earth_data_kit as edk
            >>> import geopandas as gpd
            >>> ds = edk.stitching.Dataset("example_dataset", "s3://your-bucket/path/{h}/{v}/B01.TIF", "s3")
            >>>
            >>> # Setting spatial bounds using a bounding box:
            >>> ds.set_spacebounds((19.3044861183, 39.624997667, 21.0200403175, 42.6882473822))
            >>>
            >>> # Setting spatial bounds with a grid dataframe:
            >>> gdf = gpd.GeoDataFrame()
            >>> # Assume gdf has columns 'h', 'v' that match the spatial variables in the source path
            >>> # and a 'geometry' column with the spatial extent of each grid cell
            >>> ds.set_spacebounds((19.3044861183, 39.624997667, 21.0200403175, 42.6882473822), grid_dataframe=gdf)
        """
        self.space_opts = {
            "grid_dataframe": grid_dataframe,
        }
        self.space_opts["bbox"] = bbox

    def _get_target_res(self):
        """
        Retrieves the target resolution option from target options.

        Returns:
            str: The target resolution option if found, otherwise None.
        """
        return self.target_options.get("-tr", None)

    def _get_target_srs(self):
        """
        Retrieves the target spatial reference system (SRS) option from target options.

        Returns:
            str: The target SRS option if found, otherwise None.
        """
        return self.target_options.get("-t_srs", None)

    def _get_srcnodata(self):
        """
        Retrieves the source no-data value option from source options.

        Returns:
            str: The source no-data value option if found, otherwise None.
        """
        return self.src_options.get("-srcnodata", None)

    @decorators.log_time
    @decorators.log_init
    def discover(self):
        """
        Scans the dataset source to identify, catalog, and save the intersecting tiles based on
        provided time and spatial constraints.

        This method follows a multi-step workflow:
          1. Invokes the engine's scan method to retrieve a dataframe of available tile metadata
             that match the time and spatial options.
          2. Concurrently retrieves detailed metadata for each tile by constructing Tile objects
             using a ThreadPoolExecutor.
          3. Converts the user-specified bounding box into a Shapely polygon (in EPSG:4326) and
             filters the tiles by comparing each tile's extent (also converted to EPSG:4326) to the
             bounding box using an intersection test.
          4. Saves the catalog of the intersecting tiles as a CSV file at the location specified by
             self.catalog_path.
          5. Discovers the overview information for the dataset.

        Returns:
            None

        Raises:
            Exception: Propagates any exceptions encountered during scanning, metadata retrieval,
                       spatial filtering, or catalog saving.

        Example:
            >>> import datetime
            >>> import earth_data_kit as edk
            >>> import geopandas as gpd
            >>> ds = edk.stitching.Dataset(
            ...     "modis-pds",
            ...     "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B0?.TIF",
            ...     "s3",
            ...     True
            ... )
            >>> ds.set_timebounds(datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))
            >>> # Load grid dataframe
            >>> gdf = gpd.read_file("tests/fixtures/modis.kml")
            >>> gdf['h'] = gdf['Name'].str.split(' ').str[0].str.split(':').str[1].astype(int).astype(str).str.zfill(2)
            >>> gdf['v'] = gdf['Name'].str.split(' ').str[1].str.split(':').str[1].astype(int).astype(str).str.zfill(2)
            >>> ds.set_spacebounds((19.30, 39.62, 21.02, 42.69), grid_dataframe=gdf)
            >>> ds.discover() # This will scan the dataset and save the catalog of intersecting tiles at the location specified by self.catalog_path
        """
        # Retrieve tile metadata using the engine's scan function.
        df = self.engine.scan(
            self.source, self.time_opts, self.space_opts, self.__get_ds_tmp_path__()
        )

        # Concurrently fetch metadata and construct Tile objects for each tile.
        futures = []
        tiles = []
        logger.debug(f"Fetching metadata of {len(df)} tiles")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            # Submit all tasks and store them in a list
            futures = [
                executor.submit(
                    Tile,
                    row.engine_path,
                    row.gdal_path,
                    row.date,
                    row.tile_name,
                )
                for row in df.itertuples()
            ]

            # Create a progress bar and iterate directly through futures
            for future in tqdm(futures, desc="Processing tiles", unit="tile"):
                try:
                    result = future.result()
                    tiles.append(result)
                except Exception as e:
                    logger.error(f"Error processing tile: {e}")

        # Define the user-specified bounding box as a Shapely polygon for intersection tests.
        bbox = shapely.geometry.box(*self.space_opts["bbox"], ccw=True)  # type: ignore
        intersecting_tiles = []
        for tile in tiles:
            tile_bbox = shapely.geometry.box(*tile.get_wgs_extent(), ccw=True)
            if shapely.intersects(tile_bbox, bbox):
                intersecting_tiles.append(tile)

        # Save the catalog of intersecting tiles as a CSV file.
        pd.DataFrame([t.__dict__ for t in intersecting_tiles]).to_csv(
            f"{self.catalog_path}", header=True, index=False
        )

        logger.debug(f"Catalog for dataset {self.name} saved at {self.catalog_path}")

    def get_bands(self):
        """
        Retrieve unique band configurations from tile metadata.

        Aggregates metadata from each tile by extracting attributes such as resolution
        (x_res, y_res) and coordinate reference system (crs). The data is then grouped
        by columns: band index inside tile (source_idx), band description, data type (dtype),
        x_res, y_res, and crs.

        Returns:
            pd.DataFrame: A DataFrame with unique band configurations, where each row represents
                          a unique band configuration with the following columns:
                          - source_idx: Band index within the source files
                          - description: Band description
                          - dtype: Data type of the band
                          - x_res: X resolution
                          - y_res: Y resolution
                          - crs: Coordinate reference system
                          - tiles: List of Tile objects that contain this band configuration

        Example:
            >>> import datetime
            >>> import earth_data_kit as edk
            >>> import geopandas as gpd
            >>> # Initialize the dataset
            >>> ds = edk.stitching.Dataset("modis-pds", "s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B0?.TIF", "s3", True)
            >>> ds.set_timebounds(datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 2))
            >>> # Load grid dataframe
            >>> gdf = gpd.read_file("tests/fixtures/modis.kml")
            >>> gdf['h'] = gdf['Name'].str.split(' ').str[0].str.split(':').str[1].astype(int).astype(str).str.zfill(2)
            >>> gdf['v'] = gdf['Name'].str.split(' ').str[1].str.split(':').str[1].astype(int).astype(str).str.zfill(2)
            >>> ds.set_spacebounds((19.30, 39.62, 21.02, 42.69), grid_dataframe=gdf)
            >>> ds.discover()
            >>> bands_df = ds.get_bands()
            >>> print(bands_df.head())
               source_idx                description    dtype  x_res  y_res         crs                                              tiles
            0           1  Nadir_Reflectance_Band1  uint16   30.0   30.0   EPSG:4326  [<earth_data_kit.stitching.classes.tile.Tile object...
            1           1  Nadir_Reflectance_Band2  uint16   30.0   30.0   EPSG:4326  [<earth_data_kit.stitching.classes.tile.Tile object...
            2           1  Nadir_Reflectance_Band3  uint16   30.0   30.0   EPSG:4326  [<earth_data_kit.stitching.classes.tile.Tile object...

        Notes:
            The 'source_idx' column typically represents the band index within the source files.
            In some cases, this value will be 1 for all bands, especially when each band
            is stored in a separate file.
        """
        tile_bands = self.__get_tile_bands__()
        df = pd.DataFrame(tile_bands)
        df["x_res"] = df.apply(lambda row: row.tile.get_res()[0], axis=1)
        df["y_res"] = df.apply(lambda row: row.tile.get_res()[1], axis=1)
        df["crs"] = df.apply(lambda row: row.tile.crs, axis=1)
        # Group the bands by columns: source_idx, description, dtype, x_res, y_res, and crs.
        by = ["source_idx", "description", "dtype", "x_res", "y_res", "crs"]
        df["x_res"] = df["x_res"].astype(np.float32)
        df["y_res"] = df["y_res"].astype(np.float32)

        # Group by the specified columns and collect tiles in each group
        grouped = df.groupby(by=by)
        # Create a new dataframe with the grouped columns and a new 'tiles' column
        result = grouped.agg({"tile": lambda x: list(x)}).reset_index()
        # Rename the aggregated column to 'tiles'
        result = result.rename(columns={"tile": "tiles"})
        return result

    def __get_tile_bands__(self):
        """
        Collect band metadata from all tiles.

        Iterates over each tile (from __get_tiles__) and builds a list of band dictionaries,
        each enhanced with its corresponding tile information.

        Returns:
            list: List of band dictionaries.
        """
        tiles = self.__get_tiles__()

        tile_bands = []
        for tile in tiles:
            for i in range(len(tile.bands)):
                band = tile.bands[i]
                band["tile"] = tile
                tile_bands.append(band)

        return tile_bands

    def __get_tiles__(self):
        """
        Load tiles from the catalog CSV.

        Reads the catalog file, converts necessary fields, and creates Tile objects.

        Returns:
            list: List of Tile objects.
        """
        df = pd.read_csv(self.catalog_path)
        df["bands"] = df["bands"].apply(json.loads)
        df["geo_transform"] = df["geo_transform"].apply(ast.literal_eval)
        df["date"] = pd.to_datetime(df["date"])

        tiles = Tile.from_df(df)
        return tiles

    def __get_ds_tmp_path__(self):
        """
        Get the temporary directory path for dataset processing.

        Ensures the directory exists.

        Returns:
            str: Path to the temporary directory.
        """
        path = f"{helpers.get_tmp_dir()}/{self.name}"
        helpers.make_sure_dir_exists(path)
        return path

    def __to_meter__(self, val, unit):
        """
        Convert a length value to meters.

        If the unit is 'meter' or 'metre', returns the value unchanged.
        If the unit is 'degree', applies an approximate conversion factor.

        Args:
            val (float): The value to convert.
            unit (str): The unit of the value.

        Returns:
            float: Value converted to meters.
        """
        if (unit == "metre") or (unit == "meter") or (unit == "m"):
            return val
        elif unit == "degree":
            conversion_factor = 111320  # approximate conversion factor: meters per degree at the equator
            val = val * conversion_factor
            return val

    @decorators.log_time
    @decorators.log_init
    def __extract_band__(self, band_tile):
        """
        Extract and warp a specific band into a VRT file.

        Args:
            band_tile (object): The band descriptor containing attributes such as tile, idx, and nodataval.

        Returns:
            str: Path to the generated warped VRT.
        """
        warped_vrt_path = f"{self.__get_ds_tmp_path__()}/pre-processing/{band_tile.tile.tile_name}-band-{band_tile.source_idx}-warped.vrt"

        nodataval = (
            band_tile.nodataval
            if band_tile.nodataval is not None
            else self._get_srcnodata()
        )
        if nodataval is None:
            logger.warning(
                "no data val set as None. it's advised to provide a nodataval"
            )

        if (self._get_target_res() is None and self._get_target_srs() is not None) or (
            self._get_target_res() is not None and self._get_target_srs() is None
        ):
            # It's important to supply either both tr and t_srs or nothing as in cases when only one is supplied system converts the resolution in wrong units
            logger.warning("either supply both -tr and -t_srs or supply nothing")

        t_srs = self._get_target_srs() or "EPSG:3857"

        if self._get_target_res() is not None:
            tr = self._get_target_res().split(" ")
        else:
            tr = (
                self.__to_meter__(
                    band_tile.tile.get_res()[0], band_tile.tile.length_unit
                ),
                self.__to_meter__(
                    band_tile.tile.get_res()[1], band_tile.tile.length_unit
                ),
            )

        options = gdal.WarpOptions(
            outputBounds=self.space_opts["bbox"],
            outputBoundsSRS="EPSG:4326",
            xRes=tr[0],
            yRes=tr[1],
            dstSRS=t_srs,
            srcNodata=nodataval,
            srcBands=[band_tile.source_idx],
            dstBands=[1],
            errorThreshold=0,
            targetAlignedPixels=True,
            format="VRT",
        )

        gdal.Warp(warped_vrt_path, band_tile.tile.gdal_path, options=options)

        return warped_vrt_path

    @decorators.log_time
    @decorators.log_init
    def __create_band_mosaic__(self, band_tiles, date, bands):
        """
        Create mosaic VRT files for the specified bands.

        For each band, filters the relevant tiles, writes an input file list,
        and creates a mosaic VRT using gdalbuildvrt.

        Args:
            band_tiles (pd.DataFrame): DataFrame with VRT paths for each band.
            date (datetime): Date associated with the mosaics.
            bands (list): List of band descriptions to assemble.

        Returns:
            list: Paths to the created band mosaic VRTs.
        """
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        band_mosaics = []
        for idx in range(len(bands)):
            current_bands_df = band_tiles[band_tiles["description"] == bands[idx]]
            band_mosaic_path = f"{self.__get_ds_tmp_path__()}/pre-processing/{date_str}-{bands[idx]}.vrt"

            ds = gdal.BuildVRT(
                destName=band_mosaic_path,
                srcDSOrSrcDSTab=current_bands_df["vrt_path"].tolist(),
            )
            # This saves the vrt file
            ds.Close()

            band_mosaics.append(band_mosaic_path)
        return band_mosaics

    @decorators.log_time
    @decorators.log_init
    def __stack_band_mosaics__(self, band_mosaics, date):
        """
        Stack individual band mosaic VRTs into a multi-band VRT.

        Combines the mosaic VRTs using gdalbuildvrt with the -separate option.

        Args:
            band_mosaics (list): List of band mosaic VRT paths.
            date (datetime): Date used for naming the combined VRT.

        Returns:
            str: Path to the stacked multi-band VRT.
        """
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        output_vrt = f"{self.__get_ds_tmp_path__()}/pre-processing/{date_str}.vrt"

        ds = gdal.BuildVRT(
            destName=output_vrt, srcDSOrSrcDSTab=band_mosaics, separate=True
        )
        ds.Close()

        return output_vrt

    @decorators.log_time
    @decorators.log_init
    def __combine_timestamped_vrts__(self, output_vrts):
        """
        Combine multiple timestamped VRT files into a single JSON file.

        This method creates a JSON file that references all the VRTs with their corresponding timestamps.
        The JSON structure follows the EDKDataset format, where each VRT is represented as a VRTDataset
        element with source and time attributes. The time attribute is extracted from the VRT filename.

        The resulting JSON file serves as a temporal index for the dataset, allowing for time-based
        queries and operations on the collection of VRTs.

        Args:
            output_vrts (list): List of VRT file paths to combine. Each path should contain
                                a timestamp in the format YYYY-MM-DD-HH:MM:SS in its filename.

        Returns:
            str: Path to the created JSON file, or None if output_vrts is empty.

        Example JSON structure:
            {
              "EDKDataset": {
                "name": "dataset_name",
                "source": "source_identifier",
                "engine": "engine_name",
                "catalog": "path/to/catalog.csv",
                "bbox": [xmin, ymin, xmax, ymax],
                "timebounds": ["start_date", "end_date"],
                "VRTDatasets": [
                  {
                    "source": "/path/to/2017-01-01-00:00:00.vrt",
                    "time": "2017-01-01-00:00:00",
                    "has_time_dim": true
                  },
                  {
                    "source": "/path/to/2017-01-02-00:00:00.vrt",
                    "time": "2017-01-02-00:00:00",
                    "has_time_dim": true
                  }
                ]
              }
            }
        """
        if not output_vrts:
            return None

        # Create a dictionary structure for JSON
        dataset_dict = {
            "EDKDataset": {
                "name": self.name,
                "source": self.source,
                "engine": self.engine.name,
                "catalog": self.catalog_path,
                "bbox": self.space_opts.get("bbox"),
                "timebounds": [
                    self.time_opts.get("start").strftime("%Y-%m-%d-%H:%M:%S") if self.time_opts.get("start") else None,
                    self.time_opts.get("end").strftime("%Y-%m-%d-%H:%M:%S") if self.time_opts.get("end") else None
                ],
                "VRTDatasets": [],
            }
        }

        # Add VRTDataset entries for each VRT file
        for vrt in output_vrts:
            date_str = vrt.split("/")[-1].split(".")[0]
            vrt_dataset = {
                "source": vrt,
                "time": date_str,
                "has_time_dim": date_str != "1970-01-01-00:00:00",
            }
            dataset_dict["EDKDataset"]["VRTDatasets"].append(vrt_dataset)

        # Create JSON file path
        json_path = f"{self.__get_ds_tmp_path__()}/{self.name}.json"

        # Write the JSON file with pretty formatting
        with open(json_path, "w") as json_file:
            json.dump(dataset_dict, json_file, indent=2)

        return json_path

    @decorators.log_time
    @decorators.log_init
    def __create_timestamped_vrt__(self, date, band_tiles, bands):
        """
        Create a timestamped VRT file for a specific date from band tiles.

        This method processes band tiles for a given date to create a multi-band VRT file.
        The process involves several steps:
        1. Extract each band as a single-band VRT
        2. Create mosaic VRTs for each band
        3. Stack these mosaics into a multi-band VRT
        4. Set appropriate band descriptions

        Args:
            date (tuple): A tuple containing the datetime object for which to create the VRT. First element is the datetime object
            band_tiles (DataFrame): DataFrame containing band tile information
            bands (list): List of band descriptions to include in the VRT

        Returns:
            str: Path to the created VRT file

        Note:
            The function creates temporary single-band VRTs that are reprojected to EPSG:3857
            by default unless overridden by options set via set_target_options.
        """
        curr_date = date[0]
        _band_tiles = band_tiles.copy(deep=True).reset_index(drop=True)
        
        # Extract each required band as a single-band VRT.
        # These VRTs are reprojected to EPSG:3857 by default to achieve a consistent resolution,
        # unless overridden by options configured via set_target_options.
        # Process band tiles in parallel using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            # Create arrays to store futures and their corresponding indices
            futures = []
            indices = []

            # Submit all band extraction tasks to the executor
            for _bt in _band_tiles.itertuples():
                future = executor.submit(self.__extract_band__, _bt)
                futures.append(future)
                indices.append(_bt.Index)

            # Process results as they complete
            for i, future in enumerate(futures):
                vrt_path = future.result()
                _band_tiles.at[indices[i], "vrt_path"] = vrt_path

        # Combine single-band VRTs to create mosaic VRTs per band.
        # Note: Although GDAL Raster Tile Index (GTI) is preferred for large tile counts,
        # it was avoided here due to issues with metadata copying (e.g., ColorInterp).
        # VRT format has better support than GTI for preserving metadata.
        band_mosaics = self.__create_band_mosaic__(_band_tiles, curr_date, bands)

        # Stack the band mosaics into a multi-band VRT
        output_vrt = self.__stack_band_mosaics__(band_mosaics, curr_date)

        # Set the descriptions for each band in the output VRT
        geo.set_band_descriptions(output_vrt, bands)

        return output_vrt

    @decorators.log_time
    @decorators.log_init
    def mosaic(self, bands):
        """
        Stitches the scene files together into VRTs based on the ordered band arrangement provided.
        For each unique date, this function extracts the required bands from the tile metadata and
        creates individual single-band VRTs that are reprojected to EPSG:3857 by default (if no target spatial reference
        is specified). These single-band VRTs are then mosaiced per band and finally stacked into a multi-band VRT.

        GDAL options influencing the gdalwarp process can be configured using the :meth:`edk.stitching.Dataset.set_target_options` function.
        Also look at :meth:`edk.stitching.Dataset.set_src_options` for source dataset related options influencing the gdalwarp process, eg: srcnodataval.

        Args:
            bands (list[string]): Ordered list of band descriptions to output as VRTs.

        Example:
            >>> import datetime
            >>> import earth_data_kit as edk
            >>> ds = edk.stitching.Dataset("example_dataset", "s3://your-bucket-name/path/to/data", "s3")
            >>> ds.set_timebounds(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 31))
            >>> ds.discover()  # Discover available scene files before stitching
            >>> bands = ["red", "green", "blue"]
            >>> ds.mosaic(bands)  # Use mosaic instead of to_vrts
            >>> ds.save()  # Save the output VRTs to a JSON file
        """
        # Ensuring the pre-processing directory exists
        helpers.make_sure_dir_exists(f"{self.__get_ds_tmp_path__()}/pre-processing")

        # Retrieve all bands from tiles.
        tile_bands = self.__get_tile_bands__()
        df = pd.DataFrame(tile_bands)
        df["date"] = df.apply(lambda x: x.tile.date, axis=1)

        # Filter bands based on the user-supplied list.
        # TODO: May need special handling for non-unique band descriptions in the future
        df = df[df["description"].isin(bands)]

        # Handle non-temporal datasets by filling missing dates with Jan 1, 1970
        epoch_date = datetime(1970, 1, 1, 0, 0, 0)
        df["date"] = df["date"].fillna(epoch_date)

        outputs_by_dates = df.groupby(by=["date"], dropna=False)
        output_vrts = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            futures = []
            # Iterate over each date group to create VRTs.
            for date, band_tiles in outputs_by_dates:
                futures.append(
                    executor.submit(
                        self.__create_timestamped_vrt__, date, band_tiles, bands
                    )
                )

            # Create a progress bar and iterate through futures
            for future in tqdm(futures, desc="Creating VRTs", unit="vrt"):
                result = future.result()
                output_vrts.append(result)

        self.output_vrts = output_vrts

    def save(self):
        """
        Saves the mosaiced VRTs into a combined JSON file.
        
        This method should be called after the `mosaic()` method to save the generated VRTs.
        The resulting JSON path is stored in the `json_path` attribute.
        
        Returns:
            None
        """
        json_path = self.__combine_timestamped_vrts__(self.output_vrts)
        self.json_path = json_path

    @decorators.log_time
    @decorators.log_init
    def to_dataarray(self):
        """
        Converts the dataset to an xarray DataArray.

        This method opens the JSON file created by `save()` using xarray with the 'edk_dataset' engine
        and returns the DataArray corresponding to this dataset.

        Returns:
            xarray.DataArray: A DataArray containing the dataset's data with dimensions for time, bands,
            and spatial coordinates.

        Example:
            >>> import earth_data_kit as edk
            >>> import datetime
            >>> ds = edk.stitching.Dataset("example_dataset", "s3://your-bucket/path", "s3")
            >>> ds.set_timebounds(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 31))
            >>> ds.discover()
            >>> ds.mosaic(bands=["red", "green", "blue"])
            >>> ds.save()
            >>> data_array = ds.to_dataarray()

        Note:
            This method requires that `mosaic()` and `save()` have been called first to generate the JSON file.
        """
        json_path = self.json_path

        return Dataset.dataarray_from_file(json_path)
    
    @staticmethod
    def dataarray_from_file(json_path):
        # Extract dataset name from the JSON file
        with open(json_path, 'r') as f:
            dataset_info = json.load(f)
            dataset_name = dataset_info.get('EDKDataset', {}).get('name')
            if not dataset_name:
                # If name not found in expected structure, use filename as fallback
                dataset_name = os.path.basename(os.path.splitext(json_path)[0])


        ds = gdal.Open(json_path)
        x_block_size, y_block_size = ds.GetRasterBand(1).GetBlockSize()
        ds = xr.open_dataset(
            json_path,
            engine="edk_dataset",
            chunks={"time": 1, "band": 1, "x": x_block_size, "y": y_block_size},
        )

        return ds[dataset_name]
