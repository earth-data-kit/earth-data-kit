import pandas as pd
import ast
import geopandas as gpd
import logging
from osgeo import osr
import uuid
import os
import earth_data_kit.utilities.helpers as helpers
import earth_data_kit.utilities.transform as transform
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
import earth_data_kit.stitching.engines.stac as stac
import earth_data_kit.stitching.engines.planetary_comp as planetary_comp


fiona.drvsupport.supported_drivers["kml"] = "rw"  # type: ignore
fiona.drvsupport.supported_drivers["KML"] = "rw"  # type: ignore


logger = logging.getLogger(__name__)


class Dataset:
    """
    The Dataset class is the main class implemented by the stitching module. It acts as a dataset wrapper and maps to a single remote dataset. A remote dataset can contain multiple files.
    """

    def __init__(self, name, source, engine, clean=True) -> None:
        """Initialize a new dataset instance.

        Args:
            name (str): Unique identifier for the dataset
            source (str): Source identifier (S3 URI or Earth Engine collection ID)
            engine (str): Data source engine - ``s3`` or ``earth_engine``
            clean (bool, optional): Whether to clean temporary files before processing. Defaults to True

        Raises:
            Exception: If the provided engine is not supported

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
        if engine == "stac":
            self.engine = stac.STAC()
        if engine == "planetary_comp":
            self.engine = planetary_comp.PlanetaryComp()
        self.source = source

        self.catalog_path = f"{self.__get_ds_tmp_path__()}/catalog.csv"
        if clean:
            helpers.delete_dir(f"{self.__get_ds_tmp_path__()}")

    def __str__(self):
        """Return string representation of Dataset instance including name, source, engine, time options, and spatial bounds."""
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
        """Set time bounds for data download and optional temporal resolution for combining images.

        Args:
            start (datetime): Start date
            end (datetime): End date (inclusive)
            resolution (str, optional): Temporal resolution (e.g., 'D' for daily, 'W' for weekly, 'M' for monthly)
                See pandas offset aliases for full list:
                https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases

        Example:
            >>> import datetime
            >>> from earth_data_kit.stitching import Dataset
            >>> ds = Dataset("example_dataset", "LANDSAT/LC08/C01/T1_SR", "earth_engine", clean=True)
            >>> ds.set_timebounds(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 12, 31))
            >>> # Set daily resolution
            >>> ds.set_timebounds(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 12, 31), resolution='D')
            >>> # Set monthly resolution
            >>> ds.set_timebounds(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 12, 31), resolution='M')
        """
        self.time_opts = {"start": start, "end": end, "resolution": resolution}

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

    @decorators.log_time
    @decorators.log_init
    def discover(self, band_locator="description"):
        """
        Scans the dataset source to identify, catalog, and save the intersecting tiles based on
        provided time and spatial constraints.

        This method follows a multi-step workflow:
          1. Invokes the engine's scan method to retrieve a dataframe of available tile metadata
             that match the time and spatial options.
          2. Handles any subdatasets found in the scan results.
          3. Concurrently retrieves detailed metadata for each tile by constructing Tile objects
             using a ThreadPoolExecutor.
          4. Converts the user-specified bounding box into a Shapely polygon (in EPSG:4326) and
             filters the tiles by comparing each tile's extent (also converted to EPSG:4326) to the
             bounding box using an intersection test.
          5. Saves the catalog of the intersecting tiles as a CSV file at the location specified by
             self.catalog_path.

        Args:
            band_locator (str, optional): Specifies how to locate bands in the dataset.
                Defaults to "description". Valid options are "description", "color_interp", "filename".

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
            >>> ds.discover() # This will scan the dataset and save the catalog of intersecting tiles
        """
        # Retrieve tile metadata using the engine's scan function
        tiles = self.engine.scan(
            self.source,
            self.time_opts,
            self.space_opts,
            self.__get_ds_tmp_path__(),
            band_locator,
        )

        # Filter tiles by spatial intersection with bounding box, some engines will handle this in the scan function
        bbox = shapely.geometry.box(*self.space_opts["bbox"], ccw=True)  # type: ignore
        intersecting_tiles = [
            tile
            for tile in tiles
            if shapely.intersects(
                shapely.geometry.box(*tile.get_wgs_extent(), ccw=True), bbox
            )
        ]

        if len(intersecting_tiles) == 0:
            raise Exception("No tiles found for the given time and spatial constraints")

        # Converting bands column to string while saving to csv
        df = pd.DataFrame([t.__dict__ for t in intersecting_tiles])
        df["bands"] = df["bands"].apply(json.dumps)

        # Save catalog of intersecting tiles
        df.to_csv(self.catalog_path, header=True, index=False)

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
        df["date"] = pd.to_datetime(df["date"], format="ISO8601")

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

    def __optimize_gdal_path__(self, gdal_path, band):
        """
        Optimize the GDAL path for the Earth Engine engine.
        """
        if self.engine.name == "earth_engine":
            path_parts = gdal_path.split(":")
            if len(path_parts) > 2:
                gdal_path = f"EEDAI:{path_parts[1]}:{band}"
                return gdal_path
            # This will return None if the band is not found in path. That should be okay as we will GEE driver supports bands according to docs, so any issue in that will be a problem of GDAL
        else:
            return gdal_path

    def __validate_band_properties__(self, tiles_df, resolution, dtype, crs):
        """
        Validates that all tiles in a band have consistent data types, crs, and resolutions.
        Ensures tiles can be properly mosaicked together.

        Args:
            tiles_df (pd.DataFrame): DataFrame with tile info (dtype, tile objects with projection/resolution)

        Raises:
            ValueError: If tiles have mismatched data types, crs, or resolutions
        """
        # Get unique values for each property
        unique_dtypes = tiles_df["dtype"].unique()
        unique_crs = tiles_df["tile"].apply(lambda x: x.crs).unique()
        unique_resolutions = tiles_df["tile"].apply(lambda x: x.get_res()).unique()

        # Check data type consistency
        if len(unique_dtypes) > 1 and dtype is None:
            raise ValueError(
                f"Band tiles have inconsistent data types. Found: {', '.join(map(str, unique_dtypes))}. "
                "Please pass the desired data type using the 'dtype' parameter in mosaic() function."
            )

        # Check CRS consistency
        if len(unique_crs) > 1 and crs is None:
            raise ValueError(
                f"Band tiles have inconsistent coordinate reference systems (CRS). Found: {', '.join(map(str, unique_crs))}. "
                "Please pass the desired CRS using the 'crs' parameter in mosaic() function."
            )

        # Check resolution consistency
        if len(unique_resolutions) > 1 and resolution is None:
            raise ValueError(
                f"Band tiles have inconsistent resolutions. Found: {', '.join(map(str, unique_resolutions))}. "
                "Please pass the desired resolution using the 'resolution' parameter in mosaic() function."
            )

    @decorators.log_time
    @decorators.log_init
    def __create_band_mosaic__(self, band_tiles, date, bands, resolution, dtype, crs):
        """
        Create mosaic VRT files for the specified bands.

        For each band, filters the relevant tiles, validates their properties, and creates a mosaic VRT
        using gdalbuildvrt. Handles both Earth Engine and standard GDAL data sources differently.

        Args:
            band_tiles (pd.DataFrame): DataFrame containing tile information including band descriptions
                                      and GDAL paths for each tile.
            date (datetime): Date associated with the mosaics, used in output filenames.
            bands (list): List of band descriptions to assemble into mosaics.

        Returns:
            list: Paths to the created band mosaic VRT files.

        Note:
            For Earth Engine data sources, band paths are optimized and band index is set to 1.
            For standard GDAL sources, band index is taken from the source_idx column.
        """
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        band_mosaics = []

        for band_desc in bands:
            # Filter tiles for current band
            current_bands_df = band_tiles[band_tiles["description"] == band_desc]
            current_bands_df = current_bands_df.assign(
                gdal_path=current_bands_df["tile"].apply(lambda x: x.gdal_path)
            )

            # Set up output path and validate band properties
            band_mosaic_path = f"{self.__get_ds_tmp_path__()}/pre-processing/{date_str}-{band_desc}.vrt"
            self.__validate_band_properties__(current_bands_df, resolution, dtype, crs)

            if (resolution is None and crs is not None) or (
                resolution is not None and crs is None
            ):
                raise ValueError(
                    "Both 'resolution' and 'crs' parameters must be provided together, or neither should be provided. "
                    "Found only one of them."
                )

            gdal_paths = []
            if resolution is not None or crs is not None:
                for row in current_bands_df.itertuples():
                    # Adding uuid to avoid conflicts
                    warped_vrt_path = f"{self.__get_ds_tmp_path__()}/pre-processing/{row.gdal_path.split('/')[-1].split('.')[0]}-{uuid.uuid4()}-warped.vrt"
                    options = gdal.WarpOptions(
                        format="VRT",
                        xRes=resolution[0],
                        yRes=resolution[1],
                        dstSRS=crs,
                    )
                    gdal.Warp(
                        warped_vrt_path,
                        row.gdal_path,
                        options=options,
                    )
                    gdal_paths.append(warped_vrt_path)
            else:
                gdal_paths = current_bands_df["gdal_path"].tolist()

            # Create and save the VRT mosaic
            ds = gdal.BuildVRT(
                destName=band_mosaic_path,
                srcDSOrSrcDSTab=gdal_paths,
                separate=False,
                bandList=[current_bands_df.iloc[0]["source_idx"]],
            )
            ds.Close()

            band_mosaics.append(band_mosaic_path)

        return band_mosaics

    @decorators.log_time
    @decorators.log_init
    def __stack_band_mosaics__(self, band_mosaics, date):
        """
        Stack individual band mosaic VRTs into a multi-band VRT and clip to spatial bounds.

        Combines the mosaic VRTs using gdalbuildvrt with the -separate option, then clips
        the result to the dataset's spatial bounds using gdal.Translate.

        Args:
            band_mosaics (list): List of band mosaic VRT paths to combine.
            date (datetime): Date used for naming the combined VRT.

        Returns:
            str: Path to the stacked and clipped multi-band VRT.
        """
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        output_vrt = f"{self.__get_ds_tmp_path__()}/pre-processing/{date_str}.vrt"
        tmp_vrt = f"{self.__get_ds_tmp_path__()}/pre-processing/{date_str}.tmp.vrt"

        # Stack band mosaics into a temporary multi-band VRT
        ds = gdal.BuildVRT(
            destName=tmp_vrt, srcDSOrSrcDSTab=band_mosaics, separate=True
        )
        ds.Close()

        # Extract spatial bounds and clip the stacked VRT
        xmin, ymin, xmax, ymax = self.space_opts["bbox"]
        gdal.Translate(
            output_vrt,
            tmp_vrt,
            projWin=[xmin, ymax, xmax, ymin],
            projWinSRS="EPSG:4326",
        )

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
                    (
                        self.time_opts.get("start").strftime("%Y-%m-%d-%H:%M:%S")
                        if self.time_opts.get("start")
                        else None
                    ),
                    (
                        self.time_opts.get("end").strftime("%Y-%m-%d-%H:%M:%S")
                        if self.time_opts.get("end")
                        else None
                    ),
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
    def __create_timestamped_vrt__(
        self, date, band_tiles, bands, resolution, dtype, crs
    ):
        """
        Create a timestamped VRT file for a specific date from band tiles.

        This method processes band tiles for a given date to create a multi-band VRT file.
        The process involves several steps:
        1. Extract each band as a single-band VRT
        2. Create mosaic VRTs for each band
        3. Stack these mosaics into a multi-band VRT
        4. Set appropriate band descriptions

        Args:
            date (tuple): A tuple containing the datetime object for which to create the VRT.
                         First element is the datetime object.
            band_tiles (DataFrame): DataFrame containing band tile information.
            bands (list): List of band descriptions to include in the VRT.

        Returns:
            str: Path to the created VRT file.
        """
        curr_date = date[0]
        _band_tiles = band_tiles.copy(deep=True).reset_index(drop=True)

        # Create mosaic VRTs for each band by combining single-band VRTs.
        # Note: GDAL Raster Tile Index (GTI) was considered but not used due to metadata
        # preservation limitations (e.g., ColorInterp). VRT format provides better metadata support.
        band_mosaics = self.__create_band_mosaic__(
            _band_tiles, curr_date, bands, resolution, dtype, crs
        )

        # Create multi-band VRT by stacking individual band mosaics
        output_vrt = self.__stack_band_mosaics__(band_mosaics, curr_date)

        # Apply band descriptions to the output VRT
        geo.set_band_descriptions(output_vrt, bands)

        return output_vrt

    @decorators.log_time
    @decorators.log_init
    def mosaic(
        self, bands, sync=False, overwrite=False, resolution=None, dtype=None, crs=None
    ):
        """
        Identifies and extracts the required bands from the tile metadata for each unique date. For each band,
        it creates a single-band VRT that is then mosaiced together. These individual band mosaics are finally
        stacked into a multi-band VRT according to the ordered band arrangement provided.

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
        helpers.delete_dir(f"{self.__get_ds_tmp_path__()}/pre-processing")
        # Ensuring the pre-processing directory exists
        helpers.make_sure_dir_exists(f"{self.__get_ds_tmp_path__()}/pre-processing")

        if not sync and (resolution is not None or crs is not None):
            logger.warning(
                "When resampling (resolution or crs specified), sync=True is required. "
                "This is because warping remote datasets is slow and inefficient. "
                "Please set sync=True to download the data locally when mosaicing."
            )
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

        if sync:
            df = self.engine.sync(df, self.__get_ds_tmp_path__(), overwrite=overwrite)

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
                        self.__create_timestamped_vrt__,
                        date,
                        band_tiles,
                        bands,
                        resolution,
                        dtype,
                        crs,
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
        """
        Creates an xarray DataArray from a JSON file created by the `save()` method.

        Automatically determines optimal chunking based on the underlying raster block size.

        Args:
            json_path (str): Path to the JSON file containing dataset information.

        Returns:
            xarray.DataArray: DataArray with dimensions for time, bands, and spatial coordinates.

        Example:
            >>> import earth_data_kit as edk
            >>> data_array = edk.stitching.Dataset.dataarray_from_file("path/to/dataset.json")

        Note:
            Loads a previously saved dataset without needing to recreate the Dataset object.
        """
        # Extract dataset name from the JSON file
        with open(json_path, "r") as f:
            dataset_info = json.load(f)
            dataset_name = dataset_info.get("EDKDataset", {}).get("name")
            if not dataset_name:
                # If name not found in expected structure, use filename as fallback
                dataset_name = os.path.basename(os.path.splitext(json_path)[0])

        # Get the first VRT file path from VRTDatasets in EDKDataset
        first_vrt_path = None
        if dataset_info.get("EDKDataset", {}).get("VRTDatasets"):
            vrt_datasets = dataset_info["EDKDataset"]["VRTDatasets"]
            if vrt_datasets and len(vrt_datasets) > 0:
                first_vrt = vrt_datasets[0]
                if isinstance(first_vrt, dict) and "source" in first_vrt:
                    first_vrt_path = first_vrt["source"]

        ds = gdal.Open(first_vrt_path)
        x_block_size, y_block_size = ds.GetRasterBand(1).GetBlockSize()

        # Check if block sizes are powers of 2
        def is_power_of_two(n):
            return n > 0 and (n & (n - 1)) == 0

        if is_power_of_two(x_block_size) and is_power_of_two(y_block_size):
            # Block sizes are powers of 2, ensure minimum size is 512
            x_chunk_size = max(x_block_size, 512)
            y_chunk_size = max(y_block_size, 512)
        else:
            # Default to 512 if not powers of 2
            x_chunk_size, y_chunk_size = x_block_size, y_block_size

        ds = xr.open_dataset(
            json_path,
            engine="edk_dataset",
            chunks={"time": 1, "band": 1, "x": x_chunk_size, "y": y_chunk_size},
        )

        return ds[dataset_name]
