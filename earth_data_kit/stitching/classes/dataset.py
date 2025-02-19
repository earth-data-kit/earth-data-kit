import pandas as pd
import ast
import geopandas as gpd
import logging
from osgeo import osr
import os
from earth_data_kit.stitching import geo, helpers
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

fiona.drvsupport.supported_drivers["kml"] = "rw"  # type: ignore
fiona.drvsupport.supported_drivers["KML"] = "rw"  # type: ignore


logger = logging.getLogger(__name__)


class Dataset:
    """
    The main class implemented by stitching module. Acts as a wrapper above a single remote dataset
    """

    def __init__(self, name, source, engine, clean=False) -> None:
        """
        Initializes a new dataset instance.

        Args:
            name (str): User provided name, should be unique across datasets being opened.
            source (str): Source identifier, Read more :ref:`Defining source`.
            engine (str): Remote datasource engine, accepted values - ``s3``, ``earth_engine``.
            clean (bool, optional): Whether to clean the tmp directory before stitching. Defaults to False.

        Raises:
            Exception: If the provided engine is not supported.
        """
        if engine not in constants.ENGINES_SUPPORTED:
            raise Exception(f"{engine} not supported")

        self.name = name
        self.time_opts = {}
        self.space_opts = {}
        self.gdal_options = {}
        if engine == "s3":
            self.engine = s3.S3()
        if engine == "earth_engine":
            self.engine = earth_engine.EarthEngine()
        self.source = source

        self.catalog_path = f"{self.get_ds_tmp_path()}/catalog.csv"
        if clean:
            helpers.delete_dir(f"{self.get_ds_tmp_path()}")

    def __str__(self):
        s = (
            "edk.Dataset\n"
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

    def set_timebounds(self, start, end):
        """
        Sets time bounds for which we want to download the data.

        Args:
            start (datetime): Start date.
            end (datetime): End date, inclusive.
        """
        self.time_opts = {
            "start": start,
            "end": end,
        }

    def set_spacebounds(self, bbox, grid_file=None, matcher=None):
        """
        Sets spatial bounds using a bounding box (bbox) provided.
        Optionally, you can also provide a grid file and matching function which can then be used to pinpoint the
        exact scene files to download.

        Read more on :ref:`Using a grid file`

        Args:
            bbox (tuple[float, float, float, float]): Bounding box as a set of four coordinates in EPSG:4326.
            grid_file (str, optional): File path to grid file, currently only KML files are supported. Defaults to None.
            matcher (function, optional): Lambda function to extract spatial parts for scene filepaths. Defaults to None.
        """
        self.space_opts = {
            "grid_file": grid_file,
            "matcher": matcher,
        }
        self.space_opts["bbox"] = bbox

    def set_gdal_options(self, options):
        """
        Sets GDAL options for the dataset.

        Args:
            options (dict[str]): Dictionary of GDAL command-line options.
        """
        self.gdal_options = options

    def get_target_resolution(self):
        """
        Retrieves the target resolution option from GDAL options.

        Returns:
            str: The target resolution option if found, otherwise None.
        """
        for opt in self.gdal_options:
            if opt.startswith("-tr"):
                return opt

    def get_target_srs(self):
        """
        Retrieves the target spatial reference system (SRS) option from GDAL options.

        Returns:
            str: The target SRS option if found, otherwise None.
        """
        for opt in self.gdal_options:
            if opt.startswith("-t_srs"):
                return opt

    def get_srcnodata(self):
        """
        Retrieves the source no-data value option from GDAL options.

        Returns:
            str: The source no-data value option if found, otherwise None.
        """
        for opt in self.gdal_options:
            if opt.startswith("-srcnodata"):
                return opt

    @decorators.log_time
    @decorators.log_init
    def discover(self):
        """
        Discovers and catalogs dataset tiles based on the provided time and space options.

        This function scans the source using the engine to list all the tiles, fetches metadata for each tile,
        filters the tiles spatially based on the bounding box, and saves the catalog of intersecting tiles.

        Raises:
            Exception: If any error occurs during the discovery process.
        """
        # Calling the .scan method of engine to list all the tiles
        df = self.engine.scan(
            self.source, self.time_opts, self.space_opts, self.get_ds_tmp_path()
        )

        # Fetching metadata of all files
        futures = []
        tiles = []
        logger.debug(f"Fetching metadata of {len(df)} tiles")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            for row in df.itertuples():
                futures.append(
                    executor.submit(
                        Tile,
                        row.engine_path,
                        row.gdal_path,
                        row.date,
                        row.tile_name,
                    )
                )

        for idx in range(len(futures)):
            future = futures[idx]
            result = future.result()
            logger.debug(f"Fetched metadata for tile at index {idx}")
            tiles.append(result)

        # Filtering spatially. Doing this by re-projecting raster extent to 4326 and running intersects query with bbox
        # This is done as raster can be in different coordinates (eg: UTMs) and much easier to convert multiple to 4326,
        # rather than 4326 to different multiple EPSGs (UTM zones)
        # Add the extent geo-series and set projection

        # Get polygon from user's bbox
        bbox = shapely.geometry.box(*self.space_opts["bbox"], ccw=True)  # type: ignore
        intersecting_tiles = []
        for tile in tiles:
            tile_bbox = shapely.geometry.box(*tile.get_wgs_extent(), ccw=True)
            if shapely.intersects(tile_bbox, bbox):
                intersecting_tiles.append(tile)

        # Saving catalog
        pd.DataFrame([t.__dict__ for t in intersecting_tiles]).to_csv(
            f"{self.catalog_path}", header=True, index=False
        )

        logger.debug(f"Catalog for dataset {self.name} saved at {self.catalog_path}")

    def get_bands(self):
        tile_bands = self.__get_tile_bands__()
        df = pd.DataFrame(tile_bands)
        df["x_res"] = df.apply(lambda row: row.tile.get_res()[0], axis=1)
        df["y_res"] = df.apply(lambda row: row.tile.get_res()[1], axis=1)
        df["crs"] = df.apply(lambda row: row.tile.crs, axis=1)

        by = ["idx", "description", "dtype", "x_res", "y_res", "crs"]
        df["x_res"] = df["x_res"].astype(np.float32)
        df["y_res"] = df["y_res"].astype(np.float32)

        return df.groupby(by=by).size().reset_index()[by]

    def __get_tile_bands__(self):
        tiles = self.__get_tiles__()

        tile_bands = []
        for tile in tiles:
            for i in range(len(tile.bands)):
                band = tile.bands[i]
                band["tile"] = tile
                tile_bands.append(band)

        return tile_bands

    def __get_tiles__(self):
        df = pd.read_csv(self.catalog_path)
        df["bands"] = df["bands"].apply(json.loads)
        df["geo_transform"] = df["geo_transform"].apply(ast.literal_eval)
        df["wgs_geo_transform"] = df["wgs_geo_transform"].apply(ast.literal_eval)
        df["date"] = pd.to_datetime(df["date"])

        tiles = Tile.from_df(df)
        return tiles

    def get_ds_tmp_path(self):
        path = f"{helpers.get_tmp_dir()}/{self.name}"
        helpers.make_sure_dir_exists(path)
        return path

    def __to_meter__(self, val, unit):
        if (unit == "metre") or (unit == "meter"):
            return val
        elif unit == "degree":
            conversion_factor = 111320  # approximate conversion factor: meters per degree at the equator
            val = val * conversion_factor
            return val

    def __extract_band__(self, band_tile):
        warped_vrt_path = f"{self.get_ds_tmp_path()}/pre-processing/{band_tile.tile.tile_name}-band-{band_tile.idx}-warped.vrt"

        if ((self.get_target_resolution() == None) and (self.get_target_srs() != None)) or ((self.get_target_resolution() != None) and (self.get_target_srs() == None)):
            # It's important to supply either both tr and t_srs or nothing as in cases when only one is supplied system can converts the resolution in wrong units
            logger.warning("either supply both -tr and -t_srs or supply nothing")

        nodataval = (
            f"-srcnodata {band_tile.nodataval}" if band_tile.nodataval != None else self.get_srcnodata()
        )
        t_srs = self.get_target_srs() or "-t_srs EPSG:3857"
        tr = (
            self.get_target_resolution()
            or f"-tr {self.__to_meter__(band_tile.tile.get_res()[0], band_tile.tile.length_unit)} {self.__to_meter__(band_tile.tile.get_res()[1], band_tile.tile.length_unit)}"
        )

        if nodataval == None:
            logger.warning("no data val set as None. it's advised to provide a nodataval")

        # Creating warped vrt for every tile extracting the correct band required and in the correct order
        build_warped_vrt_cmd = f"gdalwarp --quiet -te {self.space_opts["bbox"][0]} {self.space_opts["bbox"][1]} {self.space_opts["bbox"][2]} {self.space_opts["bbox"][3]} -te_srs 'EPSG:4326' {tr} {t_srs} {nodataval or ''} -srcband {band_tile.idx} -dstband 1 -et 0 -tap -of VRT -overwrite {band_tile.tile.gdal_path} {warped_vrt_path}"

        os.system(build_warped_vrt_cmd)
        return warped_vrt_path

    def __create_band_mosaic__(self, band_tiles, date, bands):
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        band_mosaics = []
        for idx in range(len(bands)):
            current_bands_df = band_tiles[band_tiles["description"] == bands[idx]]
            band_mosaic_path = (
                f"{self.get_ds_tmp_path()}/pre-processing/{date_str}-{bands[idx]}.vrt"
            )
            band_mosaic_file_list = (
                f"{self.get_ds_tmp_path()}/pre-processing/{date_str}-{bands[idx]}.txt"
            )

            current_bands_df[["vrt_path"]].to_csv(
                band_mosaic_file_list, index=False, header=False
            )
            buildvrt_cmd = f"gdalbuildvrt --quiet -overwrite -input_file_list {band_mosaic_file_list} {band_mosaic_path}"

            os.system(buildvrt_cmd)

            band_mosaics.append(band_mosaic_path)
        return band_mosaics

    def __stack_band_mosaics__(self, band_mosaics, date):
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        output_vrt = f"{self.get_ds_tmp_path()}/pre-processing/{date_str}.vrt"
        output_vrt_file_list = f"{self.get_ds_tmp_path()}/pre-processing/{date_str}.txt"
        pd.DataFrame(band_mosaics, columns=["band_mosaic_path"]).to_csv(
            output_vrt_file_list, index=False, header=False
        )
        build_mosaiced_stacked_vrt_cmd = f"gdalbuildvrt -separate {output_vrt} -input_file_list {output_vrt_file_list}"
        os.system(build_mosaiced_stacked_vrt_cmd)

        return output_vrt

    def to_vrts(self, bands):
        """
        # TODO: Update docstrings
        Stitches the scene files together according to the band arrangement provided by the user.
        Internally uses gdalwarp. Currently supported gdalwarp options are

        * t_srs - Set target spatial reference.
        * tr - Set output file resolution (in target georeferenced units).
        * r - Resampling method

        Read more about supported options: `gdalwarp <https://gdal.org/programs/gdalwarp.html>`_.

        Args:
            bands (list[string]): Ordered list of bands to output in COGs
            gdal_options (dict, optional): GDAL options to pass during stitching. Defaults to {}. Currently supported options are ``-t_srs``, ``-tr``, ``-r``. Example ``{"t_srs": "EPSG:3857", "tr": 30, "r": "nearest"}``

        Returns:
            pd.DataFrameGroupBy[tuple]: Dataframe with output vrt path, output hash and tuple of all the tile files to be combined
            list[string]: Final mosaiced and stacked vrt paths
        """

        self.output_vrts = []

        # Making sure pre-processing directory exists
        helpers.make_sure_dir_exists(f"{self.get_ds_tmp_path()}/pre-processing")

        # bands_df contains all the bands of all the tiles along with dates
        tile_bands = self.__get_tile_bands__()
        df = pd.DataFrame(tile_bands)
        df["date"] = df.apply(lambda x: x.tile.date, axis=1)

        # Now we filter bands based on user supplied bands
        df = df[df["description"].isin(bands)]

        outputs_by_dates = df.groupby(by=["date"])

        # Then we iterate over every output, create vrt and gti index for that output
        for date, band_tiles in outputs_by_dates:
            # TODO: Add multiprocessing here to add give some performance boost
            curr_date = date[0]

            _band_tiles = band_tiles.copy(deep=True).reset_index(drop=True)
            # First we extract all the required bands using a vrt which will be in that output
            for _bt in _band_tiles.itertuples():
                # vrt gets warped to 3857 to bring all tiles to a consistent resolution
                vrt_path = self.__extract_band__(_bt)
                _band_tiles.at[_bt.Index, "vrt_path"] = vrt_path

            # At this stage we have single band vrts, now we combine the same bands together create one single vrt
            # Ideally we want to do GDAL Raster Tile Index but it wasn't copying metadata properly when creating outputs. Eg: ColorInterp. GDAL Raster Tile is preferred as number of tiles can be a lot more than number of bands.
            # We also store all the band level mosaics we are creating to be later stacked together in a vrt
            band_mosaics = self.__create_band_mosaic__(_band_tiles, curr_date, bands)

            # Then we line up the bands in a single vrt file, this is stacking bands on top of each other
            output_vrt = self.__stack_band_mosaics__(band_mosaics, curr_date)

            # Setting output band descriptions
            geo.set_band_descriptions(output_vrt, bands)
