import pandas as pd
import ast
import geopandas as gpd
import logging
from osgeo import osr
import os
from shapely.geometry.polygon import orient
from earth_data_kit.stitching import geo, helpers
import earth_data_kit.stitching.constants as constants
import earth_data_kit.stitching.decorators as decorators
import earth_data_kit.stitching.engines.earth_engine as earth_engine
import earth_data_kit.stitching.engines.s3 as s3
import concurrent.futures
import earth_data_kit.stitching.classes.tile as tile
import shapely.geometry
import pyproj
import numpy as np
import fiona

fiona.drvsupport.supported_drivers["kml"] = "rw"  # type: ignore
fiona.drvsupport.supported_drivers["KML"] = "rw"  # type: ignore


logger = logging.getLogger(__name__)


class Dataset:
    """
    The main class implemented by stitching module. Acts as a wrapper above a single remote dataset
    """

    def __init__(self, name, source, engine, clean=False) -> None:
        """
        Args:
            name (string): User provided name, should be unique across datasets being opened
            source (string): Source path, Read more :ref:`Defining source`
            engine (string): Remote datasource engine, accepted values - ``s3``
            clean (bool, optional): Whether to clean the tmp directory before stitching. Defaults to False.
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
        Sets time bounds for which we want to download the data for.

        Args:
            start (datetime): Start data
            end (datetime): End date, inclusive
        """
        self.time_opts = {
            "start": start,
            "end": end,
        }

    def set_spacebounds(self, bbox, grid_file=None, matcher=None):
        """
        Sets spatial bounds using a bbox provided.
        Optionally you can also provide a grid file and matching function which can then be used to pinpoint the
        exact scene files to download.

        Read more on :ref:`Using a grid file`

        Args:
            bbox (tuple[float, float, float, float]): Bounding box as a set of four coordinates in EPSG:4326
            grid_file (string, optional): File path to grid file, currently only kml files are supported. Defaults to None.
            matcher (function, optional): Lambda function to extract spatial parts for scene filepaths. Defaults to None.
        """
        self.space_opts = {
            "grid_file": grid_file,
            "matcher": matcher,
        }
        self.space_opts["bbox"] = bbox

    def set_gdal_options(self, options):
        self.gdal_options = options

    def get_target_resolution(self):
        for opt in self.gdal_options:
            if opt.startswith("-tr"):
                return opt

    def get_target_srs(self):
        for opt in self.gdal_options:
            if opt.startswith("-t_srs"):
                return opt

    def get_srcnodata(self):
        for opt in self.gdal_options:
            if opt.startswith("-srcnodata"):
                return opt

    @decorators.log_time
    @decorators.log_init
    def discover(self):
        # TODO: Add docstring
        # Calling the .scan method of engine to list all the tiles
        df = self.engine.scan(
            self.source, self.time_opts, self.space_opts, self.get_ds_tmp_path()
        )

        # Fetching metadata of all files
        futures = []
        tiles = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            for row in df.itertuples():
                t = tile.Tile(row.engine_path, row.gdal_path, row.date, row.tile_name)
                tiles.append(t)
                futures.append(executor.submit(t.get_metadata))

        for idx in range(len(futures)):
            future = futures[idx]
            result = future.result()
            tiles[idx].set_metadata(result)

        df = tile.Tile.to_df(tiles)

        # Filtering spatially. Doing this by re-projecting raster extent to 4326 and running intersects query with bbox
        # This is done as raster can be in different coordinates (eg: UTMs) and much easier to convert multiple to 4326,
        # rather than 4326 to different multiple EPSGs (UTM zones)
        # Add the extent geo-series and set projection
        extent = gpd.GeoSeries(
            df.apply(helpers.polygonise_2Dcells, axis=1),  # type: ignore
            crs="EPSG:4326",
        )

        # Get polygon from user's bbox
        bbox = shapely.geometry.box(*self.space_opts["bbox"], ccw=True)  # type: ignore

        # Perform intersection and filtering
        intersects = extent.intersects(bbox)
        df = df[intersects == True]

        catalog_path = f"{self.catalog_path}"
        df.to_csv(catalog_path, index=False, header=True)

        bands_df = self.__get_all_bands()
        bands_df["crs"] = bands_df.apply(
            lambda x: "EPSG:"
            + osr.SpatialReference(x.projection).GetAttrValue("AUTHORITY", 1),
            axis=1,
        )

        by = ["band_idx", "description", "dtype", "x_res", "y_res", "crs"]
        bands_df["x_res"] = bands_df["x_res"].astype(np.float32)
        bands_df["y_res"] = bands_df["y_res"].astype(np.float32)
        distinct_bands = bands_df.groupby(by=by).size().reset_index()

        self.bands = distinct_bands[
            ["band_idx", "description", "dtype", "x_res", "y_res", "crs"]
        ]

    def __get_all_bands(self):
        df = pd.read_csv(self.catalog_path)

        bands_df = pd.DataFrame()

        for df_row in df.itertuples():
            _df = pd.DataFrame(ast.literal_eval(df_row.bands))  # type: ignore
            _df["tile_index"] = df_row.Index
            bands_df = pd.concat([_df, bands_df], axis=0)

        bands_df = bands_df.merge(
            df[
                [
                    "engine_path",
                    "gdal_path",
                    "date",
                    "tile_name",
                    "geo_transform",
                    "x_min",
                    "x_max",
                    "y_min",
                    "y_max",
                    "x_res",
                    "y_res",
                    "projection",
                    "length_unit",
                ]
            ],
            left_on="tile_index",
            right_index=True,
        )
        bands_df.drop(columns=["tile_index"], inplace=True)
        bands_df["date"] = pd.to_datetime(bands_df["date"])
        return bands_df

    def get_ds_tmp_path(self):
        path = f"{helpers.get_tmp_dir()}/{self.name}"
        helpers.make_sure_dir_exists(path)
        return path

    def __convert_length_to_meter(self, val, unit):
        if (unit == "metre") or (unit == "meter"):
            return val
        elif unit == "degree":
            # TODO: Add degree to meter conversion
            return val

    def extract_band(self, tile):
        warped_vrt_path = f"{self.get_ds_tmp_path()}/pre-processing/{tile.tile_name}-band-{tile.band_idx}-warped.vrt"
        
        # Creating warped vrt for every tile extracting the correct band required and in the correct order
        build_warped_vrt_cmd = f"gdalwarp -tr {self.__convert_length_to_meter(tile.x_res, tile.length_unit)} {self.__convert_length_to_meter(tile.y_res, tile.length_unit)} -t_srs EPSG:3857 -srcnodata '0' -srcband {tile.band_idx} -dstband 1 -et 0 -of VRT -overwrite {tile.gdal_path} {warped_vrt_path}"
        
        os.system(build_warped_vrt_cmd)
        return warped_vrt_path

    def create_band_mosaic(self, tiles, date, bands):
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        band_mosaics = []
        for idx in range(len(bands)):
            current_bands_df = tiles[tiles["description"] == bands[idx]]
            band_mosaic_path = (
                f"{self.get_ds_tmp_path()}/pre-processing/{date_str}-{bands[idx]}.gti"
            )
            band_mosaic_index_path = (
                f"{self.get_ds_tmp_path()}/pre-processing/{date_str}-{bands[idx]}.fgb"
            )
            band_mosaic_file_list = (
                f"{self.get_ds_tmp_path()}/pre-processing/{date_str}-{bands[idx]}.txt"
            )

            current_bands_df[["vrt_path"]].to_csv(
                band_mosaic_file_list, index=False, header=False
            )
            buildgti_cmd = f"gdaltindex -f FlatgeoBuf {band_mosaic_index_path} -gti_filename {band_mosaic_path} -lyr_name {bands[idx]} -nodata 0 -write_absolute_path -overwrite --optfile {band_mosaic_file_list}"

            os.system(buildgti_cmd)

            band_mosaics.append(band_mosaic_path)
        return band_mosaics

    def stack_band_mosaics(self, band_mosaics, date):
        date_str = date.strftime("%Y-%m-%d-%H:%M:%S")
        output_vrt = f"{self.get_ds_tmp_path()}/pre-processing/{date_str}.vrt"
        output_vrt_file_list = f"{self.get_ds_tmp_path()}/pre-processing/{date_str}.txt"
        pd.DataFrame(band_mosaics, columns=["band_mosaic_path"]).to_csv(
            output_vrt_file_list, index=False, header=False
        )
        # TODO: We need to set nodata values
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
        bands_df = self.__get_all_bands()

        # Now we filter bands based on user supplied bands
        bands_df = bands_df[bands_df["description"].isin(bands)]

        outputs_by_dates = bands_df.groupby(by=["date"])

        # Then we iterate over every output, create vrt and gti index for that output
        for date, tiles in outputs_by_dates:
            # TODO: Add multiprocessing here to add give some performance boost
            curr_date = date[0]
            # First we extract all the required bands using a vrt which will be in that output
            _tiles = tiles.copy(deep=True).reset_index(drop=True)
            for tile in _tiles.itertuples():
                vrt_path = self.extract_band(tile)
                # vrt gets warped to 3857 to bring all tiles to a consistent resolution
                # TODO: Later on we can make user pass this information to to_vrts and use it
                _tiles.at[tile.Index, "vrt_path"] = vrt_path

            # At this stage we have single band vrts, now we combine the same bands together create one gti per mosaic.
            # GDAL Raster Tile is done as number of tiles can be a lot more than number of bands
            # We also store all the band level mosaics we are creating to be later stacked together in a vrt
            band_mosaics = self.create_band_mosaic(_tiles, curr_date, bands)

            # Then we line up the bands in a single vrt file, this is stacking bands on top of each other
            output_vrt = self.stack_band_mosaics(band_mosaics, curr_date)

            # Setting output band descriptions
            geo.set_band_descriptions(output_vrt, bands)

            self.output_vrts.append(output_vrt)
