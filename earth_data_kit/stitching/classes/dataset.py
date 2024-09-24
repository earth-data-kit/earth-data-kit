from sys import executable
import pandas as pd
import ast
import geopandas as gpd
import logging
import re
import rioxarray as rio
import xarray as xr
import copy
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
import fiona
import Levenshtein as levenshtein
import json

fiona.drvsupport.supported_drivers["kml"] = "rw"
fiona.drvsupport.supported_drivers["KML"] = "rw"


logger = logging.getLogger(__name__)


class DataSet:
    """
    The main class implemented by stitching module. Acts as a wrapper above a single remote dataset
    """

    def __init__(self, id, source, engine, clean=False) -> None:
        """
        Args:
            id (string): User provided unique string
            source (string): Source path, Read more :ref:`Defining source`
            engine (string): Remote datasource engine, accepted values - ``s3``
            clean (bool, optional): Whether to clean the tmp directory before stitching. Defaults to False.
        """
        if engine not in constants.engines_supported:
            raise Exception(f"{engine} not supported")

        self.id = id
        if engine == "s3":
            self.engine = s3.S3()
        if engine == "earth_engine":
            self.engine = earth_engine.EarthEngine()
        self.source = source
        self.patterns = []
        self.tiles = []
        self.complete_inventory = f"{self.get_ds_tmp_path()}/complete-inventory.csv"
        self.filtered_inventory = f"{self.get_ds_tmp_path()}/filtered-inventory.csv"
        self.local_inventory = f"{self.get_ds_tmp_path()}/local-inventory.csv"
        if clean:
            helpers.delete_dir(f"{self.get_ds_tmp_path()}")

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

    @decorators.log_time
    @decorators.log_init
    def find_tiles(self):
        df = self.engine.create_inventory(
            self.source, self.time_opts, self.space_opts, self.get_ds_tmp_path()
        )

        futures = []
        tiles = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            for row in df.itertuples():
                t = tile.Tile(row.engine_path, row.gdal_path)
                tiles.append(t)
                futures.append(executor.submit(t.get_metadata))

            for idx in range(len(futures)):
                future = futures[idx]
                result = future.result()
                tiles[idx].set_metadata(result)

            df = tile.Tile.to_df(tiles)
            df.to_csv(f"{self.complete_inventory}", header=True, index=False)

    @decorators.log_time
    @decorators.log_init
    def filter_tiles(self):
        df = pd.read_csv(f"{self.complete_inventory}")
        # Not filtering for time dimension as we are already pin pointing files

        # Filter spatially
        # * Below function assumes the projection is gonna be same which can be
        # * usually true for a single set of tiles
        # Getting the projection from first row
        projection = df["projection"].iloc[0]
        # Add the extent geo-series and set projection
        extent = gpd.GeoSeries(
            df.apply(helpers.polygonise_2Dcells, axis=1),
            crs=pyproj.CRS.from_user_input(projection),
        )
        reprojected_extent = extent.to_crs(epsg=4326)

        # Get polygon from user's bbox
        bbox = shapely.geometry.box(*self.space_opts["bbox"], ccw=True)

        # Perform intersection and filtering
        intersects = reprojected_extent.intersects(bbox)
        df = df[intersects == True]

        filtered_inventory = f"{self.filtered_inventory}"
        df.to_csv(filtered_inventory, index=False, header=True)

    @decorators.log_time
    @decorators.log_init
    def get_distinct_bands(self):
        """
        Reads the metadata from scene files and extract distinct bands available.

        Returns:
            pd.DataFrame: Dataframe with band indexes, name and datatype
        """
        self.find_tiles()
        self.filter_tiles()
        bands_df = self.get_all_bands()
        by = ["band_idx", "description", "dtype"]
        distinct_bands = bands_df.groupby(by=by).size().reset_index()
        return distinct_bands[by]

    def get_all_bands(self):
        df = pd.read_csv(self.filtered_inventory)
        bands_df = pd.DataFrame()

        for df_row in df.itertuples():
            tile_bands = ast.literal_eval(df_row.bands)
            tile_bands_df = pd.DataFrame(tile_bands)
            tile_bands_df["engine_path"] = df_row.engine_path
            bands_df = pd.concat([bands_df, tile_bands_df])

        bands_df.reset_index(drop=True, inplace=True)

        # Creates the date_range patterns again for matching purposes
        dates = pd.date_range(
            start=self.time_opts["start"], end=self.time_opts["end"], inclusive="both"
        )
        o_df = pd.DataFrame()
        o_df["date"] = dates
        o_df["source_pattern"] = o_df["date"].dt.strftime(self.source)

        # Joining using string matching so that every tile has a date associated with it
        for in_row in bands_df.itertuples():
            max_score = -99
            max_score_idx = -1
            for out_row in o_df.itertuples():
                s = levenshtein.ratio(in_row.engine_path, out_row.source_pattern)
                if s > max_score:
                    max_score = s
                    max_score_idx = out_row.Index

            bands_df.at[in_row.Index, "date"] = o_df["date"][max_score_idx]

        return bands_df

    @decorators.log_time
    @decorators.log_init
    def sync(self):
        """
        Downloads the relevant scene files, based on temporal and spatial bounds provided by ``set_timebounds`` and ``set_spacebounds`` methods
        """
        # Reading the filtered inventory
        df = pd.read_csv(self.filtered_inventory)
        # Syncing files to local
        df = self.engine.sync_inventory(df, self.get_ds_tmp_path())
        df.to_csv(f"{self.local_inventory}", index=False, header=True)

    def get_ds_tmp_path(self):
        path = f"{helpers.get_tmp_dir()}/{self.id}"
        helpers.make_sure_dir_exists(path)
        return path

    def extract_band(self, tile):
        vrt_path = f"{self.get_ds_tmp_path()}/pre-processing/{'.'.join(tile.local_path.split('/')[-1].split('.')[:-1])}-band-{tile.band_idx}.vrt"
        # Creating vrt for every tile extracting the correct band required and in the correct order
        buildvrt_cmd = f"gdalbuildvrt -b {tile.band_idx} {vrt_path} {tile.local_path}"
        os.system(buildvrt_cmd)
        return vrt_path

    def create_band_mosaic(self, tiles, date, bands):
        date_str = date.strftime("%d-%b-%Y")
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

            buildgti_cmd = f"gdaltindex -f FlatgeoBuf {band_mosaic_index_path} -gti_filename {band_mosaic_path} -lyr_name {bands[idx]} -write_absolute_path -overwrite --optfile {band_mosaic_file_list}"

            os.system(buildgti_cmd)

            band_mosaics.append(band_mosaic_path)
        return band_mosaics

    def stack_band_mosaics(self, band_mosaics, date):
        date_str = date.strftime("%d-%b-%Y")
        output_vrt = f"{self.get_ds_tmp_path()}/pre-processing/{date_str}.vrt"
        output_vrt_file_list = f"{self.get_ds_tmp_path()}/pre-processing/{date_str}.txt"
        pd.DataFrame(band_mosaics, columns=["band_mosaic_path"]).to_csv(
            output_vrt_file_list, index=False, header=False
        )

        build_mosaiced_stacked_vrt_cmd = f"gdalbuildvrt -separate {output_vrt} -input_file_list {output_vrt_file_list}"
        os.system(build_mosaiced_stacked_vrt_cmd)

        return output_vrt

    def get_gdal_option(self, opts, opt):
        if opt in constants.gdalwarp_opts_supported and opt in opts:
            return opts[opt]
        return None

    @decorators.log_time
    @decorators.log_init
    def convert_vrt(self, src, dest, of, gdal_options={}):
        """Important options
        -te <xmin> <ymin> <xmax> <ymax> - Get from bounding box. Not given to user
        -te_srs <srs_def> - Specifies the SRS in which to interpret the coordinates given with -te - EPSG:4326
        -t_srs <srs_def> - Target SRS
        -tr <xres> <yres> | -tr square - Set output file resolution (in target georeferenced units)
        -r <resampling_method>
        """
        helpers.make_sure_dir_exists("/".join(dest.split("/")[:-1]))
        # self.bbox = left, bottom, right, top
        te = self.space_opts["bbox"]
        te_srs = "EPSG:4326"
        t_srs = self.get_gdal_option(gdal_options, "t_srs")
        tr = self.get_gdal_option(gdal_options, "tr")
        r = self.get_gdal_option(gdal_options, "r")
        # -t_srs {srs_def}  {src} {dest}
        convert_cmd = f"gdalwarp -of {of} -te {te[0]} {te[1]} {te[2]} {te[3]} -te_srs {te_srs} -overwrite"

        if t_srs:
            convert_cmd = f"{convert_cmd} -t_srs {t_srs}"

        if tr:
            convert_cmd = f"{convert_cmd} -tr {tr}"

        if r:
            convert_cmd = f"{convert_cmd} -r {r}"

        convert_cmd = f"{convert_cmd} {src} {dest}"

        os.system(convert_cmd)

    def to_vrts(self, bands):
        """
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
        # Making sure pre-processing directory exists
        helpers.make_sure_dir_exists(f"{self.get_ds_tmp_path()}/pre-processing")

        # df contains all the tiles along with local paths
        df = pd.read_csv(self.local_inventory)

        # bands_df contains all the bands of all the tiles along with dates
        bands_df = self.get_all_bands()

        # We add local_path to bands_df. Also len(bands_df) >= len(df) so number of rows after the below statement will remain same
        bands_df = bands_df.merge(
            df[["engine_path", "local_path"]],
            how="left",
            left_on="engine_path",
            right_on="engine_path",
        )

        # Now we filter bands based on user supplied bands
        bands_df = bands_df[bands_df["description"].isin(bands)]

        outputs_by_dates = bands_df.groupby(by=["date"])
        output_vrts = []
        # Then we iterate over every output, create vrt and gti index for that output
        for date, tiles in outputs_by_dates:
            curr_date = date[0]
            # First we extract all the required bands using a vrt which will be in that op
            for tile in tiles.itertuples():
                vrt_path = self.extract_band(tile)
                tiles.at[tile.Index, "vrt_path"] = vrt_path

            # At this stage we have single band vrts, now we combine the same bands together create one gti per mosaic.
            # GDAL Raster Tile is done as number of tiles can be a lot more than number of bands
            # We also store all the band level mosaics we are creating to be later stacked together in a vrt
            band_mosaics = self.create_band_mosaic(tiles, curr_date, bands)

            # Then we line up the bands in a single vrt file, this is stacking
            output_vrt = self.stack_band_mosaics(band_mosaics, curr_date)

            # Setting output band descriptions
            geo.set_band_descriptions(output_vrt, bands)

            output_vrts.append(output_vrt)

        return outputs_by_dates, output_vrts

    @decorators.log_time
    @decorators.log_init
    def to_cog(self, destination, bands, gdal_options={}):
        """
        Calls self.to_vrts to create stitched and stacked output files (vrts) and then converts them to COG

        Args:
            destination (string): Output path for generated files
            bands (list[string]): Ordered list of bands to output in COGs
            gdal_options (dict, optional): GDAL options to pass during stitching. Defaults to {}. Currently supported options are ``-t_srs``, ``-tr``, ``-r``. Example ``{"t_srs": "EPSG:3857", "tr": 30, "r": "nearest"}``
        """
        outputs_by_dates, output_vrts = self.to_vrts(bands)
        idx = 0

        dest_cogs = []
        for date, _ in outputs_by_dates:
            curr_date = date[0]
            dest_cogs.append(curr_date.strftime(destination))

        if len(list(set(dest_cogs))) != len(output_vrts):
            raise Exception("Temporal frequency mismatch")

        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=helpers.get_processpool_workers()
        )

        for idx in range(len(dest_cogs)):
            src = output_vrts[idx]
            dest = dest_cogs[idx]

            executor.submit(self.convert_vrt, src, dest, "COG", gdal_options)

        executor.shutdown(wait=True)

    @decorators.log_time
    @decorators.log_init
    def to_zarr(self, destination, bands, gdal_options={}):
        """
        Calls self.to_vrts to create stitched and stacked output files (vrts) and converts them to Zarrs.
        First vrts are converted to COGs, then read and combined using rioxarray and xarray.

        Args:
            destination (string): Output path for generated files
            bands (list[string]): Ordered list of bands to output in band dimension
            gdal_options (dict, optional): GDAL options to pass during stitching. Defaults to {}. Currently supported options are ``-t_srs``, ``-tr``, ``-r``. Example ``{"t_srs": "EPSG:3857", "tr": 30, "r": "nearest"}``
        """
        outputs_by_dates, output_vrts = self.to_vrts(bands)

        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=helpers.get_processpool_workers()
        )
        date_wise_cogs = []

        for idx in range(len(output_vrts)):
            src = output_vrts[idx]
            dest = src.replace(".vrt", ".tif")
            date_wise_cogs.append(dest)
            executor.submit(self.convert_vrt, src, dest, "COG", gdal_options)

        executor.shutdown(wait=True)

        date_index = []
        for date, _ in outputs_by_dates:
            curr_date = date[0]
            date_index.append(curr_date)

        das = []
        for idx in range(len(date_wise_cogs)):
            da = rio.open_rasterio(f"{date_wise_cogs[idx]}")
            das.append(da)

        # Concat all date wise data-arrays
        da = xr.concat(das, pd.DatetimeIndex(date_index, name="time"))

        # Converting data array to data set
        ds = da.to_dataset(name=self.id, promote_attrs=True)

        # Finally converting to zarr
        # * Writes the dataset, meaning user will have to do ds[self.id] to get the data array
        ds.to_zarr(destination, mode="w")
