from sys import executable
import pandas as pd
import ast
import geopandas as gpd
import logging
import re
import copy
import os
from shapely.geometry.polygon import orient
from earth_data_kit.stitching import geo, helpers
import earth_data_kit.stitching.constants as constants
import earth_data_kit.stitching.decorators as decorators
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
    def __init__(self, id, source, engine, clean=False) -> None:
        if engine not in constants.engines_supported:
            raise Exception(f"{engine} not supported")

        self.id = id
        if engine == "s3":
            self.engine = s3.S3()
        self.source = source
        self.patterns = []
        self.tiles = []
        self.complete_inventory = f"{self.get_ds_tmp_path()}/complete-inventory.csv"
        self.filtered_inventory = f"{self.get_ds_tmp_path()}/filtered-inventory.csv"
        self.local_inventory = f"{self.get_ds_tmp_path()}/local-inventory.csv"
        if clean:
            helpers.delete_dir(f"{self.get_ds_tmp_path()}")

    def set_timebounds(self, start, end):
        self.start = start
        self.end = end

        self.patterns = self.patterns + list(
            set(
                pd.date_range(start=start, end=end, inclusive="both").strftime(
                    self.source
                )
            )
        )

    def set_spacebounds(self, bbox, grid_file=None, matcher=None):
        self.bbox = bbox
        if not grid_file:
            # Doing nothing if grid_file is not passed
            return 1

        if grid_file.endswith(".kml") or grid_file.endswith(".KML"):
            grid_df = gpd.read_file(grid_file, driver="kml", bbox=bbox)
            space_vars = []
            for grid in grid_df.itertuples():
                space_vars.append(matcher(grid))

            patterns = self.patterns

            new_patterns = []
            for p in patterns:
                matches = re.findall(r"({.[^}]*})", p)
                # Now we replace matches and with all space_variables
                for var in space_vars:
                    tmp_p = copy.copy(p)
                    for m in matches:
                        tmp_p = tmp_p.replace(
                            m, var[m.replace("{", "").replace("}", "")]
                        )
                    new_patterns.append(tmp_p)

            self.patterns = new_patterns
        else:
            raise Exception("drivers other than kml are not supported")

    @decorators.log_time
    @decorators.log_init
    def find_tiles(self):
        df = self.engine.create_inventory(self.patterns, self.get_ds_tmp_path())

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

        if self.start and self.end:
            # Not filtering based on date_range as we are already pin-pointing the files to download
            pass

        if self.bbox:
            # * Below function assumes the projection is gonna be same which can be
            # * usually true for a single set of tiles
            # Filter spatially

            # Getting the projection from first row
            projection = df["projection"].iloc[0]
            # Add the extent geo-series and set projection
            extent = gpd.GeoSeries(
                df.apply(helpers.polygonise_2Dcells, axis=1),
                crs=pyproj.CRS.from_user_input(projection),
            )
            reprojected_extent = extent.to_crs(epsg=4326)

            # Get polygon from user's bbox
            bbox = shapely.geometry.box(*self.bbox, ccw=True)

            # Perform intersection and filtering
            intersects = reprojected_extent.intersects(bbox)
            df = df[intersects == True]

        filtered_inventory = f"{self.filtered_inventory}"
        df.to_csv(filtered_inventory, index=False, header=True)

    @decorators.log_time
    @decorators.log_init
    def sync(self):
        # Reading the filtered inventory
        df = pd.read_csv(self.filtered_inventory)

        # Syncing files to local
        df = self.engine.sync_inventory(df, self.get_ds_tmp_path())
        df.to_csv(f"{self.local_inventory}", index=False, header=True)

    def get_ds_tmp_path(self):
        path = f"{helpers.get_tmp_dir()}/{self.id}"
        helpers.make_sure_dir_exists(path)
        return path

    @decorators.log_time
    @decorators.log_init
    def get_distinct_bands(self):
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
        dates = pd.date_range(start=self.start, end=self.end, inclusive="both")
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

    def extract_band(self, tile):
        vrt_path = f"{self.get_ds_tmp_path()}/pre-processing/{'.'.join(tile.local_path.split('/')[-1].split('.')[:-1])}-band-{tile.band_idx}.vrt"
        # Creating vrt for every tile extracting the correct band required and in the correct order
        buildvrt_cmd = f"gdalbuildvrt -b {tile.band_idx} {vrt_path} {tile.local_path}"
        os.system(buildvrt_cmd)
        return vrt_path

    def create_band_mosaic(self, op, op_hash, bands):
        band_mosaics = []
        for idx in range(len(bands)):
            current_bands_df = op[op["description"] == bands[idx]]
            band_mosaic_path = (
                f"{self.get_ds_tmp_path()}/pre-processing/{op_hash}-{bands[idx]}.gti"
            )
            band_mosaic_index_path = (
                f"{self.get_ds_tmp_path()}/pre-processing/{op_hash}-{bands[idx]}.fgb"
            )
            band_mosaic_file_list = (
                f"{self.get_ds_tmp_path()}/pre-processing/{op_hash}-{bands[idx]}.txt"
            )

            current_bands_df[["vrt_path"]].to_csv(
                band_mosaic_file_list, index=False, header=False
            )

            buildgti_cmd = f"gdaltindex -f FlatgeoBuf {band_mosaic_index_path} -gti_filename {band_mosaic_path} -lyr_name {bands[idx]} -write_absolute_path -overwrite --optfile {band_mosaic_file_list}"

            os.system(buildgti_cmd)

            band_mosaics.append(band_mosaic_path)
        return band_mosaics

    def stack_band_mosaics(self, band_mosaics, op_hash):
        output_vrt = f"{self.get_ds_tmp_path()}/pre-processing/{op_hash}.vrt"
        output_vrt_file_list = f"{self.get_ds_tmp_path()}/pre-processing/{op_hash}.txt"
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
    def convert_to_cog(self, src, dest, gdal_options={}):
        """Important options
            -te <xmin> <ymin> <xmax> <ymax> - Get from bounding box. Not given to user
            -te_srs <srs_def> - Specifies the SRS in which to interpret the coordinates given with -te - EPSG:4326
            -t_srs <srs_def> - Target SRS
            -tr <xres> <yres> | -tr square - Set output file resolution (in target georeferenced units)
            -r <resampling_method>
        """
        helpers.make_sure_dir_exists("/".join(dest.split("/")[:-1]))
        # self.bbox = left, bottom, right, top
        te = self.bbox
        te_srs = "EPSG:4326"
        t_srs = self.get_gdal_option(gdal_options, "t_srs")
        tr = self.get_gdal_option(gdal_options, "tr")
        r = self.get_gdal_option(gdal_options, "r")
        # -t_srs {srs_def}  {src} {dest}
        convert_to_cog_cmd = f"gdalwarp -of COG -te {te[0]} {te[1]} {te[2]} {te[3]} -te_srs {te_srs} -overwrite"
        
        if t_srs:
            convert_to_cog_cmd = f"{convert_to_cog_cmd} -t_srs {t_srs}"

        if tr:
            convert_to_cog_cmd = f"{convert_to_cog_cmd} -tr {tr}"

        if r:
            convert_to_cog_cmd = f"{convert_to_cog_cmd} -r {r}"


        convert_to_cog_cmd = f"{convert_to_cog_cmd} {src} {dest}"
        os.system(convert_to_cog_cmd)

    @decorators.log_time
    @decorators.log_init
    def to_cog(self, destination, bands, gdal_options={}, **kwargs):
        # Making sure pre-processing directory exists and is empty
        helpers.delete_dir(f"{self.get_ds_tmp_path()}/pre-processing")
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

        # Then we add output file paths to bands_df
        bands_df["output_path"] = pd.to_datetime(bands_df["date"]).dt.strftime(
            destination
        )

        bands_df["output_hash"] = bands_df.apply(
            lambda df_row: helpers.cheap_hash(df_row.output_path), axis=1
        )

        # Then we group by output_path as these are ideally the unique output files created
        # We also group by output_hash so that we can have unique names for intermediary vrt/gti files per output file easily
        outputs = bands_df.groupby(by=["output_hash", "output_path"])

        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=helpers.get_processpool_workers()
        )

        # Then we iterate over every output, create vrt and gti index for that output
        for grp_key, op in outputs:
            op_hash = grp_key[0]
            op_path = grp_key[1]

            # First we extract all the required bands using a vrt which will be in that op
            for tile in op.itertuples():
                vrt_path = self.extract_band(tile)
                op.at[tile.Index, "vrt_path"] = vrt_path

            # At this stage we have single band vrts, now we combine the same bands together create one gti per mosaic. GDAL Raster Tile is done as number of tiles can be a lot more than number of bands
            # We also store all the band level mosaics we are creating to be later stacked together in a vrt
            band_mosaics = self.create_band_mosaic(op, op_hash, bands)

            # Then we line up the bands in a single vrt file, this is stacking
            output_vrt = self.stack_band_mosaics(band_mosaics, op_hash)

            # Setting output band descriptions
            geo.set_band_descriptions(output_vrt, bands)

            # Then we create the output COGs
            executor.submit(self.convert_to_cog, output_vrt, op_path, gdal_options)

        executor.shutdown(wait=True)
