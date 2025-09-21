import xarray as xr
import logging
import earth_data_kit.stitching.decorators as decorators
from earth_data_kit.xarray_boosted.plotters.folium import Folium
from osgeo import gdal
import earth_data_kit.utilities.helpers as helpers
import earth_data_kit.xarray_boosted.commons as commons
import pandas as pd
import os
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_fixed

from tqdm import tqdm
import math
from datetime import datetime
import glob
import json
import uuid
import numpy as np
import earth_data_kit.xarray_boosted.io as io

logger = logging.getLogger(__name__)


@xr.register_dataarray_accessor("edk")
class EDKAccessor:
    """
    The EDK Accessor extends xarray's functionality by providing additional methods for working with Earth Data Kit datasets.
    This accessor is automatically registered with xarray when edk is imported, allowing you to access these extended capabilities through the `.edk` namespace on xarray objects.
    """

    def __init__(self, xarray_obj):
        self.da = xarray_obj

    def _create_template_cog(self, da, output_path):
        driver = gdal.GetDriverByName("GTiff")

        # Get basic info like size, num_bands, dtype
        num_bands, width, height = da.shape
        gdal_dtype = commons.get_gdal_dtype(da.dtype)

        # Get chunk sizes for each dimension
        chunk_sizes = {}
        if da.chunksizes:
            for dim_name, chunks in da.chunksizes.items():
                chunk_sizes[dim_name] = chunks[0] if chunks else None
        else:
            raise ValueError(
                "No chunk sizes found. Chunking must be enabled as block size depends on chunk size."
            )

        # Ensure chunk sizes are multiples of 128
        # This is important for optimal performance with COGs
        for dim in ["x", "y"]:
            if dim in chunk_sizes and chunk_sizes[dim] is not None:
                # Ceiling to nearest multiple of 128 using math.ceil
                chunk_sizes[dim] = int(math.ceil(chunk_sizes[dim] / 128) * 128)
                # Ensure chunk size is at least 128
                chunk_sizes[dim] = max(128, chunk_sizes[dim])

        # Create the dataset
        ds = driver.Create(
            output_path,
            width,
            height,
            num_bands,
            gdal_dtype,
            {
                "TILED": "YES",
                "SPARSE_OK": "TRUE",
                "BIGTIFF": "YES",
                "COMPRESS": "LZW",
                "BLOCKXSIZE": chunk_sizes["x"],
                "BLOCKYSIZE": chunk_sizes["y"],
            },
        )

        # Calculate transform from x and y coordinates
        x_coords = da.x.values
        y_coords = da.y.values

        # Calculate pixel size
        x_res = (
            (x_coords[-1] - x_coords[0]) / (len(x_coords) - 1)
            if len(x_coords) > 1
            else 1.0
        )
        y_res = (
            (y_coords[-1] - y_coords[0]) / (len(y_coords) - 1)
            if len(y_coords) > 1
            else 1.0
        )

        # Create geotransform (upper_left_x, pixel_width, row_rotation, upper_left_y, column_rotation, pixel_height)
        transform = (
            x_coords[0] - x_res / 2,
            x_res,
            0,
            y_coords[0] - y_res / 2,
            0,
            y_res,
        )

        # Set the geotransform for the dataset
        ds.SetGeoTransform(transform)

        # Set the projection for the dataset
        ds.SetProjection(f"EPSG:{self._get_epsg_code()}")

        # Finally close the dataset to flush data to disk
        ds = None

    @decorators.log_time
    def _write_block(self, out_file, band_idx, xoff, yoff, data):
        out_ds = gdal.Open(out_file, gdal.GA_Update)
        out_band = out_ds.GetRasterBand(band_idx + 1)
        out_band.WriteArray(data.T, xoff, yoff)
        out_band.FlushCache()

        out_ds = None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True)
    def __read_and_write_block__(
        self, da, band_idx, xoff, yoff, xsize, ysize, out_file
    ):
        data = da.isel(
            band=band_idx, x=slice(xoff, xoff + xsize), y=slice(yoff, yoff + ysize)
        ).values
        return data

    def _write_data_to_cog(self, da, output_path):
        futures = []
        num_bands, width, height = da.shape

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=helpers.get_processpool_workers()
        ) as executor:
            args = []
            for band_idx in range(0, num_bands):
                x_chunk_size, y_chunk_size = (
                    self.da.chunksizes["x"][0],
                    self.da.chunksizes["y"][0],
                )

                for xoff in range(0, width, x_chunk_size):
                    for yoff in range(0, height, y_chunk_size):
                        if xoff + x_chunk_size > width:
                            xsize = width - xoff
                        else:
                            xsize = x_chunk_size

                        if yoff + y_chunk_size > height:
                            ysize = height - yoff
                        else:
                            ysize = y_chunk_size

                        args.append((band_idx, xoff, yoff))

                        futures.append(
                            executor.submit(
                                self.__read_and_write_block__,
                                da,
                                band_idx,
                                xoff,
                                yoff,
                                xsize,
                                ysize,
                                output_path,
                            )
                        )

            for idx, future in enumerate(
                tqdm(
                    futures,
                    total=len(futures),
                    desc="Writing blocks to COG",
                    position=1,
                )
            ):
                data = future.result()
                band_idx, xoff, yoff = args[idx]
                self._write_block(output_path, band_idx, xoff, yoff, data)
        return True

    def _export_to_cog(self, da, output_file_path, overwrite):
        """Can export a 3D dataarray with dims (band, x, y) to a COG"""

        if ("band", "x", "y") != da.dims:
            raise ValueError("Invalid dims")

        crs = self._get_epsg_code()
        if crs is None:
            raise ValueError("No crs found")

        # Remove the output file if it already exists
        if overwrite:
            helpers.remove_file_if_exists(output_file_path)

        try:
            ds = gdal.Open(output_file_path)
            if ds is None:
                raise FileNotFoundError(f"File {output_file_path} not found")
        except Exception as e:
            pass

        # Create a template cog
        self._create_template_cog(da, output_file_path)

        # Write data to cog parallely
        self._write_data_to_cog(da, output_file_path)

        # TODO: Add code to use cogger to convert the geotiff to a cog

        return output_file_path

    def _get_epsg_code(self):
        """
        Get the coordinate reference system (CRS) from the DataArray.

        Returns:
            int or None: The EPSG code of the CRS if available, otherwise None.
        """
        return int(self.da.coords["spatial_ref"].values)

    def export(self, output_path, overwrite=False):
        """
        Export an xarray DataArray to Cloud Optimized GeoTIFF (COG) format.

        This method handles different dimensional configurations:
        - For 4D data (time, band, x, y): Creates separate COGs for each time step
        - For 3D data (band, x, y): Creates a single COG with multiple bands
        - For 2D data (x, y): Creates a single COG with one band

        Parameters
        ----------
        output_path : str
            Path where the COG(s) will be saved. For time series data, this should be a directory.
            For single COG, this should be the full file path.
        overwrite : bool, optional
            If True, overwrites existing files at the output path. Default is False.

        Returns
        -------
        None
            Creates COG file(s) at the specified output path.

        Raises
        ------
        ValueError
            If the DataArray dimensions are not valid for COG export.
        """
        local_output_dir = os.path.join(
            helpers.get_tmp_dir(), "exports", str(uuid.uuid4())
        )

        if io.get_storage_engine(output_path) == "s3":
            helpers.make_sure_dir_exists(local_output_dir)

            # Get the last part of the output path (filename or directory name)
            output_name = os.path.basename(os.path.normpath(output_path))

            # If output_path is a directory with trailing slash, basename returns empty string
            if not output_name:
                # Get the last directory name
                output_name = os.path.basename(
                    os.path.dirname(os.path.normpath(output_path))
                )

            if output_name.endswith(".tif") or output_name.endswith(".tiff"):
                _output_path = f"{os.path.join(local_output_dir, output_name)}"
            else:
                _output_path = f"{os.path.join(local_output_dir, output_name)}/"
        else:
            _output_path = output_path

        # Generate a random folder path if needed
        helpers.make_sure_dir_exists(_output_path)

        dims = self.da.dims
        cogs_path = []
        if "time" in dims:
            for time_idx in range(len(self.da.time)):
                t = pd.to_datetime(self.da.time[time_idx].values)
                timestring = t.strftime("%Y-%m-%d-%H:%M:%S")
                output_file = f"{os.path.join(_output_path, timestring)}.tif"
                cogs_path.append(
                    self._export_to_cog(
                        self.da.isel(time=time_idx), output_file, overwrite
                    )
                )
        elif "band" in dims:
            self._export_to_cog(self.da, _output_path, overwrite)
            cogs_path.append(_output_path)
        elif "x" in dims and "y" in dims:
            # If x and y dim exists, export the dataarray as a single cog with 1 band
            # Add a new dimension 'band' with value 1
            da_with_band = self.da.expand_dims(dim={"band": [1]})

            # Ensure the dimensions are in the correct order (band, x, y)
            da_with_band = da_with_band.transpose("band", "x", "y")

            # Export as a single COG
            self._export_to_cog(da_with_band, _output_path, overwrite)
            cogs_path.append(_output_path)
        else:
            raise ValueError("No valid dims found")

        # self._create_edk_json(cogs_path)

        if io.get_storage_engine(output_path) == "s3":
            io.sync_to_s3(_output_path, output_path)
            io.remove_dir_or_file(local_output_dir)

    def _create_edk_json(self, cogs_path):
        dataset_dict = {
            "EDKDataset": {
                "name": self.da.name,
                "VRTDatasets": [],
            }
        }

        for cog_path in cogs_path:
            if len(cog_path) > 1:
                date_str = cog_path.split("/")[-1].split(".")[0]
                vrt_dataset = {
                    "source": cog_path,
                    "time": date_str,
                    "has_time_dim": True,
                }
            else:
                vrt_dataset = {
                    "source": cog_path,
                    "time": "1970-01-01-00:00:00",
                    "has_time_dim": False,
                }
            dataset_dict["EDKDataset"]["VRTDatasets"].append(vrt_dataset)

        with open(
            os.path.join(f"{os.path.dirname(cogs_path[0])}", f"{self.da.name}.json"),
            "w",
        ) as f:
            json.dump(dataset_dict, f, indent=2)

    def __read_chunk__(self, x_start, y_start, x_chunk_size, y_chunk_size):
        x_end = min(x_start + x_chunk_size, self.da.shape[0])
        y_end = min(y_start + y_chunk_size, self.da.shape[1])

        return (
            y_start,
            x_start,
            self.da.isel(y=slice(y_start, y_end), x=slice(x_start, x_end)).values,
        )

    @decorators.log_time
    @decorators.log_init
    def read_as_array(self):
        x_size, y_size = self.da.shape
        result = np.full((x_size, y_size), np.nan, dtype=self.da.dtype)
        # Get chunk size from the DataArray if available, otherwise use default
        x_chunk_size, y_chunk_size = (
            self.da.chunksizes["x"][0],
            self.da.chunksizes["y"][0],
        )

        # Create chunks
        chunks = [
            (x, y)
            for x in range(0, x_size, x_chunk_size)
            for y in range(0, y_size, y_chunk_size)
        ]

        # Process chunks in parallel using concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=helpers.get_threadpool_workers()
        ) as executor:
            futures = [
                executor.submit(self.__read_chunk__, x, y, x_chunk_size, y_chunk_size)
                for x, y in chunks
            ]

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Reading data in chunks",
                unit="chunk",
            ):
                y_start, x_start, chunk = future.result()
                chunk_x_size, chunk_y_size = chunk.shape
                result[
                    x_start : x_start + chunk_x_size, y_start : y_start + chunk_y_size
                ] = chunk

        return result

    def plot(self, colors=None, opacity=1):
        """
        Plot the data on an interactive map using folium.

        Note: This implementation currently loads all the data into memory,
        which may not be suitable for larger datasets. Future optimizations
        such as tilesets are planned to improve performance with large datasets.

        Args:
            colors (list, optional): A list of colors to use for the colormap.
                If None, the default viridis colormap will be used.
            opacity (float, optional): The opacity of the overlay, between 0 and 1.
                Defaults to 1 (fully opaque).

        Returns:
            folium.Map: An interactive map with the data overlaid on OpenStreetMap.
        """
        # Check if the DataArray is 2D (excluding time and band dimensions)
        if len(self.da.dims) != 2 or set(self.da.dims) != {"x", "y"}:
            raise ValueError(
                "DataArray must be exactly 2D with 'x' and 'y' dimensions to plot on a map"
            )

        # Import the Folium plotter and create the map
        o = Folium(self.da)
        return o.plot(colors=colors, opacity=opacity)
