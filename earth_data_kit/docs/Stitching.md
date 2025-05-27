# Stitching in Earth Data Kit

Earth Data Kit provides powerful capabilities for stitching together remote raster datasets. The stitching functionality is designed to handle various types of raster data layouts and sources, making it flexible for different use cases.

## Goal

The primary goal of the stitching module is to efficiently combine multiple raster tiles into a single, coherent dataset while:

1. Handling different data source types (S3, Earth Engine, local files)
2. Supporting various directory structures and file organizations
3. Managing both spatial and temporal dimensions
4. Preserving data quality and consistency
5. Optimizing performance for large datasets

## Supported Data Layouts

The stitching module can handle several common raster data layouts:

1. **Multi-band, Multi-scene Files**: Datasets where bands are spread across multiple scene files with known spatial and temporal directory structures (e.g., Sentinel-2 data on AWS)
2. **Remote Datasets with Wildcards**: Datasets where only the temporal structure is known, using single wildcard patterns and variables. Reads metadata using GDAL to figure out spatial properties.
3. **Spatial-only Datasets**: Datasets with only spatial dimensions and no temporal component (e.g., Copernicus DEM)
4. **Direct File Paths**: Multiple raster files specified directly. Supplied as an array of file paths.
5. **Single Raster Files**: Individual raster files. Supplied as a single file path.
6. **Unknown Directory Structures**: Datasets where the directory structure is unknown, using double wildcard patterns. Supplied as a single wildcard pattern.

## Example Use Case

A common use case is stitching together satellite imagery from AWS S3, such as MODIS data. The process involves:

1. Initializing the dataset with source information
2. Setting temporal and spatial bounds
3. Discovering available bands
4. Configuring GDAL options
5. Validate band properties like CRS, data-type, resolution, etc.
5. Creating mosaics for selected bands
6. Sync function which downloads all the underlying datasets to a faster storage. There might be cases where we want to sync the bounding box only. Eg: Huge DEM dataset
6. Converting the result to an xarray DataArray for analysis

This functionality makes it easier to work with large, distributed raster datasets while maintaining data quality and processing efficiency.

## API
1. edk.stitching.Dataset(name, source, engine, clean=True) - Creates a new dataset object. Source can be single file path, multiple file paths, single wildcard pattern with variables, double wildcard pattern without variables.

2. set_timebounds(start, end, resolution=None) - Sets the temporal bounds of the dataset. Will need custom frequency to support for 12 days of Sentinel

3. set_spacebounds(bbox, grid_dataframe=None) - Sets the spatial bounds of the dataset.

4. discover() - Discovers the bands available in the dataset. Runs the .scan method of engine internally. After this we have all the metadata of all the relevant tiles.
    1. Runs the .scan method of engine internally. Engine specific scan method will also be responsible of fetching and adding metadata.
    2. Runs intersection of the spatial bounds and the metadata to get the relevant tiles.
    3. Finally saves the metadata to a csv file, called catalog.csv.

5. mosaic(bands=[], resolution=None, crs=None, resampling_method='nearest', sync=False) - Creates a mosaic of the selected bands. Band selection is a GDAL dependent operation. We might choose to change the gdal_path, eg: in Earth Engine so that gdal performance is optimized. Might not need this as we are handling subdatasets within EE codebase.
    1. If sync=True, we will sync the underlying datasets to faster storage by calling engine specific sync method. Once this is done we use local_paths to mosaic the bands. This is done by updating catalog.csv with local_paths. We might want to sync data directly to cloud storage. Maybe using engine specific methods.
    2. Gets all the band tiles available from catalog.csv.
    3. Groups by date to see how many date-wise files we need to mosaic.
    4. Creates a timestamped vrt for each date.
        1. Get relevant scene files for each band - This is happening for a specific date.
        2. Validate band properties like CRS, data-type, resolution, etc. They all should be same.
            2.1 In case they are not same, we will raise an error.
            2.2 In the future we will add functionality to resample the bands to the same resolution and CRS then mosaic them. Again sync=True will be required for this to work efficiently.
        3. Run gdal.BuildVRT to mosaic all the bands together. We optimize gdal_paths to get the best performance.
        4. Stacks the bands together again in a VRT using gdal.BuildVRT.
        5. Runs gdal.Translate to spatially bound the VRT from user supplied bounds.
        6. If resolution or crs is supplied, we will run gdal.Warp to resample the VRT. -- This will raise a warning if sync=True is not supplied as warping remote datasets is slow.
6. to_dataarray() - Converts the dataset to an xarray DataArray.
    1. We have written a custom backend which reads data from the mosaiced vrt using gdal.ReadAsArray. It's current restriction is that it is heavily I/O bound if actual data is kept remotely. Should raise a warning.

edk.xarray
1. edk.stitching.Dataset.dataarray_from_file() -  Returns a dataarray from a file. Typically 4D arrays.
2. da.edk.export() - Exports the dataarray to COGs. This will require output metadata to be supplied like resolution, crs, etc.
3. da.edk.plot() - Plots the dataarray using folium. We might need the raw data file to be supplied to the plot function if we want the ability to use overviews.


## Examples
1. Single big COG like DEM dataset.
User will supply a single file path and a bounding box. 
edk.stitching.Dataset(name, source, engine, clean=True)
edk.stitching.Dataset.set_spacebounds(bbox, grid_dataframe=None)
edk.stitching.Dataset.discover()
edk.stitching.Dataset.mosaic(bands=["elevation"], resolution=None, crs=None, resampling_method='nearest', sync=False)
In this case we do sync=False so raw data meaning the huge file won't be downloaded, but as we are doing export only relevant AOI and bands will be downloaded and created as a COG.
da = edk.stitching.Dataset.to_dataarray()
da.edk.export("elevation.tif")
