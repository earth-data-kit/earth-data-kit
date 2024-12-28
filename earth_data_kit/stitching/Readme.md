## Syncing and Stitching for Earth Observation and Climate Modelling Data

Earth Observation data is usually structured in multiple files kept in a definitive folder structure.
The structure, grid and CRS are all according to the data provider.

Example of data providers - 
1. S3
2. Earth Engine
3. HTTP/HTTPS servers

Class Structure
1. Dataset
    * References an entire dataset distributed with space or time dimensions
    * Contains metadata like the data_provider_id
    * Attributes - 
        1. name
        2. source
        3. engine
    * Methods -
        * discover - Performs the scan method on remote dataset.
        * set_timebounds - Set's the time boundary for the dataset. Helps in filtering
        * set_spacebounds - Set's the spatial boundary for the dataset.
2. Tile
    * References a single file kept within dataset
    * Is mapped to a dataset and can contain single or multiple bands.
    * GDAL should be capable of reading it
    * Corresponds to a specific area at specific time
    * Contains bbox and a timestamp
    * gdal_path
    * engine_path
    * bbox
    * timestamp
3. Band
    * Has a specific dtype
    * Is within a Tile and has data about a specific metric, example - Temperature
    * GDAL should be capable of reading it
    * band_idx
    * dtype
    * crs
    * x_res
    * y_res
    * projection
    * description
    * gdal_path
4. DatasetProvider
    * Methods - 
        * scan - Scans the remote dataset and creates the inventory files. files.csv


Now when a user initializes a dataset and runs .discover method we scan the remote dataset and creates the data catalog.

From the data catalog we can on the fly fetch data for a specific CRS, resolution and AOI with a specific band arrangement.

Examples
Some S3 examples
1. Modis - https://docs.opendata.aws/modis-pds/readme.html
2. Sentinel-2 https://registry.opendata.aws/sentinel-2/
3. Landsat - https://docs.opendata.aws/landsat-pds/readme.html

Data is either kept in folders or an object store which then can be accessed by specialized APIs, like in earth engine or by downloading files individually and working on them.

In case data provider, provides APIs - it becomes easy for users to download and combine data.

When there are no APIs, it's a little tricky. Example of data kept in object store

## Modis-PDS
`/product/horizontal_grid/vertical_grid/date/DATA`
where:
product = MCD43A4.006 - MODIS product ID
horizontal_grid = 21 - horizontal grid in the MODIS Sinusoidal Tiling System
vertical_grid = 07 - vertical grid in the MODIS Sinusoidal Tiling System
date = 2011360 - year and three-digit day of year representation
DATA - individual object (e.g., granule, metadata or thumbnail)

Ex. - modis-pds/MCD43A4.006/21/07/2011360/

## Sentinel-2-l1c
Path 
`tiles/[UTM code]/latitude band/square/[year]/[month]/[day]/[sequence]/DATA`
Where:
[UTM code] = e.g. 10 - grid zone designator.
latitude band = e.g. S - latitude band are lettered C- X (omitting the letters "I" and "O").
square = e.g DG - pair of letters designating one of the 100,000-meter side grid squares inside the grid zone.
[year] = e.g. 2015 - is the year the data was collected.
[month] = e.g. 12 - is the month of the year the data was collected (without leading zeros).
[day] = e.g. 7 - is the day of the month the data was collected (without leading zeros).
[sequence] = e.g. 0 - in most cases there will be only one image per day. In case there are more (in northern latitudes), the following images will be 1,2,…\.
DATA - data for each tile are organised as in original product S2 MSI Product Specification.

Ex. - s3://sentinel-s2-l1c/tiles/1/C/CV/2020/3/7/0/B01.jp2
s3://sentinel-s2-l1c/tiles/1/C/CV/2020/3/7/1/B01.jp2


## Landsat
`landsat-pds/[collection]/[landsat]/[scene path]/[scene row]/LXSS_LLLL_PPPRRR_YYYYMMDD_yyymmdd_CC_TX/`
The “c1” refers to Collection 1, the “L8” refers to Landsat 8, “139” refers to the scene’s path, “045” refers to the scene’s row, and the final directory matches the product’s identifier, which uses the following naming convention: LXSS_LLLL_PPPRRR_YYYYMMDD_yyymmdd_CC_TX, in which:

L = Landsat
X = Sensor
SS = Satellite
PPP = WRS path
RRR = WRS row
YYYYMMDD = Acquisition date
yyyymmdd = Processing date
CC = Collection number
TX = Collection category

# APIs
`.discover()` - Accepts a bbox, start_date and end_date as inputs and creates the catalog of dataset kept in remote location. This catalog is can then used to fetch data in a specific CRS, resolution and grid system.
Algorithm - 
1. First finds all scene files kept in the remote dataset using the `.scan` method. Takes in the `source`, `time_opts` and `space_opts`
2. Once all the scene files are fetched we use gdal to get the metadata and create `band` model instances.

`.to_vrts()` - Is typically called after discover method and can be used to combine dataset into a singlea
Algorithm - 
* We list all the tiles and get band data from every tile using gdal.
* This is stored in tiles and bands models.
* From bands we can identify unique bands available. Unique in terms of description, x_res, y_res, dtype and crs.
* Then user supplies the band arrangement they want along with some creation options (-co) for GDAL.
* We fetch the data catalog
* Then we split all bands from all files using `gdalbuildvrt`.
* Then we mosaic all relevant bands with same timestamp using `gdalbuildvrt`. This should be okay as number of scenes per timestamp will be less, in orders of 1-100. This typically happens when a satellite captures multiple images in a single day to make sure nothing is missed and data is distributed daily. Example in sentinel-2
* Then we combine vrts with same timestamps again but this time with the entire AOI. This creates band wise vrts for the entire area of interest. This is done by create a GDAL raster tile index. `gdaltindex`
* Then we stack these mosaics into bands as required by the user again using `gdalbuildvrt`
* Finally we set the metadata of our output rearranged dataset


Gotchas & Assumption - 
* Spatial resolution of a single band can be in cms and time resolution in less than a day. 
* We can have multiple images per day which are overlapping. In this case we need to mosaic the data together using `gdalbuilvrt`. Note it's important to have the correct no data value when running `gdalbuilvrt` or else it will overlap incorrectly.