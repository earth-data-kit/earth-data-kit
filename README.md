# Earth Data Kit

EDK is designed to simplify building complete end-to-end data pipeline, helping you connect various parts of the GIS process with ease. With Earth Data Kit, data scientists can focus on analyzing data and drawing insights instead of wrestling with complex data processes and engineering challenges.

## Warning

### This project is under active development

**If you wish to contribute please reach out on <siddhant@earthlabs.io>**

## Getting Started

### Prerequisites

Before using Earth Data Kit, ensure that the following are installed:

* Python 3.12 or newer

* Docker (required) - [https://www.docker.com/get-started](https://www.docker.com/get-started)

### Installation & Usage

The preferred way to install and use Earth Data Kit is via edk-cli.

1. Install edk-cli:
    ```bash
    pip3 install https://github.com/earth-data-kit/edk-cli/releases/download/0.1.0/edk_cli-0.1.0-py3-none-any.whl
    ```

2. Now you can use the below commands to configure or run edk containers:
    ```bash
    edk configure # Helps user create an .env file for edk.
    ```
    More env options can be found on https://earthlabs.io/earth-data-kit/getting-started.html#environment-configuration
- Use relative paths (not absolute paths) when specifying directories.
     - Example: ./workspace
     - Avoid: /Users/username/earth-data-kit/workspace

    ```bash
    edk run # Runs edk container
    ```

    ```bash
    edk notebook # Runs a jupyter notebook within edk container
    ```

> There are more example scripts available inside the `examples/` directory of this repository. Explore them to see how to use Earth Data Kit for different geospatial workflows.

## Supported Engines and Formats

Earth Data Kit supports several data engines and formats, as summarized below. We're working to add more—feel free to contact the maintainers if you'd like to contribute!

**Supported Engines**

| Engine         | Description                                      | Notes                                                      |
|----------------|--------------------------------------------------|------------------------------------------------------------|
| `s3`           | Files stored on S3                               |                                                            |
| `earth_engine` | Google Earth Engine                              |                                                            |
| `stac`         | STAC Collections                                 |                                                            |


**Supported Formats**

| Format         | Description                                      | Notes                                                      |
|----------------|--------------------------------------------------|------------------------------------------------------------|
| `geotiff`      | GeoTIFF/COGs                                     | Most portable and widely supported                         |
| `netcdf`       | NetCDF                                           | **Only supported on Linux environments**                   |
| `stac_asset`   | Assets referenced via STAC                       |                                                            |
| `earth_engine` | Google Earth Engine assets                       |                                                            |

If you need a format or engine not listed here, please [open an issue](https://github.com/earth-data-kit/earth-data-kit/issues) or contact the maintainers.

Checkout the [docs](https://earthlabs.io/earth-data-kit/) for more details.

See the [examples](./examples) in this repository.

Checkout the [roadmap](https://earthlabs.io/earth-data-kit/roadmap.html).

> **Note:**  
> 
> In this toolkit, `x` = longitude (**-180** to **180**), `y` = latitude (**-90** to **90**).  
> **Always use (x, y) = (longitude, latitude) order** for bounding boxes and coordinates.
>
> Many GIS tools (like QGIS) display coordinates as (latitude, longitude) or (y, x).  
> **Double-check coordinate order** when copying from GIS tools.
>
> For rasters:  
> - `x` (longitude) = column (horizontal)  
> - `y` (latitude) = row (vertical)  
> 
> **GDAL** and most raster libraries return data as `[row, col]` or `[y, x]` (not `[x, y]`).  
> **Be careful to use the correct axis order** when working with arrays and coordinates.
