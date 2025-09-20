# Earth Data Kit

EDK is designed to simplify building complete end-to-end data pipeline, helping you connect various parts of the GIS process with ease. With Earth Data Kit, data scientists can focus on analyzing data and drawing insights instead of wrestling with complex data processes and engineering challenges.

## Getting Started

### Prerequisites

Before using Earth Data Kit, ensure that the following are installed:

* Python 3.12 or newer

* Docker (required) - [https://www.docker.com/get-started](https://www.docker.com/get-started)

### Installation & Usage

To use Earth Data Kit, follow these steps:

1. Clone the repository:
    ```bash
    git clone https://github.com/earth-data-kit/earth-data-kit.git
    cd earth-data-kit
    ```

2. Copy the sample environment file and edit as needed:
    ```bash
    cp sample.env .env
    # Edit .env to set your own values
    ```

3. Initialize your environment using the provided script:
    ```bash
    bash init.sh
    ```
    This will build and start a docker container

4. Go inside the docker container:
    ```bash
    bash exec-edk.sh
    ```

5. Run any example or your own script
    ```bash
    python3 sentinel2.py
    ```

> There are more example scripts available inside the `examples/` directory of this repository. Explore them to see how to use Earth Data Kit for different geospatial workflows.

## Warning

### This project is under active development

**If you wish to contribute please reach out on <siddhantgupta3@gmail.com>**

Checkout the [docs](https://earth-data-kit.github.io/) for more details.

See the [examples](./examples) in this repository.

Checkout the [roadmap](https://earth-data-kit.github.io/roadmap.html).

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
