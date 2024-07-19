## Stitching

Earth Observation data is usually distributed in small scene files, with different projections, resolutions across data providers. eg: s3://modis-pds/, s3://sentinel-s2-l1c/.

Before using this data it's important to combine different scene files together, re-arrange or combine bands from different files or change the projection or resolution of the dataset.

This module aims to make it easy for anyone to download and combine EO scene files, re-arrange bands, change projection or resolution.

The main class implemented in `dataset`.

### Usage Examples
1. Using a grid file. Grid file is typically a kml or shapefile which contains a mapping of data provider's grid to world coordinates.

* First we import the dataset class
```python
  from spacetime_tools.stitching.classes import dataset
```

* Then we initialize source and destination variables
```python
  source = "s3://modis-pds/MCD43A4.006/{x}/{y}/%Y%j/*_B0?.TIF"
  destination = "/Volumes/Data/spacetime-tools/final/modis-pds/%d-%m-%Y-b07.TIF"
  grid_fp = "stitching/sample_data/sample_kmls/modis.kml"

  region = "us-west-2"
  bbox = country_bounding_boxes["AL"] # Bounding box to stitch together
  date_range = (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 10)) # Date range (start, end) for time dimension
```

* Then downloading and stitching together the scene files can be done by
```python
  # This initializes the dataset object
  ds = dataset.DataSet("modis-pds", "s3", source, overwrite=False)

  # Setting time bounds
  ds.set_timebounds(date_range[0], date_range[1])

  # Setting spatial bounds
  ds.set_spacebounds(bbox[1], grid_fp, fn)

  # Getting distinct bands, this is important to find what bands exists in the dataset. Uses GDAL internally
  bands = ds.get_distinct_bands()
  print (bands)

  # Downloads data to local filesystem
  ds.sync()

  # Stitching data together as COGs.
  # An ordered list of bands should be provided to decide the arrangement of bands in output file(s).
  ds.to_cog(
      destination,
      bands=[
          "Nadir_Reflectance_Band1",
          "Nadir_Reflectance_Band3",
          "Nadir_Reflectance_Band7",
      ],
  )
```
