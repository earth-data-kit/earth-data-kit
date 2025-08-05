Earth Data Kit's

1. edk.stitching.Dataset -- A class to initialize a remote dataset, either raster or vector. 
    * Should be able to support the following raster layouts
        1. Raster datasets with multiple bands kept across multiple scene files. Directory structure has spatial and temporal dimensions. User knows the directory structure.
            eg: https://registry.opendata.aws/sentinel-2/
            Source specified will be s3://sentinel-2/tiles/{utm_code}/{lat_band}/{square}/%Y/%m/%d/\*/*.tif
            In this case, the dataset will be stitched by first stitching the tiles for a given date and then by date. VRTs are created for each date and then listed together in a json file.
        2. Raster datasets kept remotely with spatial and temporal dimensions. User knows the directory structure but only for temporal dimension. Single wildcard (*) cases.
            eg: https://registry.opendata.aws/sentinel-2/
            Source specified will be s3://sentinel-2/tiles/\*/\*/\*/%Y/%m/%d/\*/*.tif
            Follows case 1 for stitching. For discovery lists all the metadata for all the tiles and then runs and intersection query on it.
        3. Raster datasets kept in a folder and has only spatial dimension, no temporal dimension. User knows the directory structure. Single wildcard (*) cases.
            eg: https://registry.opendata.aws/copernicus-dem/
            Source specificed will be s3://copernicus-dem-30m/\*/\*.tif
            In this case, the dataset will be stitched together by listing all the files in the directory. Band selection will still be done.
            VRT will be created for each band and they will be stacked together in single vrt file. As we don't have date informatation in the source we will hardcode the date as epoch start date -- 1970-01-01. Then the system will follow the normal stitching process.
            The to_dataarray function remains the same as we are not hardcoding the date to epoch start date in case it doesn't exist.
        3. Direct file paths to the raster files -- multiple files.
            eg: https://github.com/globalmaps/gm_lc_v3
            VRT will be created for each band and they will be stacked together in single vrt file. Similar to case 2.
        4. Single raster file.
            eg: s3://us-west-2.opendata.source.coop/vizzuality/lg-land-carbon-data/deforest_100m_cog.tif
            Similar to case 2. Pass the raster file path in an array.
        5. Raster datasets kept in folder but user doesn't know the directory structure. Double wildcard (**)
            eg: https://registry.opendata.aws/copernicus-dem/
                Someone might want to specify s3://copernicus-dem-30m/*\*/\*.tif
                Files will be listed without expansion and directly with s5cmd in case of s3. In case of earth_engine wildcards are not supported so doesn't matter.
                On listing will follow case 2.