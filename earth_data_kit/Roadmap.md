Earth Data Kit's

1. edk.stitching.Dataset -- A class to initialize a remote dataset, either raster or vector. 
    * Should be able to support the following raster layouts
        1. Raster datasets with multiple bands kept across multiple scene files. Directory structure has spatial and temporal dimensions.
            eg: https://registry.opendata.aws/sentinel-2/
            Source specified will be s3://sentinel-2/tiles/{utm_code}/{lat_band}/{square}/%Y/%m/%d/\*/*.tif
        2. Raster datasets kept in a folder and user knows the directory structure. Single wildcard (*). Doesn't have temporal dimension.
            eg: https://registry.opendata.aws/copernicus-dem/
            Source specificed will be s3://copernicus-dem-30m/\*/\*.tif
        3. Direct file paths to the raster files -- multiple files.
            eg: https://github.com/globalmaps/gm_lc_v3
        4. Single raster file.
            eg: s3://us-west-2.opendata.source.coop/vizzuality/lg-land-carbon-data/deforest_100m_cog.tif
        5. Raster datasets kept in folder but user doesn't know the directory structure. Double wildcard (**)
            eg: https://registry.opendata.aws/copernicus-dem/
                Someone might want to specify s3://copernicus-dem-30m/*\*/\*.tif