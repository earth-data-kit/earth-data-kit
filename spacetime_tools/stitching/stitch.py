def stitch(i_df, band_arrangement="file-per-band", frequency="daily"):
    """Gets an inventory file path and stitches based on geotransform, resolution and blocksizes.
    Creates a GTI - GDAL Raster Tile Index
    https://gdal.org/drivers/raster/gti.html#raster-gti"""
    # Gets the inventory as input

    # Gets unique bands and resolutions
    unique_bands = i_df.groupby(by=["description", ""])

    # Gets band-arrangement from user, for now file-per-band which means
    # one file per band - res combination

    # Get's frequency and pattern from user telling which part is the date
    # From there create output folders, one per band-datetime
    # Inside that create a index file and a .gti xml file

    # Add an option of save which will create COG out of that gti
