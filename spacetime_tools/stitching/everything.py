spacetime_tools = None

def sync_and_stitch(space_filters_applied=None):

    # Gets the pattern list as input and explodes it with different combinations across time
    spacetime_tools.resolve_time_filters()

    # Explodes along space dimension, atleast tries to using a grid file, if present.
    # If a grid file is present then the user also needs to provide the column and a small matching function
    # If a grid file is not present then we don't explodes using space dimension
    spacetime_tools.resolve_space_filters()

    # Then we run discovery functions, first minimally. Minimally means just the unix-like metadata.
    # size, file_path, last_modified. This will output an inventory which is nothing but a list of files we need to download
    spacetime_tools.discover(minimal=True)

    # Once that is done we run gdalinfo on all the files parallelly, see if we can call go library in here?
    if space_filters_applied:
        # This will download all the data
        spacetime_tools.sync()

        # Runs locally
        spacetime_tools.discover(minimal=False)
    else:
        # This will run gdal-info on all files and get extents and things like that. Does remote-discovery
        spacetime_tools.discover(minimal=False)

        # Then we filter files which we need, this is done spatially
        spacetime_tools.filter_inventory()

        # This will download all the data
        spacetime_tools.sync()

    # After the above if condition data is downloaded locally and is ready to be stitched
    spacetime_tools.stitch()
    
    pass


if __name__ == '__main__':
    kwargs = {
        "time_filter": None,
        "space_filter": None,
        "pattern_list": [],
        "source": "s3/earth_engine",
        "output_parameters": {}
    }


    spacetime_tools.sync_and_stitch(**kwargs)