Defining source
---------------

Source is usually made up of three components and is derived from the full path of a single scene file.
Taking an example of modis data on AWS, <https://registry.opendata.aws/modis/> 

Documentation - <https://docs.opendata.aws/modis-pds/readme.html>

Path of single scene file

``s3://modis-pds/MCD43A4.006/01/08/2013160/MCD43A4.A2013160.h00v08.006.2016138043045_B07.TIF``

According to the documentation the file path layout is

``/product/horizontal_grid/vertical_grid/date/DATA``

So for our example 

* ``horizontal_grid`` - 01 - h
* ``vertical_grid`` - 08 - v
* ``date`` - 2013160 - %Y%j according to python's standard format codes <https://docs.python.org/3/library/datetime.html#format-codes>
* ``*_B07.TIF`` - For the file name. _You can use * or ? wildcards to select multiple files

So the source becomes

``s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF```