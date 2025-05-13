Example: MODIS PDS
==================

A source pattern replaces parts of a file path with placeholders or wildcards to match multiple files with similar naming patterns.

For example, consider MODIS data on AWS:

• MODIS on AWS: <https://registry.opendata.aws/modis/>
• MODIS Documentation: <https://docs.opendata.aws/modis-pds/readme.html>

Take this example scene file path:

``s3://modis-pds/MCD43A4.006/01/08/2013160/MCD43A4.A2013160.h00v08.006.2016138043045_B07.TIF``

The file path follows this layout as described in the documentation:

``/product/horizontal_grid/vertical_grid/date/DATA``

In this layout:

• The "product" segment identifies the dataset (e.g., "MCD43A4.006").
• The "horizontal_grid" segment (e.g., "01") is replaced with the placeholder {h}.
• The "vertical_grid" segment (e.g., "08") is replaced with the placeholder {v}.
• The "date" segment (e.g., "2013160") is formatted using Python's "%Y%j" date format.
• The "DATA" segment represents the file name, where wildcards like * or ? (e.g., \*_B07.TIF) can be used to select multiple files.

Thus, the generalized source pattern becomes:

``s3://modis-pds/MCD43A4.006/{h}/{v}/%Y%j/*_B07.TIF``

This pattern allows you to match and process all files that follow the same naming convention, regardless of the specific values of {h}, {v}, and the date.
