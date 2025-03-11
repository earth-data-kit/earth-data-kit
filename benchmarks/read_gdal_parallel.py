from osgeo import gdal
from earth_data_kit.stitching.decorators import log_time, log_init
import time
import concurrent.futures


def read_block(vrt_path, x, y, x_size, y_size):
    print("Reading block at", x, y, x_size, y_size)
    ds = gdal.Open(vrt_path)
    block = ds.ReadAsArray(x, y, x_size, y_size)
    return block.mean(), block.size


@log_init
@log_time
def read_gdal_parallel(vrt_path, block_multiplier, max_workers):
    ds = gdal.Open(vrt_path)
    x_raster_size, y_raster_size = ds.RasterXSize, ds.RasterYSize
    x_block_size, y_block_size = 128, 128

    x_jump_size = x_block_size * block_multiplier
    y_jump_size = y_block_size * block_multiplier

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for x in range(0, x_raster_size, x_jump_size):
            for y in range(0, y_raster_size, y_jump_size):

                # print (block.shape)
                if x + x_jump_size > x_raster_size:
                    x_size = x_raster_size - x
                else:
                    x_size = x_jump_size

                if y + y_jump_size > y_raster_size:
                    y_size = y_raster_size - y
                else:
                    y_size = y_jump_size

                futures.append(
                    executor.submit(read_block, vrt_path, x, y, x_size, y_size)
                )

        total_mean, total_size = 0, 0
        for future in concurrent.futures.as_completed(futures):
            mean, size = future.result()
            total_mean = total_mean + (mean * size)
            total_size = total_size + size

        print(total_mean / total_size)
