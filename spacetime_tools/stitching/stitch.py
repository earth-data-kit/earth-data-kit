import pandas as pd
import concurrent.futures
import logging
import datetime
import Levenshtein as levenshtein
import os
from spacetime_tools.stitching import decorators
import spacetime_tools.stitching.helpers as helpers

logger = logging.getLogger(__name__)


def add_date_to_raw_files(i_df, source, date_range):
    # Creates the date_range patterns again for matching purposes
    dates = pd.date_range(start=date_range[0], end=date_range[1], inclusive="both")
    o_df = pd.DataFrame()
    o_df["date"] = dates
    o_df["source_pattern"] = o_df["date"].dt.strftime(source)

    # Joining using string matching so that every tile has a date associated with it
    for in_row in i_df.itertuples():
        max_score = -99
        max_score_idx = -1
        for out_row in o_df.itertuples():
            s = levenshtein.ratio(in_row.engine_path, out_row.source_pattern)
            if s > max_score:
                max_score = s
                max_score_idx = out_row.Index

        i_df.at[in_row.Index, "date"] = o_df["date"][max_score_idx]

    return i_df


def create_vrts_per_raw_tile(file):
    vrt_files = []
    for tile in file.tiles:
        # Creating vrt for every tile extracing a single band
        vrt_file_path = "/".join(
            (
                tile.split("/")[:-1]
                + [".".join(tile.split("/")[-1].split(".")[:-1]) + ".vrt"]
            )
        )
        buildvrt_cmd = f"gdalbuildvrt -b {file.band_idx} {vrt_file_path} {tile}"
        vrt_files.append(vrt_file_path)
        os.system(buildvrt_cmd)
    return vrt_files


def create_gti_file(file, vrt_files):
    # Now we combine vrts using gdaltindex, one .git file per band-date combination
    gti_base_name = f"{file.description}-{file.date.strftime('%d-%m-%Y')}"
    # For this we need file list first
    pd.DataFrame(vrt_files, columns=["local_path"]).to_csv(
        f"{helpers.get_tmp_dir()}/processing/{gti_base_name}-file-list.txt",
        header=False,
        index=False,
    )

    buildgti_cmd = f"gdaltindex -f FlatgeoBuf {helpers.get_tmp_dir()}/processing/{gti_base_name}.fgb -gti_filename {helpers.get_tmp_dir()}/processing/{gti_base_name}.gti -lyr_name {file.description} -write_absolute_path -overwrite --optfile {helpers.get_tmp_dir()}/processing/{gti_base_name}-file-list.txt"
    os.system(buildgti_cmd)

    return gti_base_name


def convert_to_cog(gti_path, dest_path):
    cmd = f"gdal_translate -of COG {gti_path} {dest_path}"
    os.system(cmd)


def convert_to_zarr():
    pass


@decorators.timed
@decorators.log_init
def stitch(local_inventory_file, source, date_range, dest):
    # Creates the processing directory of intermediary files
    helpers.make_sure_dir_exists(f"{helpers.get_tmp_dir()}/processing/")

    # Gets the inventory as input
    i_df = pd.read_csv(local_inventory_file)

    i_df = add_date_to_raw_files(i_df, source, date_range)

    # We create one file per band_idx-description-resolution-dtype-date combination
    file_by = ["band_idx", "description", "x_res", "y_res", "dtype", "date"]
    files = i_df.groupby(by=file_by).agg(tiles=("local_path", list)).reset_index()

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=helpers.get_max_workers()
    ) as executor:
        futures = []
        for file in files.itertuples():
            vrt_files = create_vrts_per_raw_tile(file)

            gti_base_name = create_gti_file(file, vrt_files)

            # Converting to cog
            futures.append(
                executor.submit(
                    convert_to_cog,
                    f"{helpers.get_tmp_dir()}/processing/{gti_base_name}.gti",
                    f"{files['date'][file.Index].strftime(dest)}",
                )
            )

        executor.shutdown(wait=True)
