import os
import shutil
from shapely import Polygon
import hashlib
import logging

logger = logging.getLogger(__name__)


def make_sure_dir_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def empty_dir(path):
    try:
        shutil.rmtree(path)
    except Exception as e:
        logger.error(e)


def make_list(ele):
    if type(ele) != list:
        return [ele]
    else:
        return ele


def get_max_workers():
    cpu_count = 1
    if os.cpu_count() != None:
        cpu_count = os.cpu_count()
    return cpu_count - 2


def get_tmp_dir():
    tmp_dir = f'{os.getenv("TMP_DIR")}/tmp'
    make_sure_dir_exists(tmp_dir)
    return tmp_dir


def delete_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)


def polygonise_2Dcells(df_row):
    return Polygon(
        [
            (df_row.x_min, df_row.y_min),
            (df_row.x_max, df_row.y_min),
            (df_row.x_max, df_row.y_max),
            (df_row.x_min, df_row.y_max),
        ]
    )


def cheap_hash(input):
    return hashlib.md5(input.encode("utf-8")).hexdigest()[:6]
