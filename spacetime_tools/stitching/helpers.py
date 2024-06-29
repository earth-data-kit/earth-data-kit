import os
import shutil


def make_sure_dir_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def get_tmp_dir():
    tmp_dir = f'{os.getenv("TMP_DIR")}/.tmp'
    make_sure_dir_exists(tmp_dir)
    return tmp_dir

def delete_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)
