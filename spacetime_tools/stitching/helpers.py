import os
import shutil


def make_sure_dir_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


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
