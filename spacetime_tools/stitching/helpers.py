import os
import shutil

def make_sure_dir_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def delete_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)
