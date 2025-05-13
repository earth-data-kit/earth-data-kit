import os
import shutil


def get_storage_engine(path):
    if path.startswith("s3://"):
        return "s3"
    elif path.startswith("/") or path.startswith("./"):
        return "local"
    else:
        raise ValueError(f"Invalid path: {path}")


def sync_to_s3(_output_path, output_path):
    cmd = f"s5cmd sync {_output_path} {output_path}"
    os.system(cmd)


def remove_dir_or_file(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)
