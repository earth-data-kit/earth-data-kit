import os
import pandas as pd
import logging
import spacetime_tools.stitching.helpers as helpers
import pathlib

logger = logging.getLogger(__name__)


class S3:
    def __init__(self) -> None:
        no_sign_flag = os.getenv("AWS_NO_SIGN_REQUEST")
        request_payer_flag = os.getenv("AWS_REQUEST_PAYER")
        profile_flag = os.getenv("AWS_PROFILE")
        json_flag = "--json"
        if no_sign_flag and ((no_sign_flag).upper() == "YES"):
            no_sign_flag = "--no-sign-request"
        else:
            no_sign_flag = ""

        if (request_payer_flag) and (request_payer_flag.upper() == "requester"):
            request_payer_flag = f"--request-payer requester"
        else:
            request_payer_flag = ""

        if profile_flag:
            profile_flag = f"--profile {profile_flag}"
        else:
            profile_flag = ""

        self.base_cmd = (
            f"s5cmd {no_sign_flag} {request_payer_flag} {profile_flag} {json_flag}"
        )

        print(self.base_cmd)

    def create_inventory(self, patterns, tmp_base_dir):
        ls_cmds_fp = f"{tmp_base_dir}/ls_commands.txt"
        inventory_file_path = f"{tmp_base_dir}/inventory.csv"
        df = pd.DataFrame(patterns, columns=["path"])

        # go-lib expects paths in unix style
        df["path"] = df["path"].str.replace("s3://", "/")

        df.to_csv(ls_cmds_fp, index=False, header=False)
        lib_path = os.path.join(
            pathlib.Path(__file__).parent.resolve(),
            "..",
            "shared_libs",
            "builds",
            "go-lib",
        )

        ls_cmd = f"{lib_path} {ls_cmds_fp} {inventory_file_path}"
        os.system(ls_cmd)

        df = pd.read_csv(inventory_file_path, names=["key"])

        # Fixing output from go-lib
        df["key"] = "s3://" + df["key"].str[1:]

        # Adding gdal_path
        df["gdal_path"] = df["key"].str.replace("s3://", "/vsis3/")
        df["engine_path"] = df["key"]

        return df[["engine_path", "gdal_path"]]

    def sync_inventory(self, df, tmp_base_dir):
        # Deleting /raw dir where data will be synced
        base_path = f"{tmp_base_dir}/raw"
        helpers.delete_dir(f"{base_path}/")
        local_path = f"{base_path}/" + df["engine_path"].map(
            lambda x: x.replace("s3://", "")
        )
        cmds = "cp" + " " + df["engine_path"].map(str) + " " + local_path
        cmds_fp = f"{tmp_base_dir}/sync_commands.txt"

        cmds.to_csv(cmds_fp, header=False, index=False)
        s5_cmd = f"{self.base_cmd} run {cmds_fp}"

        os.system(s5_cmd)

        df["local_path"] = local_path
        return df
