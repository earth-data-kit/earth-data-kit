import os
import pandas as pd
import logging
import spacetime_tools.stitching.helpers as helpers

logger = logging.getLogger(__name__)

class S3:
    def __init__(self, options) -> None:
        self.options = options

    def create_inventory(self, patterns, tmp_base_dir):
        # TODO: Add optimization to search till common first wildcard and filter them later
        # This is done because sometimes space dimension is before time and s5cmd lists all files till first wildcard and then filters them in memory
        ls_cmds_fp = f"{tmp_base_dir}/ls_commands.txt"
        inventory_fp = f"{tmp_base_dir}/inventory.json"

        fp = open(ls_cmds_fp, mode="w")
        cmds = []
        for pl in patterns:
            cmd = f"ls {pl}\n"
            cmds.append(cmd)
        fp.writelines(cmds)
        fp.close()

        s5_cmd = f"s5cmd --no-sign-request --json run {ls_cmds_fp} > {inventory_fp}"
        os.system(s5_cmd)

        df = pd.read_json(inventory_fp, lines=True)

        # Adding gdal_path
        df["gdal_path"] = df["key"].str.replace("s3://", "/vsis3/")
        df["engine_path"] = df["key"]

        return df[["engine_path", "gdal_path", "size"]]


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
        s5_cmd = f"s5cmd --no-sign-request run {cmds_fp}"

        os.system(s5_cmd)

        df["local_path"] = local_path
        return df
