import os
import pandas as pd
import logging
import spacetime_tools.stitching.helpers as helpers

logger = logging.getLogger(__name__)


def create_inventory(patterns):
    ls_cmds_fp = f"{helpers.get_tmp_dir()}/ls_commands.txt"
    inventory_fp = f"{helpers.get_tmp_dir()}/inventory.json"

    fp = open(ls_cmds_fp, mode="w")
    cmds = []
    for pl in patterns:
        cmd = f"ls {pl}\n"
        cmds.append(cmd)
    fp.writelines(cmds)
    fp.close()

    s5_cmd = f"s5cmd --json run {ls_cmds_fp} > {inventory_fp}"
    os.system(s5_cmd)
    return inventory_fp


def sync_inventory(df):
    # Deleting /raw dir where data will be synced
    base_path = f"{helpers.get_tmp_dir()}/raw"
    helpers.delete_dir(f"{base_path}/")
    local_path = f"{base_path}/" + df["engine_path"].map(
        lambda x: x.replace("s3://", "")
    )
    cmds = "cp" + " " + df["engine_path"].map(str) + " " + local_path
    cmds_fp = f"{helpers.get_tmp_dir()}/sync_commands.txt"

    cmds.to_csv(cmds_fp, header=False, index=False)
    s5_cmd = f"s5cmd run {cmds_fp}"

    os.system(s5_cmd)

    df["local_path"] = local_path
    local_inventory_file = f"{helpers.get_tmp_dir()}/local-inventory.csv"

    df.to_csv(local_inventory_file, header=True, index=False)
    return local_inventory_file
