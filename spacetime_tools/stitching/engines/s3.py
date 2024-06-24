import os
import pandas as pd
import logging
import spacetime_tools.stitching.helpers as helpers

logger = logging.getLogger(__name__)


def create_inventory(pattern_list, engine_opts):
    # Creating commands.txt file for s5cmd
    cmds_fp = f"{os.getcwd()}/.tmp/ls_commands.txt"
    inventory_fp = f"{os.getcwd()}/.tmp/inventory.tsv"
    fp = open(cmds_fp, mode="w")
    cmds = []
    for pl in pattern_list:
        cmd = f"ls {pl}\n"
        cmds.append(cmd)
    fp.writelines(cmds)
    fp.close()

    s5_cmd = f"AWS_REGION={engine_opts['region']} s5cmd run {cmds_fp} > {inventory_fp}"
    os.system(s5_cmd)
    return inventory_fp


def sync_inventory(pattern_list, engine_opts):
    # Deleting /raw dir where data will be synced
    helpers.delete_dir(f"{os.getcwd()}/.tmp/raw/")

    cmds_fp = f"{os.getcwd()}/.tmp/sync_commands.txt"
    fp = open(cmds_fp, mode="w")
    cmds = []
    for pl in pattern_list:
        cmd = f"sync {pl} {os.getcwd()}/.tmp/raw/\n"
        cmds.append(cmd)
    fp.writelines(cmds)
    fp.close()

    logger.info(f"Will sync {len(cmds)} patterns")
    s5_cmd = f"AWS_REGION={engine_opts['region']} s5cmd run {cmds_fp}"

    os.system(s5_cmd)
