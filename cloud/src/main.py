from __future__ import annotations

import argparse
import ast
import datetime
import json
import os
import shutil
import sys
import uuid

import analyses_config
import backend_analysis
import backend_processing
import boto3
import transforms
import transforms_config
import yaml
from dotenv import load_dotenv
from loguru import logger


def init_config() -> dict:
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    logger.info(config)
    return config


# class S3Handler:
#     def __init__(self, bucket_name, log_file_path):
#         self.bucket_name = bucket_name
#         self.log_file_path = log_file_path
#         self.s3_client = boto3.client("s3")

#     def write(self, message):
#         self.s3_client.put_object(
#             Bucket=self.bucket_name, Key=self.log_file_path, Body=message
#         )


def init_logger(config, cmd_arg) -> None:
    log_level = config["log_level"]
    start_runtime = datetime.datetime.now(datetime.timezone.utc)
    logfile_name = f"{start_runtime.strftime('%Y%m%d_%H%M%S')}.log"
    year = start_runtime.strftime("%Y")
    month = start_runtime.strftime("%m")
    day = start_runtime.strftime("%d")

    local_logfile = os.path.join(
        config["local_log_dir"],
        cmd_arg.step,
        "year=" + year,
        "month=" + month,
        "day=" + day,
        logfile_name,
    )
    # Create a logger instance
    logger.remove()  # Remove default handler
    # logger.add(lambda msg: S3Handler(config['s3_log_bucket'], logfile).write(msg), format="{message}", level=log_level)
    logger.add(sys.stdout, format="{time} | {level} | {message} ", level=log_level)
    if cmd_arg.is_local:
        logger.add(
            local_logfile,
            format="{time} | {level} | {message}",
            level=log_level,
            rotation="50 MB",
        )
    # You can set the global logger if needed, but it's better to return or use the instance directly
    return logger


def parse_cmd_arguments() -> argparse.Namespace:
    desc = """
    placeholders
    """
    parser = argparse.ArgumentParser(desc)
    parser.add_argument(
        "-is_local",
        action="store_true",
        default=False,
        dest="is_local",
        help="By default, assumes that script is run in Lambda and uses Lambda file pathing",
    )
    parser.add_argument(
        "-no_download",
        action="store_true",
        default=False,
        dest="no_download",
        help="If specified, disables downloading.",
    )

    # Define a boolean flag for recompile
    parser.add_argument(
        "-no_recompile",
        action="store_true",
        default=False,
        dest="no_recompile",
        help="If specified, disables recompiling.",
    )
    parser.add_argument(
        "-t",
        action="store",
        type=str,
        dest="t",
        help="Date in YYYY-MM-DD UTC to run for. Will look at the partition for this date.",
    ),
    parser.add_argument(
        "-t1",
        action="store",
        type=str,
        dest="t1",
        help="Only for analysis, if intending to bulk run analysis across t > t1 dates inclusive",
    ),
    parser.add_argument(
        "-step",
        action="store",
        type=str,
        choices=["transform", "analysis"],
        dest="step",
        help="Step to run",
    )
    known_arg, unknown_arg = parser.parse_known_args()
    logger.info(known_arg)
    return known_arg


def ts_to_ymdh(ts):
    if not ts:
        ts = datetime.datetime.now(datetime.UTC)
    ymdh = {}
    ymdh["y"] = ts.strftime("%Y")
    ymdh["m"] = ts.strftime("%m")
    ymdh["d"] = ts.strftime("%d")
    ymdh["h"] = ts.strftime("%H")

    return ymdh


def parse_lambda_arguments(event):
    # Example function to parse arguments from the event
    class Args:
        def __init__(
            self,
            step=None,
            t=None,
            t1=None,
            is_local=False,
            no_download=False,
            no_recompile=False,
        ):
            self.step = step
            self.t = t
            self.t1 = t1
            self.is_local = is_local
            self.no_download = no_download
            self.no_recompile = no_recompile

    return Args(
        step=event.get("step"),
        t=event.get("t"),
        t1=event.get("t1"),
        is_local=False,
        no_download=False,
        no_recompile=False,
    )


def lambda_handler(event, context):

    # Directory path
    dir_paths = [
        "/tmp/data/htmls",
        "/tmp/data/raw",
        "/tmp/data/transformed",
    ]

    # Create the directory if it doesn't exist
    for dir_path in dir_paths:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)

    cmd_arg = parse_lambda_arguments(event)
    if cmd_arg.t:
        cmd_arg.t = datetime.datetime.strptime(cmd_arg.t, "%Y-%m-%d")
    else:
        cmd_arg.t = datetime.datetime.now(datetime.timezone.utc)

    if cmd_arg.t1:
        cmd_arg.t1 = datetime.datetime.strptime(
            cmd_arg.t1, "%Y-%m-%d"
        )  # t1 is not used for transform

    config = init_config()
    init_logger(config, cmd_arg)
    logger.info(f"Running main.py [{cmd_arg.step}] in directory: {os.getcwd()} ")
    ymdh = ts_to_ymdh(cmd_arg.t)
    config["ymdh"] = ymdh

    if cmd_arg.step == "transform":
        backend_processing.run(cmd_arg, config)
    elif cmd_arg.step == "analysis":
        backend_analysis.run(cmd_arg, config)

    logger.info("[main] EXITING.")

    return {"statusCode": 200, "body": json.dumps({"message": "Execution completed"})}


# USAGE FOR TESTING:
# python src/main.py -step transform -t 2024-09-04 -no_download -no_recompile
if __name__ == "__main__":

    cmd_arg = parse_cmd_arguments()
    if cmd_arg.t:
        cmd_arg.t = datetime.datetime.strptime(cmd_arg.t, "%Y-%m-%d")
    else:
        cmd_arg.t = datetime.datetime.now(datetime.UTC)

    if cmd_arg.t1:
        cmd_arg.t1 = datetime.datetime.strptime(
            cmd_arg.t1, "%Y-%m-%d"
        )  # t1 is not used for transform

    config = init_config()
    init_logger(config, cmd_arg)
    logger.info(f"Running main.py [{cmd_arg.step}] in directory: {os.getcwd()} ")
    ymdh = ts_to_ymdh(cmd_arg.t)
    config["ymdh"] = ymdh

    if cmd_arg.step == "transform":
        backend_processing.run(cmd_arg, config)
    if cmd_arg.step == "analysis":
        backend_analysis.run(cmd_arg, config)
    logger.info("[main] EXITING.")
