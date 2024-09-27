from __future__ import annotations

import argparse
import ast
import datetime
import os
import sys
import uuid

import backend_download_html
import backend_generate_headers
import utils
import yaml
from dotenv import load_dotenv
from loguru import logger


def init_config() -> dict:
    with open("src/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    return config


# class S3Handler:
#     def __init__(self, bucket_name, log_file_path):
#         self.bucket_name = bucket_name
#         self.log_file_path = log_file_path
#         self.s3_client = boto3.client('s3')

#     def write(self, message):
#         self.s3_client.put_object(
#             Bucket=self.bucket_name,
#             Key=self.log_file_path,
#             Body=message
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
        "-step",
        dest="step",
        action="store",
        choices=["generate_headers", "download_html", "upload"],
        required=True,
    )
    parser.add_argument(
        "-n_workers",
        dest="n_workers",
        type=int,
        default=8,
        help="""
        Number of workers to run the scrape in parallel. Default=8.
        """,
    )

    known_arg, unknown_arg = parser.parse_known_args()
    return known_arg


if __name__ == "__main__":

    load_dotenv()
    cmd_arg = parse_cmd_arguments()
    config = init_config()
    init_logger(config, cmd_arg)
    logger.info(f"Running main.py [{cmd_arg.step}] in directory: {os.getcwd()} ")
    ymdh = utils.ts_to_ymdh(None)
    config["ymdh"] = ymdh
    match cmd_arg.step:
        case "generate_headers":
            backend_generate_headers.main(cmd_arg, config)
            pass
        case "download_html":
            backend_download_html.scrape(cmd_arg, config)
            pass
        case "upload":
            backend_download_html.upload(cmd_arg, config)
            pass
