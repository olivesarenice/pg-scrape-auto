import json
import requests
import urllib3
import concurrent.futures
import random
import time
import datetime
import os
import utils
from bs4 import BeautifulSoup
import argparse
from loguru import logger

# Configurations
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Functions
def delete_files_in_directory(directory_path):
    # Check if the directory exists
    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        # Iterate over the files in the directory
        n = 0
        for file_name in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file_name)
            try:
                # Check if it is a file (not a directory)
                if os.path.isfile(file_path) and not file_name.startswith("."):
                    # Remove the file
                    os.remove(file_path)
                    n += 1
                    logger.debug(f"Deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to delete file {file_path}. Error: {str(e)}")

        logger.info(f"Deleted {n} files")
    else:
        logger.error(f"Directory not found: {directory_path}")

def get_headers(path_to_har, bot_trigger_url):
    global config
    with open(path_to_har, "r", encoding="utf8") as har:
        logs = json.loads(har.read())
        entries = logs["log"]["entries"]
    target = bot_trigger_url
    for i, entry in enumerate(entries):
        if (
            entry["request"]["url"].startswith(target)
            and entry["request"]["method"] == "GET"
        ):
            logger.info(f"Valid GET request at entry index {i}")
            break
        else:
            raise ValueError("HAR header not found")

    headers_raw = entry["request"]["headers"]

    exclude_headers = [":authority", ":method", ":path", ":scheme"]
    headers = {}
    for header in headers_raw:
        if header["name"] not in exclude_headers:
            headers[header["name"]] = header["value"]
    return headers


def test_conn(headers, test_url):
    session = requests.Session()
    logger.info("Test headers on new URL")
    resp = session.get(test_url, headers=headers, verify=False)
    if resp.status_code == 200:
        print("VALID headers")
    else:
        logger.error("INVALID headers, exiting")
        exit(1)


def get_pages(headers, filter_url_params, pg_endpoint):
    url = f"{pg_endpoint}1?{filter_url_params}"
    r = requests.get(
        url,
        headers=headers,
        verify=False,
    )
    r.encoding='utf-8'
    if r.status_code == 200:

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        #print(r.text)
        # Find all elements with class 'pagination'
        pagination_elements = soup.find_all(class_="pagination")
        # Extract numbers from the 'pagination' elements and find the maximum value
        #print(pagination_elements)
        numbers = [
            int(link.get("data-page", 0))
            for element in pagination_elements
            for link in element.find_all("a")
            if link.get("data-page").isdigit()
        ]

        if numbers:
            max_number = max(numbers)
            logger.info(f"# pages: {max_number}")
            return max_number
        else:
            logger.error(f"No page number: {url}")
            exit(1)


def truncate_html(response):
    html = response.content
    # print(response.content)
    # Keep only the var guruApp and the 20 listings per html page
    soup = BeautifulSoup(html, "html.parser")
    summary_data = soup.find("script", text=lambda x: x and "var guruApp" in x)
    main_body = soup.find(id="listings-container")

    # Create a new html
    combined_soup = BeautifulSoup("", "html.parser")
    combined_soup.append(summary_data)
    combined_soup.append(main_body)
    return combined_soup.prettify()


def process_item(item):
    config = item["config"]
    headers = item["headers"]
    name = item["name"]
    page = item["page"]
    url = item["url"]
    # print(url)
    s1 = time.time()
    time.sleep(1 * random.random())
    s2 = time.time()
    g1 = time.time()
    response = requests.get(url, headers=headers, verify=False)
    g2 = time.time()

    if "Bot Protection" in response.text:
        raise ValueError(f"ERROR: Hit bot protection on PAGE <{page}> | URL <{url}>")
    # print(f"Processed item: {item}, Result: {response}")
    w1 = time.time()

    html_file_path = f"{config["data"]["html_dir"]}/{name}_{page}.html"
    with open(html_file_path, "w", encoding="utf-8") as file:
        file.write(truncate_html(response))
        # file.write(response.text)
    w2 = time.time()
    if page %10 == 0:
        logger.debug(f'{name}_{page} | SLEEP: {(s2 -s1) :.4f}s |GET: {(g2 -g1) :.4f}s | WRITE: {(w2 -w1) :.4f}s')
    return html_file_path


def parallel_process(items, num_workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        html_file_paths = list(executor.map(process_item, items))
        return html_file_paths


# def get_directory_size(directory_path, filter_url_name):
#     total_size = 0

#     for dirpath, dirnames, filenames in os.walk(directory_path):
#         for filename in filenames:
#             if filter_url_name in filename:
#                 file_path = os.path.join(dirpath, filename)
#                 total_size += os.path.getsize(file_path)

#     # Convert total_size to kilobytes (optional)
#     total_size_mb = total_size / (1024.0 * 1024)

#     return total_size_mb


# def log_run(dirpath, filter_url_name):
#     with open(
#         f"{dirpath}/0_{filter_url_name}_run_report.txt", "w", encoding="utf-8"
#     ) as file:
#         file.write(f"Start: {start_t} \n")
#         file.write(f"End: {datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')} \n")
#         file.write(f"Pages: {TOTAL_PAGES} \n")
#         file.write(f"Total Size (MB): {get_directory_size(dirpath,filter_url_name)}\n")
import zipfile
def create_zip_file(list_of_file_paths, zip_file_path):
    """Create a zip file containing the specified files.
    
    Args:
        list_of_file_paths (list of str): List of file paths to be zipped.
        zip_file_path (str): Path where the zip file will be saved.
    """
    os.makedirs(os.path.dirname(zip_file_path), exist_ok=True)
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file_path in list_of_file_paths:
            if os.path.isfile(file_path):
                zipf.write(file_path, os.path.basename(file_path))
            else:
                print(f"File {file_path} does not exist and will be skipped.")

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
def upload_to_s3(zip_file_path, bucket_name, s3_key, region_name):
    """Upload a file to an S3 bucket.
    
    Args:
        zip_file_path (str): Path to the file to be uploaded.
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): S3 object key for the uploaded file.
        aws_access_key_id (str, optional): AWS access key ID.
        aws_secret_access_key (str, optional): AWS secret access key.
        region_name (str, optional): AWS region name.
    """
    s3_client = boto3.client(
        's3',
        region_name=region_name
    )
    
    try:
        s3_client.upload_file(zip_file_path, bucket_name, s3_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def scrape(cmd_arg, config):
    ymdh = config["ymdh"]
    dirpath = config["data"]["html_dir"]

    try:
        os.makedirs(dirpath)
    except FileExistsError as e:
        logger.warning("Data directory already exists.")
    delete_files_in_directory(dirpath)

    har_dir = f'{config["scraper"]["har_dir"]}/{config["scraper"]["har_file"]}'
    headers = get_headers(har_dir, config["scraper"]["bot_trigger_url"])

    test_conn(headers, config["scraper"]["test_url"])

    all_file_paths = []
    ### Allows specific URL params
    for filter_config in config["filter_configs"]:
        if filter_config["enabled"]:
            filter_name = filter_config["name"]
            filter_params = filter_config["params"]
            TOTAL_PAGES = get_pages(headers, filter_params, config["scraper"]["pg_endpoint"])
            if cmd_arg.run_type == "test":
                if TOTAL_PAGES > 25:
                    TOTAL_PAGES = 25

            base_url = config["scraper"]["pg_endpoint"]
            url_list = [
                {   "config" : config,
                    "headers" : headers,
                    "name": filter_name,
                    "page": i,
                    "url": f"{base_url}{i}?{filter_params}/",
                }
                for i in range(1, TOTAL_PAGES + 1, 1)[::-1]
            ]

            # Perform parallel processing
            html_file_paths = parallel_process(
                url_list, cmd_arg.n_workers
            ) 
            all_file_paths.extend(html_file_paths)
    logger.info("Scrape complete.")
    return None
 

def upload(cmd_args, config):
    ymdh = config["ymdh"]
    all_file_paths = utils.get_file_paths_matching(f"{config['data']['html_dir']}", ".html")
       
    zip_fp = f"data/02_zipped/{ymdh["y"]}/{ymdh["m"]}/{ymdh["d"]}/htmls.zip"
    create_zip_file(all_file_paths, zip_fp)

    partition_key = f"02_zipped/{ymdh["y"]}/{ymdh["m"]}/{ymdh["d"]}/htmls.zip" 
    upload_to_s3(zip_fp, config["upload"]["s3_bucket"], partition_key, config["aws"]["region"])
    
    logger.info(f"Uploaded {zip_fp} as {partition_key}.")
    return None

if __name__ == "__main__":
    pass
