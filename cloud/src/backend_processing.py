import json
import os
import re
import zipfile
from tempfile import NamedTemporaryFile

import boto3
import pandas as pd
import tqdm
import transforms_config
from bs4 import BeautifulSoup
from loguru import logger


def parse_summary(html):

    summary_str = str(html)

    pattern = re.compile(r"var guruApp = ({.*?});", re.DOTALL)

    match = pattern.search(summary_str)

    if match:
        # Extract the JSON string
        json_str = match.group(1)

        # Convert the JSON string to a Python dictionary
        guruapp_dict = json.loads(json_str)
        summary_listings = guruapp_dict["listingResultsWidget"]["gaECListings"]
        # print(summary_listings)

        # Do custom unpacking
        keep_cols = {
            "id": "id",
            "name": "name",
            "category": "category",
            "project": "project",
            "dimension40": "is_new_project",
            "brand": "developer",
            "price": "price",
            "floorArea": "floor_area",
            "bedrooms": "bedrooms",
            "bathrooms": "bathrooms",
            "dimension23": "is_turbo_listing",
            "district": "district",
            "districtCode": "district_code",
            "region": "region",
            "regionCode": "region_code",
        }

        flat_data = []
        for item in summary_listings:
            datum = item.get("productData", {})
            # Use dictionary comprehension to extract desired keys, filling missing ones with None
            extracted_data = {
                col_name: datum.get(key) for key, col_name in keep_cols.items()
            }
            flat_data.append(extracted_data)
        raw_df = pd.DataFrame.from_dict(flat_data)

        # Grab the search metadata:
        search_type = guruapp_dict["listingSearch"]["searchParams"][
            "property_type_code"
        ]
        search_type_val = ",".join(search_type)
        raw_df["search_type"] = search_type_val
        return raw_df
    else:
        logger.warning("No match found")


def extract_value_from_element(column_name, element):
    try:
        match column_name:
            case "id":
                value = element.get("data-listing-id")
            case "url":
                value = element.select_one(".listing-description .nav-link")["href"]
            case "title":
                value = element.select_one(".listing-description h3 a").get("title")
            case "agent_id":
                value = element.select_one(".headline").find_all("a")[1][
                    "data-agent-id"
                ]
            case "address":
                value = element.select_one(
                    ".listing-description .listing-location span"
                ).text.strip()
            case "agent_name":
                value = element.select_one(".agent-name .name").text.strip()

            case "headline":
                value = element.select_one(".headline").find_all("div")[3].text.strip()
            case "proximity_mrt":
                value = element.select_one(
                    ".listing-description .pgicon-walk"
                ).next_sibling.strip()
            case "recency":
                value = element.select_one(
                    ".listing-description .listing-recency"
                ).text.strip()
        return value
    except:
        return None


def parse_details(html):
    elements = html.find_all(class_="listing-card")

    # Enforce typing
    extract_columns = {
        "id": "int64",
        "url": "object",
        "title": "object",
        "address": "object",
        "agent_id": "object",
        "agent_name": "object",
        "headline": "object",
        "proximity_mrt": "object",
        "recency": "object",
    }

    listing_data = []
    for i, element in enumerate(elements):
        # print(f"Listing: {i}")
        listing_datum = {}
        for column_name in extract_columns.keys():
            column_val = extract_value_from_element(column_name, element)
            listing_datum[column_name] = column_val
        listing_data.append(listing_datum)
        # print(listing_data)
    listing_df = pd.DataFrame.from_dict(listing_data).astype(extract_columns)
    return listing_df


def process_file(html_fp):

    with open(html_fp, "r", encoding="utf-8") as file_data:
        html = file_data.read()

    soup = BeautifulSoup(html, "html.parser")

    summary_data = soup.find("script", string=lambda x: x and "var guruApp" in x)
    summary_df = parse_summary(summary_data)
    details_data = soup.find(id="listings-container")
    details_df = parse_details(details_data)
    # print(summary_df.dtypes)
    # print(details_df.dtypes)
    joined_df = pd.merge(
        summary_df,
        details_df,
        how="left",
        on="id",
    )

    # print(joined_df)

    # print(df)
    # joined_df.to_csv(f"data/tmp/{file_name}_joined.csv", index=False)
    return joined_df


def download_htmls(bucket_name, s3_prefix, dest_dir):
    s3_client = boto3.client("s3")

    # Create local directory if it doesn't exist
    if not os.path.exists(dest_dir):
        os.makedirs(local_dir)

    # List objects in the specified S3 prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

    if "Contents" not in response:
        raise ValueError("No files found with the given prefix.")

    file_paths = []

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".zip"):
            # Download the zip file
            with NamedTemporaryFile(delete=False) as temp_file:
                s3_client.download_fileobj(bucket_name, key, temp_file)
                temp_file_path = temp_file.name

            # Unzip the file
            with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
                zip_ref.extractall(dest_dir)
                for file_info in zip_ref.infolist():
                    file_path = os.path.join(dest_dir, file_info.filename)
                    file_paths.append(file_path)

            # Clean up temporary zip file
            os.remove(temp_file_path)

    return file_paths


def get_file_paths_matching(directory, ext=""):
    file_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(ext):
                file_path = os.path.join(root, file)
                file_paths.append(file_path)
    return file_paths


def format_dataframe(
    df: pd.DataFrame,
    convert_schema: dict,
    rename_schema: dict = None,
    varchar_limit: dict = None,
) -> pd.DataFrame:
    if rename_schema:
        df.rename(columns=rename_schema, inplace=True)

    # Handle embedding columns separately
    convert_schema_embed = [k for k, v in convert_schema.items() if v == "array<float>"]
    convert_schema_regular = {
        k: v for k, v in convert_schema.items() if v != "array<float>"
    }

    df1 = df.astype(convert_schema_regular)

    if varchar_limit:
        for k, v in varchar_limit.items():
            # trim row that exceeds redshift octet length
            df1[k] = df1[k].apply(
                lambda x: (
                    x.encode("utf-8")[:v].decode("utf-8")
                    if isinstance(x, str) and x
                    else x
                )
            )
    df2 = df1.astype(convert_schema_regular)

    for c in convert_schema_embed:
        df2[c] = df2[c].apply(lambda x: np.array(x, dtype=np.float32))

    # If we have more columns than specified, remove them
    df2 = df2[[c for c in df2.columns if c in convert_schema.keys()]]

    return df2


def run_process_listings(file_paths):

    dfs = []
    for html_fp in tqdm.tqdm(file_paths):
        dfs.append(process_file(html_fp))
    df_concat = pd.concat(dfs)

    return df_concat


def run_transform_df(source_df, transform_config):
    # Run the transformation chain
    df = source_df.copy()
    for transform_step in transform_config.transformations:
        logger.info(
            f"[!] Applying transformation: {transform_step.transform_fn.__name__}"
        )

        logger.info(f"Input df shape: {df.shape}")
        df = transform_step.transform_fn(
            df,
            transform_step.input_cols,
            transform_step.output_cols,
        )
        logger.info(f"Output df shape: {df.shape}")

    convert_schema = transform_config.dest_parquet
    rename_schema = None
    varchar_limit = None

    df = format_dataframe(df, convert_schema, rename_schema, varchar_limit)
    return df


def retrieve_transform_config(
    table_name: str,
) -> transforms_config.TransformConfig:
    transform_config = getattr(transforms_config, f"{table_name}_config")
    return transform_config


def run(cmd_arg, config):

    bucket_name = config["s3_bucket"]
    y = config["ymdh"]["y"]
    m = config["ymdh"]["m"]
    d = config["ymdh"]["d"]
    bucket_name = config["s3_bucket"]
    # Download raw and compile
    s3_prefix = f"{config["raw_zip_s3_prefix"]}/{y}/{m}/{d}"
    logger.info(f"Raw data from: {s3_prefix}")
    if not cmd_arg.no_download:
        download_htmls(bucket_name, s3_prefix, "data/htmls")
    file_paths = get_file_paths_matching("data/htmls", ".html")
    df_fp = "data/raw/raw_df.csv"
    if not cmd_arg.no_recompile:
        df_concat = run_process_listings(file_paths)
        df_concat.to_csv(df_fp, index=False)
    else:
        df_concat = pd.read_csv(df_fp)

    # Transform and save
    df_transform = run_transform_df(df_concat, retrieve_transform_config("listings"))
    parquet_fp = f"data/transformed/df_concat.parquet"
    df_transform.to_parquet(parquet_fp, index=False)

    # Upload
    s3_client = boto3.client("s3")
    s3_key = f"{config["transformed_s3_prefix"]}/{y}/{m}/{d}/df.parquet"
    s3_client.upload_file(parquet_fp, bucket_name, s3_key)
    logger.info(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")

    # Run analysis
