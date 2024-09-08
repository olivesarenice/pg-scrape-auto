import datetime
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
    # joined_df.to_csv(f"tmp/data/tmp/{file_name}_joined.csv", index=False)
    return joined_df

def download_htmls(bucket_name, s3_prefix, dest_dir):
    s3_client = boto3.client("s3")

    # List objects in the specified S3 prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    
    if "Contents" not in response:
        raise ValueError("No files found with the given prefix.")

    # Delete the files in the local dir
    n_removed = 0
    for file_name in os.listdir(dest_dir):
        file_path = os.path.join(dest_dir, file_name)
        if os.path.isfile(file_path) and not file_name.startswith("."):
            os.remove(file_path)
            n_removed += 1
    logger.info(f"Emptied {n_removed} files from {dest_dir}")
    
    # Saving files
    file_paths = []
    logger.info("Unzipping files")
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

    # Handle custom formatting separately
    convert_schema_int_string = [k for k, v in convert_schema.items() if v == "int_string"]
    convert_schema_regular = {
        k: v for k, v in convert_schema.items() if v != "int_string"
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

    for c in convert_schema_int_string:
        df2[c] = df2[c].astype('Int64').astype('str')

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

from google.cloud import storage


def upload_to_gcs(blob_name, path_to_file, bucket_name, creds_fp):
    """ Upload data to a bucket"""
     
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        creds_fp)

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    
    #returns a public url
    return blob.public_url

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def delete_bq_partition(dataset_id, table_id, partition_date, creds_fp):
    # Initialize a BigQuery client
    client = bigquery.Client.from_service_account_json(
    creds_fp)
    try:
        client.get_table(f"{dataset_id}.{table_id}")
    except NotFound:
        logger.warning("Table does not exist")
        return False
    
    query = f"""
    SELECT COUNT(*) as row_count FROM `{dataset_id}.{table_id}`
    WHERE partition_ts = '{partition_date}'
    """

    # Run the query
    query_job = client.query(query)
    row = next(query_job.result())  # Since there's only one row expected
    logger.info(f"Deleting {row['row_count']} rows in partition {partition_date}...")

    # Construct the SQL DELETE query
    query = f"""
    DELETE FROM `{dataset_id}.{table_id}`
    WHERE partition_ts = '{partition_date}'
    """

    # Run the query
    query_job = client.query(query)

    # Wait for the job to complete
    query_job.result()

    logger.info(f"Deleted rows for {partition_date} from {dataset_id}.{table_id}")

def load_bq_schema(schema_map, pk):
    schema = []
    for field_name, field_type in schema_map.items():
        field_type = schema_map.get(field_name) 
        if field_name in pk:
            mode = "REQUIRED"
        else:
            mode = "NULLABLE"
        schema.append(bigquery.SchemaField(field_name, field_type, mode=mode))
    logger.debug(schema)
    return schema

def copy_gcs_to_bq(table_id, uri, schema,creds_fp):
    client = bigquery.Client.from_service_account_json(
    creds_fp)
    #table_id = 'your_project.your_dataset.your_table'
    #uri = 'gs://your-bucket-name/file.parquet'
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    logger.info(f"Loaded {load_job.output_rows} rows into {table_id}.")

def run(cmd_arg, config):

    if cmd_arg.is_local:
        absolute_path = ""
    else:
        absolute_path = "/"
    
    bucket_name = config["s3_bucket"]
    y = config["ymdh"]["y"]
    m = config["ymdh"]["m"]
    d = config["ymdh"]["d"]
    partition_date = f"{y}-{m}-{d}"
    bucket_name = config["s3_bucket"]
    # Download raw and compile
    s3_prefix = f"{config["raw_zip_s3_prefix"]}/{y}/{m}/{d}"
    logger.info(f"Raw data from: {s3_prefix}")
    if not cmd_arg.no_download:
        logger.info("Re-downloading HTMLs")
        download_htmls(bucket_name, s3_prefix, f"{absolute_path}tmp/data/htmls")
    file_paths = get_file_paths_matching(f"{absolute_path}tmp/data/htmls", ".html")
    df_fp = f"{absolute_path}tmp/data/raw/raw_df.parquet"
    if not cmd_arg.no_recompile:
        logger.info("Re-compiling HTMLs into single file")
        df_concat = run_process_listings(file_paths)
        df_concat['partition_ts'] = datetime.datetime.strptime(partition_date, "%Y-%m-%d")
        df_concat.to_parquet(df_fp, index=False)
    else:
        logger.info("Reading from existing compiled file")
        df_concat = pd.read_parquet(df_fp)

    # Transform and save
    transform_config = retrieve_transform_config("listings")
    df_transform = run_transform_df(df_concat, transform_config)
    parquet_fp = f"{absolute_path}tmp/data/transformed/df_concat.parquet"
    df_transform.to_parquet(parquet_fp, index=False)
    logger.debug(df_transform.columns)

    # Upload
    s3_client = boto3.client("s3")
    s3_key = f"{config["transformed_s3_prefix"]}/{y}/{m}/{d}/df.parquet"
    s3_client.upload_file(parquet_fp, bucket_name, s3_key)
    logger.info(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")

    # Push to BigQuery
    
    
    # Need to create BQ dataset, GCS bucket, and Service Account for GCS + BQ, download gcs_credentials.json 
    ## First need to upload to a tmp/ folder in GCS
    
    upload_to_gcs(blob_name = s3_key,path_to_file=parquet_fp,bucket_name=config["gcs_bucket"],creds_fp = config["gcs_sa_creds"],)

    bq_schema = load_bq_schema(transform_config.dest_bq, transform_config.primary_key)

    dataset = 'pg_listings'
    table = 'listings_raw'
    
    delete_bq_partition(dataset, table, partition_date, creds_fp = config["gcs_sa_creds"])
    copy_gcs_to_bq(table_id = "propguru.pg_listings.listings_raw", uri = f"gs://{config["gcs_bucket"]}/{s3_key}", schema = bq_schema, creds_fp = config["gcs_sa_creds"])
    ## Then call a function to copy over to BQ


    # gcs_bucket = 'pg-scrape-auto-tmp'

