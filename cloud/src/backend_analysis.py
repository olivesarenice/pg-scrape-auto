import boto3
import pandas as pd
from loguru import logger
import itertools
from pandasql import sqldf
def generate_analysis(df):

# APPROACH:: 
# will need to put data in bigquery and run multiple queries per row required,. tooo may aggregations to do.``
# for each segment in:
# - property_type
# - district_code
# calculate these metrics:
# - daily listing counts (compared to ytd)
# > less than 1 week old
# > less than 1 month old
# > all existing
# > new
# > removed

# - past N days statistics(n = 1 day, 1 week, 1 month)
# > median psf
# > 25th and 75th pctile psf
# > median price
# > 25th and 75th pctile price
    metrics = {}
    groups = ['property_type',
                    'district_code',]

    records = df
    # Extract unique values for each column
    #unique_values = {col: df[col].unique() for col in groups}
    # Compute Cartesian product of unique values
    #combinations = list(itertools.product(*unique_values.values()))
    # Create a DataFrame from the combinations
    #index = pd.DataFrame(combinations, columns=groups)
    q = """SELECT 
    partition_ts, 
    property_type, 
    district_code,
    count(*) as listings_all,
    median(psf) as psf_median_1d,
    median(price) as median_price_1d
    FROM records
    GROUP BY partition_ts, property_type, district_code
    """
    print(sqldf(q, locals()))
    a_df = records.head()
    return a_df

def run(cmd_arg, config):

    bucket_name = config["s3_bucket"]
    y = config["ymdh"]["y"]
    m = config["ymdh"]["m"]
    d = config["ymdh"]["d"]
    bucket_name = config["s3_bucket"]
    # Download transformed data
    s3_client = boto3.client("s3")
    s3_key = f"{config["transformed_s3_prefix"]}/{y}/{m}/{d}/df.parquet"
    df_fp = "data/transformed/df.parquet"
    #s3_client.download_file(bucket_name, s3_key, df_fp)

    # Create analysis tables
    ###
    df = pd.read_parquet(df_fp,"pyarrow")
    analysis_df = generate_analysis(df)

    # Save
    analysis_fp = f"data/analysis/df_local.parquet"
    analysis_df.to_parquet(analysis_fp, index=False)

    # Upload
    s3_client = boto3.client("s3")
    s3_key = f"{config["analysis_s3_prefix"]}/{y}/{m}/{d}/df.parquet"
    #s3_client.upload_file(analysis_fp, bucket_name, s3_key)
    logger.info(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")
