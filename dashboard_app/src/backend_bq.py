import json

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger

data_dir = "./data/init"


def bq_execute_query(sql, dataset_id, table_id, creds_fp="creds/gcp_sa.json"):
    # Initialize a BigQuery client
    client = bigquery.Client.from_service_account_json(creds_fp)
    try:
        client.get_table(f"{dataset_id}.{table_id}")
    except NotFound:
        logger.warning("Table does not exist")
        return False

    # Run the query
    query_job = client.query(sql)
    res = query_job.result()
    df = res.to_dataframe()
    # logger.info(f"Retrieved {len(df)} rows")
    # print(df)
    return df


def get_metadata(md_fp=f"{data_dir}/metadata.json"):
    with open(md_fp, "r") as json_file:
        data = json.load(json_file)
    return data


def load_data(N_DAYS, is_all_region: bool) -> pd.DataFrame:
    # sql_format = backend_bq.SQL_BASIC_METRICS.format(N_DAYS=N_DAYS)
    # df_r = backend_bq.bq_execute_query(
    #     sql_format,
    #     dataset_id="pg_listings",
    #     table_id="agg_viz_group",
    # )
    md = get_metadata()
    dt = md["dt"]
    fp = f"{data_dir}/{dt}_{N_DAYS}.parquet"
    df_r = pd.read_parquet(fp)
    if is_all_region:
        df_r = df_r[df_r["region"] == "ALL"]
    else:
        df_r = df_r[df_r["region"] != "ALL"]
    return df_r


SQL_MAX_DT = """
SELECT FORMAT_DATE('%Y-%m-%d',MAX(dt)) AS max_dt FROM pg_listings.agg_viz_group"""

SQL_BASIC_METRICS = """WITH psf_history AS (
    SELECT
        dt,
        region,
        viz_group_code,
        ARRAY_AGG(STRUCT(FORMAT_DATE('%Y-%m-%d', dt) AS time, median_q as value)) OVER (PARTITION BY region, viz_group_code ORDER BY dt ASC) AS median_q_history,
        ARRAY_AGG(STRUCT(FORMAT_DATE('%Y-%m-%d', dt) AS time, median_psf as value)) OVER (PARTITION BY region, viz_group_code ORDER BY dt ASC) AS median_psf_history,
        ARRAY_AGG(STRUCT(FORMAT_DATE('%Y-%m-%d', dt) AS time, listings as value)) OVER (PARTITION BY region, viz_group_code ORDER BY dt ASC) AS listings_history
    FROM
        pg_listings.agg_viz_group
    WHERE dt > DATE_SUB((SELECT MAX(dt) FROM pg_listings.agg_viz_group), INTERVAL {N_DAYS} DAY)
),
psf_history_results AS (
select * from psf_history
WHERE dt = (SELECT MAX(dt) FROM pg_listings.agg_viz_group)
),
medians AS (
SELECT rfg.description ,m.* FROM (
    SELECT 
        dt,
        region,
        viz_group_code,
        median_q,
        (median_q / LAG(median_q, {N_DAYS}) OVER (PARTITION BY region, viz_group_code ORDER BY dt)) -1 AS q_pct_delta,
        median_psf,
        (median_psf / LAG(median_psf, {N_DAYS}) OVER (PARTITION BY region, viz_group_code ORDER BY dt)) -1 AS psf_pct_delta,
        listings,
        (listings / LAG(listings, {N_DAYS}) OVER (PARTITION BY region, viz_group_code ORDER BY dt)) -1 AS listings_pct_delta,
        median_lifetime
    FROM 
        propguru.pg_listings.agg_viz_group
) as m
LEFT JOIN pg_listings.ref_viz_groups as rfg
ON m.viz_group_code = rfg.code
WHERE 
    dt = (SELECT MAX(dt) FROM pg_listings.agg_viz_group))
SELECT 
r.region_code,
med.*, 
h.median_psf_history, 
h.listings_history ,
h.median_q_history
FROM medians med
LEFT JOIN psf_history_results h
ON med.dt = h.dt
JOIN pg_listings.ref_regioncode_region r
ON r.region = med.region
AND med.region = h.region
AND med.viz_group_code = h.viz_group_code
ORDER BY viz_group_code ASC, region_code ASC
"""
