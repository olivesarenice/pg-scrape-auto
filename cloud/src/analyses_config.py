from dataclasses import dataclass
from typing import Optional


@dataclass
class AnalysisConfig:
    dest_table: str
    dest_bq: dict
    primary_key: Optional[list] = None


agg_stats_filter_group_config = AnalysisConfig(
    dest_table="agg_stats_filter_group",
    dest_bq={
        "partition_ts": "TIMESTAMP",
        "filter_code": "STRING",
        "filter_description": "STRING",
        "day_active_listings": "INTEGER",  # Change to "FLOAT64" if it's a float
        "day_new_listings": "INTEGER",  # Change to "FLOAT64" if it's a float
        "day_removed_listings": "INTEGER",  # Change to "FLOAT64" if it's a float
        "wtd_active_listings": "INTEGER",  # Change to "FLOAT64" if it's a float
        "lifespan_1d": "FLOAT64",  # Change to "FLOAT64" if it's a float
        "lifespan_7d": "FLOAT64",  # Change to "FLOAT64" if it's a float
        "day_median_psf": "FLOAT64",
        "day_25_psf": "FLOAT64",
        "day_75_psf": "FLOAT64",
        "day_median_q": "FLOAT64",
        "day_25_q": "FLOAT64",
        "day_75_q": "FLOAT64",
    },
    primary_key=["filter_code", "partition_ts"],
)


SQL_AGG_STATISTICS_FILTER_GROUP = """
WITH rel_records as (
    SELECT * FROM pg_listings.listings_raw {filter_clause}), 
ytd_records as (
    SELECT * FROM rel_records WHERE DATE(partition_ts) = (SELECT DATETIME_SUB('{partition_date}', INTERVAL 1 DAY))
),
day_active_listings as 
    (SELECT COUNT(*) as day_active_listings 
    FROM rel_records 
    WHERE partition_ts = '{partition_date}'),
day_new_listings as (SELECT COUNT(*) as day_new_listings FROM rel_records where id not in (SELECT id from rel_records where DATE(partition_ts) = (SELECT DATETIME_SUB('{partition_date}', INTERVAL 1 DAY))) and partition_ts = '{partition_date}'),
day_removed_listings as (SELECT COUNT(*) as day_removed_listings FROM rel_records where id not in (SELECT id from rel_records where partition_ts = '{partition_date}') and DATE(partition_ts) = (SELECT DATETIME_SUB('{partition_date}', INTERVAL 1 DAY))),
wtd_active_listings as (SELECT COUNT(distinct id) as wtd_active_listings FROM rel_records WHERE DATE(partition_ts) > (SELECT DATETIME_SUB('{partition_date}', INTERVAL 1 DAY)) and partition_ts <= '{partition_date}' ),
now as (SELECT id from rel_records WHERE partition_ts = '{partition_date}'),
past_1d as (SELECT id from rel_records WHERE DATE(partition_ts) = (SELECT DATETIME_SUB('{partition_date}', INTERVAL 1 DAY))),
past_7d as (SELECT id from rel_records WHERE DATE(partition_ts) = (SELECT DATETIME_SUB('{partition_date}', INTERVAL 7 DAY))),
lifespan_1d as (
  SELECT
      SAFE_DIVIDE(COUNT(p.id) * 1.0 ,(SELECT COUNT(*) FROM now)) AS lifespan_1d
  FROM
      past_1d p
  JOIN
      now n
  ON
      p.id = n.id
),
lifespan_7d as (
  SELECT
      SAFE_DIVIDE(COUNT(p.id) * 1.0 ,(SELECT COUNT(*) FROM now)) AS lifespan_7d
  FROM
      past_7d p
  JOIN
      now n
  ON
      p.id = n.id
),
day_median_psf as (SELECT PERCENTILE_CONT(psf, 0.5)  OVER() as day_median_psf FROM ytd_records LIMIT 1),
day_25_psf as (SELECT PERCENTILE_CONT(psf, 0.25)  OVER() as day_25_psf FROM ytd_records LIMIT 1),
day_75_psf as (SELECT PERCENTILE_CONT(psf, 0.75) OVER() as day_75_psf FROM ytd_records LIMIT 1),
day_median_q as (SELECT PERCENTILE_CONT(price, 0.5)  OVER() as day_median_q FROM ytd_records LIMIT 1),
day_25_q as (SELECT PERCENTILE_CONT(price, 0.25)  OVER() as day_25_q FROM ytd_records LIMIT 1),
day_75_q as (SELECT PERCENTILE_CONT(price, 0.75) OVER() as day_75_q FROM ytd_records LIMIT 1)
SELECT 
    CAST('{partition_date}' AS TIMESTAMP) as partition_ts,
    '{filter_code}' as filter_code,
    '{filter_description}' as filter_description,
    (SELECT day_active_listings.day_active_listings FROM day_active_listings LIMIT 1) AS day_active_listings,
    (SELECT day_new_listings.day_new_listings FROM day_new_listings LIMIT 1) AS day_new_listings,
    (SELECT day_removed_listings.day_removed_listings FROM day_removed_listings LIMIT 1) AS day_removed_listings,
    (SELECT wtd_active_listings.wtd_active_listings FROM wtd_active_listings LIMIT 1) AS wtd_active_listings,
    (SELECT lifespan_1d.lifespan_1d FROM lifespan_1d LIMIT 1) AS lifespan_1d,
    (SELECT lifespan_7d.lifespan_7d FROM lifespan_7d LIMIT 1) AS lifespan_7d,
    (SELECT day_median_psf.day_median_psf FROM day_median_psf LIMIT 1) AS day_median_psf,
    (SELECT day_25_psf.day_25_psf FROM day_25_psf LIMIT 1) AS day_25_psf,
    (SELECT day_75_psf.day_75_psf FROM day_75_psf LIMIT 1) AS day_75_psf,
    (SELECT day_median_q.day_median_q FROM day_median_q LIMIT 1) AS day_median_q,
    (SELECT day_25_q.day_25_q FROM day_25_q LIMIT 1) AS day_25_q,
    (SELECT day_75_q.day_75_q FROM day_75_q LIMIT 1) AS day_75_q

"""
