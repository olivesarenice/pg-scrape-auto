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
        "dt": "DATE",
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
    CAST('{partition_date}' AS DATE) as dt,
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


agg_viz_group_config = AnalysisConfig(
    dest_table="agg_viz_group",
    dest_bq={
        "partition_ts": "TIMESTAMP",
        "dt": "DATE",
        "region": "STRING",
        "viz_group_code": "STRING",
        "listings": "INTEGER",
        "median_psf": "FLOAT64",
        "p25_psf": "FLOAT64",
        "p75_psf": "FLOAT64",
        "median_lifetime": "FLOAT64",
        "p25_lifetime": "FLOAT64",
        "p75_lifetime": "FLOAT64",
    },
    primary_key=["dt", "region", "viz_group_code"],
)


SQL_AGG_VIZ_GROUP = """

WITH w_labels AS (
  SELECT
    lr.id,
    lr.partition_ts,
    lr.psf,
    COALESCE(lr.region,dr.region) AS region,    
    CASE 
        WHEN property_type = '1/2 ROOM HDB' THEN 'AH2'
        WHEN property_type = '3 ROOM HDB' THEN 'AH3'
        WHEN property_type = '4 ROOM HDB' THEN 'AH4'
        WHEN property_type = '5 ROOM HDB' THEN 'AH5'
        WHEN property_type = 'OTHER HDB' THEN 'AH0'
        
        WHEN property_segment = 'Non-Landed' AND bedrooms <= 1 THEN 'AN1'
        WHEN property_segment = 'Non-Landed' AND bedrooms = 2 THEN 'AN2'
        WHEN property_segment = 'Non-Landed' AND bedrooms = 3 THEN 'AN3'
        WHEN property_segment = 'Non-Landed' AND bedrooms = 4 THEN 'AN4'
        WHEN property_segment = 'Non-Landed' AND bedrooms >= 5 THEN 'AN5'

        WHEN property_type = 'Terraced House' THEN 'AL1'
        WHEN property_type IN ('Semi-Detached House', 'Corner Terrace') THEN 'AL2'
        WHEN property_type IN ('Detached House', 'Good Class Bungalow', 'Bungalow House') THEN 'AL3'
        
        ELSE 'NA' -- Default case for any unmatched criteria
    END AS viz_group_code
FROM
    pg_listings.listings_raw lr
LEFT JOIN
    pg_listings.ref_districtcode_region dr
ON lr.district_code = dr.district_code
),
r_lifetime_id AS (
    SELECT
        id,
        viz_group_code,region,
        MIN(partition_ts) as first_date,
        MAX(partition_ts) as last_date,
        DATE_DIFF(MAX(partition_ts), MIN(partition_ts),day) as lifetime
    FROM
        w_labels
    WHERE partition_ts <= '{partition_date}' -- The partition date we are calculating for
    AND partition_ts >= '{first_partition_date}' -- Start date of all valid data
    group by id, viz_group_code, region
),
r_lifetime_pct AS (
  SELECT viz_group_code,region, PERCENTILE_CONT(lifetime, 0.5) OVER (PARTITION BY viz_group_code, region) AS median_lifetime,
  PERCENTILE_CONT(lifetime, 0.25) OVER (PARTITION BY viz_group_code, region) AS p25_lifetime,
  PERCENTILE_CONT(lifetime, 0.75) OVER (PARTITION BY viz_group_code, region) AS p75_lifetime
  FROM r_lifetime_id
  -- WHERE last_date != '{partition_date}' -- This limits the scope to only listings that are already removed i.e. 'sold'
),
r_lifetime_final AS (
  SELECT viz_group_code, region,
  MAX(median_lifetime) AS median_lifetime, 
  MAX(p25_lifetime) AS p25_lifetime, 
  MAX(p75_lifetime) AS p75_lifetime
  FROM r_lifetime_pct
  GROUP BY viz_group_code, region
),
--SELECT viz_group_code, median_lifetime FROM lifetime_final order by median_lifetime asc
r_metrics_tmp AS (
  SELECT viz_group_code, region, id, 
  PERCENTILE_CONT(psf, 0.5) OVER (PARTITION BY viz_group_code,region) AS median_psf,
  PERCENTILE_CONT(psf, 0.25) OVER (PARTITION BY viz_group_code, region) AS p25_psf,
  PERCENTILE_CONT(psf, 0.75) OVER (PARTITION BY viz_group_code, region) AS p75_psf
  FROM w_labels
  WHERE partition_ts = '{partition_date}'
),
r_metrics AS (
  SELECT CAST('{partition_date}' AS timestamp) AS partition_ts,
  CAST('{partition_date}' AS date) AS dt ,  
  region,
  viz_group_code,
  COUNT(id) AS listings,  
  MAX(median_psf) AS median_psf, 
  MAX(p25_psf) AS p25_psf,
  MAX(p75_psf) AS p75_psf,
  FROM r_metrics_tmp
  GROUP BY viz_group_code, region
),
r_results AS (
SELECT m.*, l.median_lifetime, l.p25_lifetime, l.p75_lifetime  
FROM r_metrics m
LEFT JOIN r_lifetime_final l
ON m.viz_group_code = l.viz_group_code
AND m.region = l.region
WHERE m.viz_group_code != 'NA'),
lifetime_id AS (
    SELECT
        id,
        viz_group_code,
        MIN(partition_ts) as first_date,
        MAX(partition_ts) as last_date,
        DATE_DIFF(MAX(partition_ts), MIN(partition_ts),day) as lifetime
    FROM
        w_labels
    WHERE partition_ts <= '{partition_date}' -- The partition date we are calculating for
    AND partition_ts >= '{first_partition_date}'  -- Start date of all valid data
    group by id, viz_group_code
),
lifetime_pct AS (
  SELECT viz_group_code, PERCENTILE_CONT(lifetime, 0.5) OVER (PARTITION BY viz_group_code) AS median_lifetime,
  PERCENTILE_CONT(lifetime, 0.25) OVER (PARTITION BY viz_group_code) AS p25_lifetime,
  PERCENTILE_CONT(lifetime, 0.75) OVER (PARTITION BY viz_group_code) AS p75_lifetime
  FROM lifetime_id
  -- WHERE last_date != '{partition_date}' -- This limits the scope to only listings that are already removed i.e. 'sold'
),
lifetime_final AS (
  SELECT viz_group_code, 
  MAX(median_lifetime) AS median_lifetime, 
  MAX(p25_lifetime) AS p25_lifetime, 
  MAX(p75_lifetime) AS p75_lifetime
  FROM lifetime_pct
  GROUP BY viz_group_code
),
--SELECT viz_group_code, median_lifetime FROM lifetime_final order by median_lifetime asc
metrics_tmp AS (
  SELECT viz_group_code, id, 
  PERCENTILE_CONT(psf, 0.5) OVER (PARTITION BY viz_group_code) AS median_psf,
  PERCENTILE_CONT(psf, 0.25) OVER (PARTITION BY viz_group_code) AS p25_psf,
  PERCENTILE_CONT(psf, 0.75) OVER (PARTITION BY viz_group_code) AS p75_psf
  FROM w_labels
  WHERE partition_ts = '{partition_date}'
),
metrics AS (
  SELECT CAST('{partition_date}' AS timestamp) AS partition_ts ,
  CAST('{partition_date}' AS date) AS dt ,
  'ALL' as region,
  viz_group_code, 
  COUNT(id) AS listings,  
  MAX(median_psf) AS median_psf, 
  MAX(p25_psf) AS p25_psf,
  MAX(p75_psf) AS p75_psf,
  FROM metrics_tmp
  GROUP BY viz_group_code
),
results AS (
SELECT
m.*,
l.median_lifetime, 
l.p25_lifetime, 
l.p75_lifetime,
FROM metrics m
LEFT JOIN lifetime_final l
ON m.viz_group_code = l.viz_group_code
WHERE m.viz_group_code != 'NA')
(SELECT * FROM results WHERE region IS NOT NULL)
UNION ALL
(SELECT * FROM r_results WHERE region IS NOT NULL)
"""
