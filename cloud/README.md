# Schedule

Local scrape happens at: 
D 00:30 SGT | D-1 16:30 UTC
and writes to partition D-1

Cloud lambda happens at
D 01:00 SGT | D-1 17:00

# Analysis approach

```SQL 
-- Example D = 2024-09-09
WITH rel_records as (SELECT * FROM pg_listings.listings_raw WHERE district_code = 'D15' and property_segment = 'Non-Landed' and bedrooms = 3), 
day_active_listings as (SELECT COUNT(*) as day_active_listings FROM rel_records WHERE partition_ts = '2024-09-09'),
day_new_listings as (SELECT COUNT(*) as day_new_listings FROM rel_records where id not in (SELECT id from rel_records where DATE(partition_ts) = (SELECT DATETIME_SUB('2024-09-09', INTERVAL 1 DAY))) and partition_ts = '2024-09-09'),
day_removed_listings as (SELECT COUNT(*) as day_removed_listings FROM rel_records where id not in (SELECT id from rel_records where partition_ts = '2024-09-09') and DATE(partition_ts) = (SELECT DATETIME_SUB('2024-09-09', INTERVAL 1 DAY))),
wtd_active_listings as (SELECT COUNT(distinct id) as wtd_active_listings FROM rel_records WHERE DATE(partition_ts) > (SELECT DATETIME_SUB('2024-09-09', INTERVAL 1 DAY)) and partition_ts <= '2024-09-09' ),
now as (SELECT id from rel_records WHERE partition_ts = '2024-09-09'),
past_1d as (SELECT id from rel_records WHERE DATE(partition_ts) = (SELECT DATETIME_SUB('2024-09-09', INTERVAL 1 DAY))),
past_7d as (SELECT id from rel_records WHERE DATE(partition_ts) = (SELECT DATETIME_SUB('2024-09-09', INTERVAL 3 DAY))),
lifespan_1d as (
  SELECT
      COUNT(p.id) * 1.0 / (SELECT COUNT(*) FROM now) AS lifespan_1d
  FROM
      past_1d p
  JOIN
      now n
  ON
      p.id = n.id
),
lifespan_7d as (
  SELECT
      COUNT(p.id) * 1.0 / (SELECT COUNT(*) FROM now) AS lifespan_7d
  FROM
      past_7d p
  JOIN
      now n
  ON
      p.id = n.id
),
day_median_psf as (SELECT PERCENTILE_CONT(psf, 0.5)  OVER() as day_median_psf FROM rel_records LIMIT 1),
day_25_psf as (SELECT PERCENTILE_CONT(psf, 0.25)  OVER() as day_25_psf FROM rel_records LIMIT 1),
day_75_psf as (SELECT PERCENTILE_CONT(psf, 0.75) OVER() as day_75_psf FROM rel_records LIMIT 1),
day_median_q as (SELECT PERCENTILE_CONT(price, 0.5)  OVER() as day_median_q FROM rel_records LIMIT 1),
day_25_q as (SELECT PERCENTILE_CONT(price, 0.25)  OVER() as day_25_q FROM rel_records LIMIT 1),
day_75_q as (SELECT PERCENTILE_CONT(price, 0.75) OVER() as day_75_q FROM rel_records LIMIT 1)
SELECT 
    '2024-09-09' as date,
    'new_projects' as filter,
    (SELECT day_active_listings FROM day_active_listings) AS day_active_listings,
    (SELECT day_new_listings FROM day_new_listings) AS day_new_listings,
    (SELECT day_removed_listings FROM day_removed_listings) AS day_removed_listings,
    (SELECT wtd_active_listings FROM wtd_active_listings) AS wtd_active_listings,
    (SELECT lifespan_1d FROM lifespan_1d) AS lifespan_1d,
    (SELECT lifespan_7d FROM lifespan_7d) AS lifespan_7d,
    (SELECT day_median_psf FROM day_median_psf) AS day_median_psf,
    (SELECT day_25_psf FROM day_25_psf) AS day_25_psf,
    (SELECT day_75_psf FROM day_75_psf) AS day_75_psf,    
    (SELECT day_median_q FROM day_median_q) AS day_median_q,
    (SELECT day_25_q FROM day_25_q) AS day_25_q,
    (SELECT day_75_q FROM day_75_q) AS day_75_q   
```
