# Schedule

Local scrape happens at: 
D 00:30 SGT | D-1 16:30 UTC
and writes to partition D-1

Cloud lambda happens at
D 01:00 SGT | D-1 17:00


# Logical groups for analysis

Visualisation will happen on these dimensions:

- property_segment
  - number of bedrooms
  - type of property
  - region/ or entire SGP

To standardize the dimensions, we define 2 dimensions to aggregate on:

1. Region - this is the PropGuru provided region. It also includes an `ALL` value which refers to aggregating across ALL regions.
2. Custom viz_group as defined in this reference table:

**`ref_viz_groups`**
```sql
INSERT INTO pg_listings.ref_viz_groups (`code`, description, criteria_json)
VALUES
    ('AH2', 'HDB | 1/2 ROOM', JSON '[{"criteria": {"col": "property_type", "val": "1/2 ROOM HDB"}}]'),
    ('AH3', 'HDB | 3 ROOM', JSON '[{"criteria": {"col": "property_type", "val": "3 ROOM HDB"}}]'),
    ('AH4', 'HDB | 4 ROOM', JSON '[{"criteria": {"col": "property_type", "val": "4 ROOM HDB"}}]'),
    ('AH5', 'HDB | 5 ROOM', JSON '[{"criteria": {"col": "property_type", "val": "5 ROOM HDB"}}]'),
    ('AH0', 'HDB | OTHERS', JSON '[{"criteria": {"col": "property_type", "val": "OTHER HDB"}}]'),
    ('AN1', 'NL | Studio/ 1 BR', JSON '[{"criteria": {"col": "property_segment", "val": "Non-Landed"}}, {"criteria": {"col": "bedrooms", "val": "<= 1"}}]'),
    ('AN2', 'NL | 2 BR', JSON '[{"criteria": {"col": "property_segment", "val": "Non-Landed"}}, {"criteria": {"col": "bedrooms", "val": "2"}}]'),
    ('AN3', 'NL | 3 BR', JSON '[{"criteria": {"col": "property_segment", "val": "Non-Landed"}}, {"criteria": {"col": "bedrooms", "val": "3"}}]'),
    ('AN4', 'NL | 4 BR', JSON '[{"criteria": {"col": "property_segment", "val": "Non-Landed"}}, {"criteria": {"col": "bedrooms", "val": "4"}}]'),
    ('AN5', 'NL | 5+ BR', JSON '[{"criteria": {"col": "property_segment", "val": "Non-Landed"}}, {"criteria": {"col": "bedrooms", "val": ">= 5"}}]'),
    ('AL1', 'L | Terrace', JSON '[{"criteria": {"col": "property_type", "val": "Terraced House"}}]'),
    ('AL2', 'L | Semi-Detached', JSON '[{"criteria": {"col": "property_type", "val": "Semi-Detached House, Corner Terrace"}}]'),
    ('AL3', 'L | Detached', JSON '[{"criteria": {"col": "property_type", "val": "Detached House, Good Class Bungalow, Bungalow House"}}]');
```

The SQL that labels each record into a viz_group is hardcoded for now, although it can also be dynamically generated based on the above criteria JSON.

```SQL
SELECT
    *,
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
    pg_listings.listings_raw
```

Since Landed records don't have region's specified, we will standardize them by mapping their district_codes to the region:

```SQL
INSERT INTO pg_listings.ref_districtcode_region (district_code, region)
VALUES
    ('D01', 'City & South West (D01-08)'),
    ('D02', 'City & South West (D01-08)'),
    ('D03', 'City & South West (D01-08)'),
    ('D04', 'City & South West (D01-08)'),
    ('D05', 'City & South West (D01-08)'),
    ('D06', 'City & South West (D01-08)'),
    ('D07', 'City & South West (D01-08)'),
    ('D08', 'City & South West (D01-08)'),
    ('D09', 'Orchard / Holland (D09-10)'),
    ('D10', 'Orchard / Holland (D09-10)'),
    ('D11', 'Newton / Bt. Timah (D11, 21)'),
    ('D12', 'Balestier / Geylang (D12-14)'),
    ('D13', 'Balestier / Geylang (D12-14)'),
    ('D14', 'Balestier / Geylang (D12-14)'),
    ('D15', 'East Coast (D15-16)'),
    ('D16', 'East Coast (D15-16)'),
    ('D17', 'Changi / Pasir Ris (D17-18)'),
    ('D18', 'Changi / Pasir Ris (D17-18)'),
    ('D19', 'Serangoon / Thomson (D19-20)'),
    ('D20', 'Serangoon / Thomson (D19-20)'),
    ('D21', 'Newton / Bt. Timah (D11, 21)'),
    ('D25', 'North (D25-28)'),
    ('D26', 'North (D25-28)'),
    ('D27', 'North (D25-28)'),
    ('D28', 'North (D25-28)');
```

# Analysis tables

To reduce workload on BigQuery, we will pre-calculate the daily metrics each day.

The calculated metrics go into the `agg_daily_viz` table, partitioned by date `dt` column.

# Aggregated metrics

On a daily basis, we calculate the following metrics at the 25th , 50th (median), and 75th percentiles:

- Number of listings
- PSF
- Listing lifetime

    Lifetime is defined as the number of days between the first_date and last_date that the listing appears across the entire database. This is possible because the entire set of listings is collected daily.


The full query for the **ALL region** calculation is:

```SQL
WITH w_labels AS (
  SELECT
    *,
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
    pg_listings.listings_raw
),
lifetime_id AS (
    SELECT
        id,
        viz_group_code,
        MIN(partition_ts) as first_date,
        MAX(partition_ts) as last_date,
        DATE_DIFF(MAX(partition_ts), MIN(partition_ts),day) as lifetime
    FROM
        w_labels
    WHERE partition_ts <= '2024-09-21' -- The partition date we are calculating for
    AND partition_ts >= '2024-09-03' -- Start date of all valid data
    group by id, viz_group_code
),
lifetime_pct AS (
  SELECT viz_group_code, PERCENTILE_CONT(lifetime, 0.5) OVER (PARTITION BY viz_group_code) AS median_lifetime,
  PERCENTILE_CONT(lifetime, 0.25) OVER (PARTITION BY viz_group_code) AS p25_lifetime,
  PERCENTILE_CONT(lifetime, 0.75) OVER (PARTITION BY viz_group_code) AS p75_lifetime
  FROM lifetime_id
  -- WHERE last_date != '2024-09-21' -- This limits the scope to only listings that are already removed i.e. 'sold'
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
  WHERE partition_ts = '2024-09-21'
),
metrics AS (
  SELECT CAST('2024-09-21' AS date) AS dt ,
  'ALL' as region,
  viz_group_code, 
  COUNT(id) AS listings,  
  MAX(median_psf) AS median_psf, 
  MAX(p25_psf) AS p25_psf,
  MAX(p75_psf) AS p75_psf,
  FROM metrics_tmp
  GROUP BY viz_group_code
)
SELECT
m.*,
l.median_lifetime, 
l.p25_lifetime, 
l.p75_lifetime,
FROM metrics m
LEFT JOIN lifetime_final l
ON m.viz_group_code = l.viz_group_code
WHERE m.viz_group_code != 'NA'
```

And region-specific:

```SQL
WITH w_labels AS (
  SELECT
    *,
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
    pg_listings.listings_raw
),
lifetime_id AS (
    SELECT
        id,
        viz_group_code,region,
        MIN(partition_ts) as first_date,
        MAX(partition_ts) as last_date,
        DATE_DIFF(MAX(partition_ts), MIN(partition_ts),day) as lifetime
    FROM
        w_labels
    WHERE partition_ts <= '2024-09-21' -- The partition date we are calculating for
    AND partition_ts >= '2024-09-03' -- Start date of all valid data
    group by id, viz_group_code, region
),
lifetime_pct AS (
  SELECT viz_group_code,region, PERCENTILE_CONT(lifetime, 0.5) OVER (PARTITION BY viz_group_code, region) AS median_lifetime,
  PERCENTILE_CONT(lifetime, 0.25) OVER (PARTITION BY viz_group_code, region) AS p25_lifetime,
  PERCENTILE_CONT(lifetime, 0.75) OVER (PARTITION BY viz_group_code, region) AS p75_lifetime
  FROM lifetime_id
  -- WHERE last_date != '2024-09-21' -- This limits the scope to only listings that are already removed i.e. 'sold'
),
lifetime_final AS (
  SELECT viz_group_code, region,
  MAX(median_lifetime) AS median_lifetime, 
  MAX(p25_lifetime) AS p25_lifetime, 
  MAX(p75_lifetime) AS p75_lifetime
  FROM lifetime_pct
  GROUP BY viz_group_code, region
),
--SELECT viz_group_code, median_lifetime FROM lifetime_final order by median_lifetime asc
metrics_tmp AS (
  SELECT viz_group_code, region, id, 
  PERCENTILE_CONT(psf, 0.5) OVER (PARTITION BY viz_group_code,region) AS median_psf,
  PERCENTILE_CONT(psf, 0.25) OVER (PARTITION BY viz_group_code, region) AS p25_psf,
  PERCENTILE_CONT(psf, 0.75) OVER (PARTITION BY viz_group_code, region) AS p75_psf
  FROM w_labels
  WHERE partition_ts = '2024-09-21'
),
metrics AS (
  SELECT CAST('2024-09-21' AS date) AS dt ,  region,
  viz_group_code,
  COUNT(id) AS listings,  
  MAX(median_psf) AS median_psf, 
  MAX(p25_psf) AS p25_psf,
  MAX(p75_psf) AS p75_psf,
  FROM metrics_tmp
  GROUP BY viz_group_code, region
)
SELECT m.*, l.median_lifetime, l.p25_lifetime, l.p75_lifetime  FROM metrics m
LEFT JOIN lifetime_final l
ON m.viz_group_code = l.viz_group_code
AND m.region = l.region
WHERE m.viz_group_code != 'NA'
```


---
---
---


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

# Other analysis

## Price history of each listing
WITH price_history AS (
    SELECT
        id,
        TO_JSON_STRING(ARRAY_AGG(STRUCT(partition_ts AS date, price) ORDER BY partition_ts)) AS price_history
    FROM
        pg_listings.listings_raw
    GROUP BY
        id
),
per_id AS (
    SELECT
        id,
        url,
        MIN(partition_ts) AS first_date,
        MAX(partition_ts) AS last_date
    FROM
        pg_listings.listings_raw
    GROUP BY
        id, url
),
price_summary AS (
    SELECT
        per_id.id,
        first_date,
        r1.price AS first_price,
        last_date,
        r2.price AS last_price,
        r2.price - r1.price AS abs_delta,
        SAFE_DIVIDE(r2.price, r1.price) AS pct_delta,  -- Use SAFE_DIVIDE to avoid division by zero
        per_id.url
    FROM
        per_id
    LEFT JOIN 
        pg_listings.listings_raw r1
    ON 
        r1.id = per_id.id
        AND r1.partition_ts = per_id.first_date
    LEFT JOIN
        pg_listings.listings_raw r2
    ON
        r2.id = per_id.id
        AND r2.partition_ts = per_id.last_date
),
final AS (
    SELECT 
        s.*, 
        h.price_history
    FROM 
        price_history h
    LEFT JOIN 
        price_summary s
    ON 
        h.id = s.id
)
SELECT 
    FLOOR(pct_delta * 100) / 100 AS pct_delta_bucket,  -- Create buckets of size 0.1
    COUNT(*) AS count
FROM 
    final
WHERE 
    pct_delta != 1.0
    and pct_delta < 2.0
    and pct_delta > 0.5
GROUP BY 
    pct_delta_bucket
ORDER BY 
    pct_delta_bucket;




WITH price_history as (SELECT
    id,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(partition_ts AS date, price) ORDER BY partition_ts)) AS price_history
FROM
  pg_listings.listings_raw
GROUP BY
    id
ORDER BY
    id),
per_id AS (
    SELECT
        id,
        url,
        MIN(partition_ts) as first_date,
        MAX(partition_ts) as last_date,
    FROM
        pg_listings.listings_raw
    group by id, url
),
price_summary as (
SELECT
    per_id.id,
    first_date,
    r1.price as first_price,
    last_date,
    r2.price as last_price,
    r2.price - r1.price as abs_delta,
    r2.price/r1.price as pct_delta,
    per_id.url
FROM
    per_id
LEFT JOIN 
  pg_listings.listings_raw r1
ON 
  r1.id = per_id.id
  AND r1.partition_ts = per_id.first_date
LEFT JOIN
  pg_listings.listings_raw r2
ON
  r2.id = per_id.id
  AND r2.partition_ts = per_id.last_date),
final as (
SELECT s.*, h.price_history
FROM price_history h
LEFT JOIN price_summary s
ON h.id = s.id)
SELECT *
FROM final 
--where pct_delta > 1 # 1006 5%
--where pct_delta < 1 #2957 2%
where pct_delta != 1 # 55734 93%
order by pct_delta asc

## Per project price ranges