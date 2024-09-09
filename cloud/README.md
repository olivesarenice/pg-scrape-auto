# Schedule

Local scrape happens at: 
D 00:30 SGT | D-1 16:30 UTC
and writes to partition D-1

Cloud lambda happens at
D 01:00 SGT | D-1 17:00

# Analysis approach

```SQL 

-- Listings in D that was not in D-1 (new)
WITH ytd_ids as (SELECT id from pg_listings.listings_raw where partition_ts = '2024-09-07')
SELECT COUNT(*) as new_ids FROM pg_listings.listings_raw where id not in (select id from ytd_ids) and partition_ts = '2024-09-08';

-- Listings that were in D-1 but are now not in D (taken down)
WITH ytd_ids as (SELECT id from pg_listings.listings_raw where partition_ts = '2024-09-08')
SELECT COUNT(*) as lost_ids FROM pg_listings.listings_raw where id not in (select id from ytd_ids) and partition_ts = '2024-09-07'

-- Listings that were in both D and D-1
SELECT count(l1.id)
FROM `pg_listings.listings_raw` l1
JOIN `pg_listings.listings_raw` l2
ON l1.id = l2.id
WHERE l1.partition_ts = '2024-09-08'
  AND l2.partition_ts = '2024-09-07'
```
