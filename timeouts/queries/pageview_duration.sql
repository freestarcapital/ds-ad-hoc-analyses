with t1 as (
  select session_id, hit_time
  FROM `freestar-157323.prod_eventstream.pagehits_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null
  qualify count(*) over (partition by pageview_id) = 1
), t2 as (
  select TIMESTAMP_DIFF(TIMESTAMP_MILLIS(LEAD(hit_time) OVER (PARTITION BY session_id ORDER BY hit_time ASC)),
      TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS duration_ms
  from t1
  qualify count(*) over (partition by session_id) > 1
)
select duration_ms
from t2
where duration_ms is not null