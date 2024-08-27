with t1 as (
select device_category, country_code,
  array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '2')) AS client_bidders,
  array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '3')) AS server_bidders,
  revenue
from `streamamp-qa-239417.DAS_eventstream_session_data.DTF_DAS_expt_stats_2024-08-20_30_1`
), t2 as (
    select *, client_bidders + server_bidders all_bidders from t1
)
 select {which_bidders}, {country_or_device},
  avg(revenue) * 1000 rps, count(*) count
  from t2
  group by 1, 2
order by 1, 2