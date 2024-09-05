with t1 as (
select device_category, country_code,
  array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '2')) + 6 AS client_bidders,
  array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '3')) AS server_bidders,
  revenue
from `streamamp-qa-239417.DAS_eventstream_session_data.{DTF_or_eventstream}_DAS_expt_stats_{table_ext}`
where fs_clientservermask is not null

), t2 as (
    select *, client_bidders + server_bidders all_bidders from t1
)
 select {which_bidders}, {country_or_device},
  avg(revenue) * 1000 rps, count(*) count
  from t2
  group by 1, 2
order by 1, 2
