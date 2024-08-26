with t1 as (
select array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '2')) AS client_bidders,
  array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '3')) AS server_bidders,
  revenue
from `streamamp-qa-239417.DAS_eventstream_session_data.DTF_DAS_expt_stats_2024-08-20_30_1`
), client as (
  select client_bidders bidders, avg(revenue) * 1000 rps_client
  from t1
  group by 1
), server as (
  select server_bidders bidders, avg(revenue) * 1000 rps_server
  from t1
  group by 1
), client_server as (
  select client_bidders + server_bidders bidders, avg(revenue) * 1000 rps_client_server
  from t1
  group by 1
),
t1_split_revenue as (
select array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '2')) AS client_bidders,
  array_length(REGEXP_EXTRACT_ALL(fs_clientservermask, '3')) AS server_bidders,
  revenue
from `streamamp-qa-239417.DAS_eventstream_session_data.DTF_DAS_expt_stats_split_revenue_2024-08-20_30_1`
), client_split_revenue as (
  select client_bidders bidders, avg(revenue) * 1000 rps_client_split_revenue
  from t1_split_revenue
  group by 1
), server_split_revenue as (
  select server_bidders bidders, avg(revenue) * 1000 rps_server_split_revenue
  from t1_split_revenue
  group by 1
), client_server_split_revenue as (
  select client_bidders + server_bidders bidders, avg(revenue) * 1000 rps_client_server_split_revenue
  from t1_split_revenue
  group by 1
)
select *
from client
full join server using (bidders)
full join client_server using (bidders)
full join client_split_revenue using (bidders)
full join server_split_revenue using (bidders)
full join client_server_split_revenue using (bidders)
order by 1




