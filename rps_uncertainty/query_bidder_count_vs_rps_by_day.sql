with t1_split_revenue as (
select date,
    array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
        + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0) AS client_bidders,
    array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '3')) AS server_bidders,
    revenue
from `streamamp-qa-239417.DAS_eventstream_session_data.{DTF_or_eventstream}_DAS_expt_stats_split_revenue_{table_ext}`
where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23  and regexp_contains(fs_clientservermask, '[0123]{23}')
 {and_where}
), client_split_revenue as (
  select date, client_bidders bidders, avg(revenue) * 1000 rps_client,
     sqrt((avg(pow(revenue, 2)) - pow(avg(revenue), 2)) / count(*)) * 1000 rps_client_err
  from t1_split_revenue
  group by 1, 2
), server_split_revenue as (
  select date, server_bidders bidders, avg(revenue) * 1000 rps_server,
     sqrt((avg(pow(revenue, 2)) - pow(avg(revenue), 2)) / count(*)) * 1000 rps_server_err
  from t1_split_revenue
  group by 1, 2
), client_server_split_revenue as (
  select date, client_bidders + server_bidders bidders, avg(revenue) * 1000 rps_client_server,
     sqrt((avg(pow(revenue, 2)) - pow(avg(revenue), 2)) / count(*)) * 1000 rps_client_server_err
  from t1_split_revenue
  group by 1, 2
)
select coalesce(t1.date, t2.date, t3.date) date, coalesce(t1.bidders, t2.bidders, t3.bidders) bidders,
    rps_client, rps_client_err, rps_server, rps_server_err, rps_client_server, rps_client_server_err
from client_split_revenue t1
full join server_split_revenue t2 using (date, bidders)
full join client_server_split_revenue t3 using (date, bidders)
where coalesce(t1.bidders, t2.bidders, t3.bidders) < 20

order by 1




