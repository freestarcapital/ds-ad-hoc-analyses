with t1 as (
    select {dims},
        array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
            + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0) AS client_bidders,
        array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '3')) AS server_bidders,
        revenue
    from `{project_id}.DAS_increment.{tablename_from}`
    where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23  and regexp_contains(fs_clientservermask, '[0123]{23}')
        {and_where}
), client as (
    select client_bidders bidders, {dims},
        avg(revenue) * 1000 rps_client,
        sqrt((avg(pow(revenue, 2)) - pow(avg(revenue), 2)) / count(*)) * 1000 rps_client_err,
        count(*) session_count_client
    from t1
    group by 1, {dims}
), server as (
    select server_bidders bidders, {dims},
        avg(revenue) * 1000 rps_server,
        sqrt((avg(pow(revenue, 2)) - pow(avg(revenue), 2)) / count(*)) * 1000 rps_server_err,
        count(*) session_count_server
    from t1
    group by 1, {dims}
), client_server as (
    select client_bidders + server_bidders bidders, {dims},
        avg(revenue) * 1000 rps_client_server,
        sqrt((avg(pow(revenue, 2)) - pow(avg(revenue), 2)) / count(*)) * 1000 rps_client_server_err,
        count(*) session_count_client_server
    from t1

    group by 1, {dims}
)
select --coalesce(t1.date, t2.date, t3.date) date, coalesce(t1.bidders, t2.bidders, t3.bidders) bidders,
    {dims}, bidders,
    rps_client, rps_client_err, rps_server, rps_server_err, rps_client_server, rps_client_server_err,
    session_count_client, session_count_server, session_count_client_server
from client t1
full join server t2 using ({dims}, bidders)
full join client_server t3 using ({dims}, bidders)

order by 1