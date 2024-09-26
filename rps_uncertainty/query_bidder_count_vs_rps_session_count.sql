with opt_data as (
select
    array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
        + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0) AS client_bidders,
    sum(if(date < '2024-08-28', 1, 0)) as session_count_before_opt,
    sum(if((date >= '2024-08-29') and (date <= '2024-09-03'), 1, 0)) as session_count_after_from_Aug29_to_Sep03_opt,
    sum(if((date >= '2024-09-04') and (date <= '2024-09-17'), 1, 0)) as session_count_after_from_Aug04_to_Sep17_opt,
    sum(if((date >= '2024-09-18') and (date <= '2024-09-23'), 1, 0)) as session_count_after_from_Sep18_to_Sep23_opt,
    sum(if(date >= '2024-09-24', 1, 0)) as session_count_from_Sep24_opt

from `streamamp-qa-239417.DAS_eventstream_session_data.{DTF_or_eventstream}_DAS_opt_stats_split_revenue_{table_ext}`
where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23  and regexp_contains(fs_clientservermask, '[0123]{23}')
    {and_where}
group by 1
), expt_data as (
select
    array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
        + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0) AS client_bidders,
    sum(if(date < '2024-08-28', 1, 0)) as session_count_before_expt,
    sum(if((date >= '2024-08-29') and (date <= '2024-09-03'), 1, 0)) as session_count_after_from_Aug29_to_Sep03_expt,
    sum(if((date >= '2024-09-04') and (date <= '2024-09-17'), 1, 0)) as session_count_after_from_Aug04_to_Sep17_expt,
    sum(if((date >= '2024-09-18') and (date <= '2024-09-23'), 1, 0)) as session_count_after_from_Sep18_to_Sep23_expt,
    sum(if(date >= '2024-09-24', 1, 0)) as session_count_from_Sep24_expt

from `streamamp-qa-239417.DAS_eventstream_session_data.{DTF_or_eventstream}_DAS_expt_stats_split_revenue_{table_ext}`
where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23  and regexp_contains(fs_clientservermask, '[0123]{23}')
    {and_where}
group by 1
)
select client_bidders,
    coalesce(session_count_before_opt, 0) + coalesce(session_count_before_expt, 0) session_count_before,
    coalesce(session_count_after_from_Aug29_to_Sep03_opt, 0) + coalesce(session_count_after_from_Aug29_to_Sep03_expt, 0) session_count_after_from_Aug29_to_Sep03,
    coalesce(session_count_after_from_Aug04_to_Sep17_opt, 0) + coalesce(session_count_after_from_Aug04_to_Sep17_expt, 0) session_count_after_from_Aug04_to_Sep17,
    coalesce(session_count_after_from_Sep18_to_Sep23_opt, 0) + coalesce(session_count_after_from_Sep18_to_Sep23_expt, 0) session_count_after_from_Sep18_to_Sep23,
    coalesce(session_count_from_Sep24_opt, 0) + coalesce(session_count_from_Sep24_expt, 0) session_count_from_Sep24

from opt_data full join expt_data using (client_bidders)
order by 1
