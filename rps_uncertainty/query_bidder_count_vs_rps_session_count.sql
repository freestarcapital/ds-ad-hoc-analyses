with opt_data as (
select
    array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
        + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0)
        - if(date >= "2024-10-9", 4, 0) AS client_bidders,
    sum(if(date < '2024-08-28', 1, 0)) as session_count_before_opt,
    sum(if((date >= '2024-08-29') and (date <= '2024-09-23'), 1, 0)) as session_count_after_from_Aug29_to_Sep23_opt,
--    sum(if((date >= '2024-08-29') and (date <= '2024-09-03'), 1, 0)) as session_count_after_from_Aug29_to_Sep03_opt,
--    sum(if((date >= '2024-09-04') and (date <= '2024-09-17'), 1, 0)) as session_count_after_from_Aug04_to_Sep17_opt,
--    sum(if((date >= '2024-09-18') and (date <= '2024-09-23'), 1, 0)) as session_count_after_from_Sep18_to_Sep23_opt,
    sum(if((date >= '2024-09-25') and (date <= '2024-10-8'), 1, 0)) as session_count_from_Sep25_to_Oct8_opt,
    sum(if(date >= '2024-10-9', 1, 0)) as session_count_from_Oct9_opt

from `streamamp-qa-239417.DAS_eventstream_session_data.{DTF_or_eventstream}_DAS_opt_stats_split_revenue_{table_ext}`
where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23  and regexp_contains(fs_clientservermask, '[0123]{23}')
     and substr(fs_clientservermask, 10, 1) in ('0', '1') and substr(fs_clientservermask, 11, 1) in ('0', '1')
    and substr(fs_clientservermask, 21, 1) in ('0', '1')
    and substr(fs_clientservermask, 22, 1) in ('0', '1')
    and substr(fs_clientservermask, 17, 1) in ('0', '1') and substr(fs_clientservermask, 18, 1) in ('0', '1')
    and substr(fs_clientservermask, 19, 1) in ('0', '1')
    --and substr(fs_clientservermask, 13, 1) in ('0', '1')
   {and_where}
group by 1
), expt_data as (
select
    array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
        + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0) AS client_bidders,
    sum(if(date < '2024-08-28', 1, 0)) as session_count_before_expt,
    sum(if((date >= '2024-08-29') and (date <= '2024-09-23'), 1, 0)) as session_count_after_from_Aug29_to_Sep23_expt,
--    sum(if((date >= '2024-08-29') and (date <= '2024-09-03'), 1, 0)) as session_count_after_from_Aug29_to_Sep03_expt,
--    sum(if((date >= '2024-09-04') and (date <= '2024-09-17'), 1, 0)) as session_count_after_from_Aug04_to_Sep17_expt,
--    sum(if((date >= '2024-09-18') and (date <= '2024-09-23'), 1, 0)) as session_count_after_from_Sep18_to_Sep23_expt,
    sum(if((date >= '2024-09-25') and (date <= '2024-10-8'), 1, 0)) as session_count_from_Sep25_to_Oct8_expt,
    sum(if(date >= '2024-10-9', 1, 0)) as session_count_from_Oct9_expt

from `streamamp-qa-239417.DAS_eventstream_session_data.{DTF_or_eventstream}_DAS_expt_stats_split_revenue_{table_ext}`
where (fs_clientservermask is not null) and char_length(fs_clientservermask) = 23  and regexp_contains(fs_clientservermask, '[0123]{23}')
    {and_where}
group by 1
)
select client_bidders,
    coalesce(session_count_before_opt, 0) + coalesce(session_count_before_expt, 0) session_count_to_Aug28,
    coalesce(session_count_after_from_Aug29_to_Sep23_opt, 0) + coalesce(session_count_after_from_Aug29_to_Sep23_expt, 0) session_count_after_from_Aug29_to_Sep23,
--    coalesce(session_count_after_from_Aug29_to_Sep03_opt, 0) + coalesce(session_count_after_from_Aug29_to_Sep03_expt, 0) session_count_after_from_Aug29_to_Sep03,
--    coalesce(session_count_after_from_Aug04_to_Sep17_opt, 0) + coalesce(session_count_after_from_Aug04_to_Sep17_expt, 0) session_count_after_from_Aug04_to_Sep17,
--    coalesce(session_count_after_from_Sep18_to_Sep23_opt, 0) + coalesce(session_count_after_from_Sep18_to_Sep23_expt, 0) session_count_after_from_Sep18_to_Sep23,
    coalesce(session_count_from_Sep25_to_Oct8_opt, 0) + coalesce(session_count_from_Sep25_to_Oct8_expt, 0) session_count_from_Sep25_to_Oct8,
    coalesce(session_count_from_Oct9_opt, 0) + coalesce(session_count_from_Oct9_expt, 0) session_count_from_Oct9


from opt_data full join expt_data using (client_bidders)
order by 1
