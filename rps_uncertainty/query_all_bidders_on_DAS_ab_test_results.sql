--select country_code,
--  sum(if(test_group=0,gross_revenue,0)) revenue_0,
--  sum(if(test_group=0,sessions,0)) sessions_0,
--  ifnull(safe_divide(sum(if(test_group=0,gross_revenue,0)), sum(if(test_group=0,sessions,0))),0)*1000 rps_0,
--  sum(if(test_group=1,gross_revenue,0)) revenue_1,
--  sum(if(test_group=1,sessions,0)) sessions_1,
--  ifnull(safe_divide(sum(if(test_group=1,gross_revenue,0)), sum(if(test_group=1,sessions,0))),0)*1000 rps_1
--from `streamamp-qa-239417.DAS_eventstream_session_data.DAS_ab_test`
--where '2024-08-20' <= record_date__mst and record_date__mst <= '2024-08-23'
--group by 1
--order by sum(sessions) desc

select {domain_or_country_code},
  sum(if(test_group=0,gross_revenue,0)) revenue_0,
  sum(if(test_group=0,sessions,0)) sessions_0,
  ifnull(safe_divide(sum(if(test_group=0,gross_revenue,0)), sum(if(test_group=0,sessions,0))),0)*1000 rps_0,
  sum(if(test_group=1,gross_revenue,0)) revenue_1,
  sum(if(test_group=1,sessions,0)) sessions_1,
  ifnull(safe_divide(sum(if(test_group=1,gross_revenue,0)), sum(if(test_group=1,sessions,0))),0)*1000 rps_1
from `streamamp-qa-239417.DAS_eventstream_session_data.DAS_ab_test` ab
left join `freestar-157323.dashboard.sites` s on s.id = ab.site_id
where '{start_date}' <= record_date__mst and record_date__mst <= '{end_date}'
and {domain_or_country_code} is not null
group by 1
order by sum(sessions) desc

