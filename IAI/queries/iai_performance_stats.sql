DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('{start_date}'), DATE('{end_date}'));

with dtf as (
    select fs_session_id, min(date) date,
    sum(if({ad_unit_name_match}, revenue, 0)) revenue_iai,
    sum(revenue) revenue_total,
    sum(if({ad_unit_name_match}, impressions, 0)) impressions_iai,
    sum(impressions) impressions_total,
    sum(if({ad_unit_name_match}, unfilled, 0)) unfilled_iai,
    sum(unfilled) unfilled_total
    from `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data_{test_id}`
    group by 1
), ar as (
  select session_id fs_session_id, percentile_placement,
    count(*) over (partition by session_id) session_id_row_count
  from `freestar-157323.prod_eventstream.iai_high_impact_tracking`
  WHERE _PARTITIONDATE in UNNEST(ddates)
  group by 1, 2
)
select date, floor(percentile_placement*4)/4 percentile_placement,
    count(*) sessions,
    avg(impressions_iai) impressions_iai, avg(impressions_total) impressions_total,
    avg(unfilled_iai) unfilled_iai, avg(unfilled_total) unfilled_total,
    avg(revenue_iai) * 1000 rps_iai, avg(revenue_total) *1000 rps_total
from dtf join ar using (fs_session_id)
where session_id_row_count = 1
group by 1, 2
order by 1, 2