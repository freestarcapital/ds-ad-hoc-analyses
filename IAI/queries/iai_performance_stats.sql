DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('{start_date}'), DATE('{end_date}'));

with dtf as (
    select fs_session_id, min(date) date,
    sum(revenue) revenue_total,
    sum(impressions) impressions_total,
    sum(unfilled) unfilled_total,
    sum(if(((lower(ad_unit_name) like "%iai%") or (lower(ad_unit_name) like "%dynamic%")
        or (lower(ad_unit_name) like "%incontent%")), revenue, 0)) revenue_iai,
    sum(if(((lower(ad_unit_name) like "%iai%") or (lower(ad_unit_name) like "%dynamic%")
        or (lower(ad_unit_name) like "%incontent%")), impressions, 0)) impressions_iai,
    sum(if(((lower(ad_unit_name) like "%iai%") or (lower(ad_unit_name) like "%dynamic%")
        or (lower(ad_unit_name) like "%incontent%")), unfilled, 0)) unfilled_iai,
    sum(if(lower(fs_placement_name) like "%flying_carpet%", revenue, 0)) revenue_flying_carpet,
    sum(if(lower(fs_placement_name) like "%flying_carpet%", impressions, 0)) impressions_flying_carpet,
    sum(if(lower(fs_placement_name) like "%flying_carpet%", unfilled, 0)) unfilled_flying_carpet

    from `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data_new_{test_id}`
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
    avg(impressions_total) impressions_total,
    avg(unfilled_total) unfilled_total,
    avg(revenue_total) *1000 rps_total,
    avg(impressions_iai) impressions_iai,
    avg(unfilled_iai) unfilled_iai,
    avg(revenue_iai) * 1000 rps_iai,
    avg(impressions_flying_carpet) impressions_flying_carpet,
    avg(unfilled_flying_carpet) unfilled_flying_carpet,
    avg(revenue_flying_carpet) * 1000 rps_flying_carpet

from dtf join ar using (fs_session_id)
where session_id_row_count = 1
group by 1, 2
order by 1, 2