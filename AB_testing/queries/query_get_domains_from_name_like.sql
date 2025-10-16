with test_names as
(
    select distinct id test_name
    from `freestar-157323.dashboard.pubfig_ab_test`
    where name like '{name_like}'
)

select NET.REG_DOMAIN(page_url) domain, count(*) page_hits
from `freestar-157323.prod_eventstream.pagehits_raw`
join test_names using (test_name)
where NET.REG_DOMAIN(page_url) is not null
    and lower(NET.REG_DOMAIN(page_url)) != 'none'
    and _PARTITIONDATE >= date_sub('{start_date}', interval 1 day)
    and _PARTITIONDATE <= date_add('{end_date}', interval 1 day)
    and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) >= '{start_date}'
    and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) <= '{end_date}'
group by 1
having safe_divide(page_hits, date_diff(date('{end_date}'), date('{start_date}'), day)) > {min_page_hits}