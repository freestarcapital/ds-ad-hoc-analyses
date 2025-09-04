with test_names as
(
    select distinct id test_name
    from `freestar-157323.dashboard.pubfig_ab_test`
    where collection_id in {collection_ids_list}
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
having page_hits > {min_page_hits}