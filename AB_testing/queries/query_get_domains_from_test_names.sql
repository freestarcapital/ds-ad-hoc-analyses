select NET.REG_DOMAIN(page_url) domain, test_name, count(*) page_hits
from `freestar-157323.prod_eventstream.pagehits_raw`
where _PARTITIONDATE >= date_sub('{start_date}', interval 1 day)
    and _PARTITIONDATE <= date_add('{end_date}', interval 1 day)
    and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) >= '{start_date}'
    and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) <= '{end_date}'
    and test_name in {test_names_list}
group by 1, 2
having safe_divide(page_hits, date_diff(date('{end_date}'), date('{start_date}'), day)) > {min_page_hits}