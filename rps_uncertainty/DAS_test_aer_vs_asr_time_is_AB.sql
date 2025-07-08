 -- getting sessions that are within randomised controlled trials only - include all data_cache with more dimension/metrics: SOV requirements
with auction_end_raw__test as
( select date(timestamp_trunc(_PARTITIONTIME, day)) as record_date__utc ,
date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst ,
test_name ,
test_group ,
session_id ,
site_id ,
count(*) as requests
from `freestar-157323.prod_eventstream.auction_end_raw`
where timestamp_trunc(_PARTITIONTIME, day) >= timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 6 day)
and timestamp_trunc(_PARTITIONTIME, day) < timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 0 day)
and test_name is not null
and test_name like '475797f2-9607-4cbf-b1e0-5322a67ef367'
group by 1,2,3,4,5,6 ),

auction_start_raw__test as
( select date(timestamp_trunc(_PARTITIONTIME, day)) as record_date__utc ,
date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst ,
test_name ,
test_group ,
session_id ,
site_id ,
count(*) as requests
from `freestar-157323.prod_eventstream.auction_start_raw`
where timestamp_trunc(_PARTITIONTIME, day) >= timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 6 day)
and timestamp_trunc(_PARTITIONTIME, day) < timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 0 day)
and test_name is not null
and test_name like '475797f2-9607-4cbf-b1e0-5322a67ef367'
group by 1,2,3,4,5,6 ),
aer_session_count as (
select record_date__utc,
test_name , test_group , count(distinct(session_id)) session_count_aer
from auction_end_raw__test
group by 1, 2, 3
), asr_session_count as (
select record_date__utc,
test_name , test_group , count(distinct(session_id)) session_count_asr
from auction_start_raw__test asr
group by 1, 2, 3
)
select *, session_count_asr/session_count_aer

from aer_session_count join asr_session_count using (record_date__utc, test_name , test_group)
order by 1, 2, 3



-- select * from `freestar-157323.dashboard.pubfig_ab_test`
-- where created_by like '%Ted%' collection_id like '%DAS%'
