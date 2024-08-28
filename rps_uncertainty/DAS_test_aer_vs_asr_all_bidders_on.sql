 -- getting sessions that are within randomised controlled trials only - include all data with more dimension/metrics: SOV requirements
with auction_end_raw__test as
( select date(timestamp_trunc(_PARTITIONTIME, day)) as record_date__utc ,
date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst ,
test_name ,
test_group ,
session_id ,
a.site_id ,
count(*) as requests
from `freestar-157323.prod_eventstream.auction_end_raw` a
join `freestar-157323.dashboard.pubfig_ab_test` ab on ab.id = test_name
where timestamp_trunc(_PARTITIONTIME, day) >= timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 10 day)
and timestamp_trunc(_PARTITIONTIME, day) < timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 0 day)
and ab.name like 'DS-725 DAS%'
group by 1,2,3,4,5,6 ),

auction_start_raw__test as
( select date(timestamp_trunc(_PARTITIONTIME, day)) as record_date__utc ,
date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst ,
test_name ,
test_group ,
session_id ,
a.site_id ,
count(*) as requests
from `freestar-157323.prod_eventstream.auction_start_raw` a
join `freestar-157323.dashboard.pubfig_ab_test` ab on ab.id = test_name
where timestamp_trunc(_PARTITIONTIME, day) >= timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 10 day)
and timestamp_trunc(_PARTITIONTIME, day) < timestamp_sub(timestamp_trunc(current_timestamp(), day), interval 0 day)
and ab.name like 'DS-725 DAS%'
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
), t10 as (
  select record_date__utc, test_name , test_group, session_count_aer/session_count_asr conversion_rate
from aer_session_count join asr_session_count using (record_date__utc, test_name , test_group)
)
select test_group, avg(conversion_rate)
from t10
group by 1

