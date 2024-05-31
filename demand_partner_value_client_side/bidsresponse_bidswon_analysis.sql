with t1 as
(
  select auction_id, count(*) count_bidswon_raw, avg(server_time) avg_server_time
  from `freestar-157323.prod_eventstream.bidswon_raw`
  where 1716465600000 < server_time and server_time < 1716469200000  -- 23 May 12:00 to 13:00 GMT
  group by 1
), t2 as
(
  select auction_id, count(*) count_bidsresponse_raw, avg(server_time) avg_server_time
  from `freestar-157323.prod_eventstream.bidsresponse_raw`
  where 1716465600000 < server_time and server_time < 1716469200000  -- 23 May 12:00 to 13:00 GMT
  group by 1
)
select coalesce(t1.auction_id, t2.auction_id) auction_id,
  --TIMESTAMP_MILLIS(cast(t1.avg_server_time as int64)) avg_server_time_bidswon_raw,
  --TIMESTAMP_MILLIS(cast(t2.avg_server_time as int64)) avg_server_time_bidresponse_raw,
  ifnull(count_bidswon_raw, 0) count_bidswon_raw,
  ifnull(count_bidsresponse_raw, 0) count_bidsresponse_raw
 from t1
full outer join t2 on (t1.auction_id = t2.auction_id)
