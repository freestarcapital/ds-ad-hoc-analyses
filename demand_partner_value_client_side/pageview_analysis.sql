with t1 as
(
  select pageview_id, count(*) count_auction_end_raw, avg(server_time) avg_server_time
  from `freestar-157323.prod_eventstream.auction_end_raw`
  where 1716465600000 < server_time and server_time < 1716469200000  -- 23 May 12:00 to 13:00 GMT
  group by 1
), t2 as
(
  select pageview_id, count(*) count_bidsresponse_raw, avg(server_time) avg_server_time
  from `freestar-157323.prod_eventstream.bidsresponse_raw`
  where 1716465600000 < server_time and server_time < 1716469200000  -- 23 May 12:00 to 13:00 GMT
  --where 	1716462000000 < server_time and server_time < 1716472800000  -- 23 May 11:00 to 14:00 GMT
  group by 1
)
select coalesce(t1.pageview_id, t2.pageview_id) pageview_id,
--  TIMESTAMP_MILLIS(cast(t1.avg_server_time as int64)) avg_server_time_auction_end_raw,
--  TIMESTAMP_MILLIS(cast(t2.avg_server_time as int64)) avg_server_time_bidresponse_raw,
  ifnull(count_auction_end_raw, 0) count_auction_end_raw,
  ifnull(count_bidsresponse_raw, 0) count_bidsresponse_raw
 from t1
full outer join t2 on (t1.pageview_id = t2.pageview_id)

