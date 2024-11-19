with pgv as (
  select pageview_id, hit_time, server_time pgv_server_time
  FROM `freestar-157323.prod_eventstream.pagehits_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null
  qualify count(*) over (partition by pageview_id) = 1
), brr as (
  select pageview_id,
    min(request_timestamp) request_timestamp_min, min(response_timestamp) response_timestamp_min,
    max(request_timestamp) request_timestamp_max, max(response_timestamp) response_timestamp_max,
    max(greatest(coalesce(request_timestamp, 0), coalesce(response_timestamp, 0))) max_time_brr,
    min(auction_timeout) auction_timeout_min, max(auction_timeout) auction_timeout_max,
    min(time_to_respond) time_to_respond_min, max(time_to_respond) time_to_respond_max,
    count(*) bid_count

  from `freestar-157323.prod_eventstream.bidsresponse_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null
  group by 1
), asr as (
  select pageview_id
  from `freestar-157323.prod_eventstream.auction_start_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null and server_time is not null
  group by 1
)

select --*,
    if(asr.pageview_id is not null, 1, 0) as is_in_asr_table,
    brr.bid_count,
    Timestamp_diff(TIMESTAMP_MILLIS(pgv_server_time), TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS hit_to_pv_servertime,
    coalesce(Timestamp_diff(TIMESTAMP_MILLIS(max_time_brr), TIMESTAMP_MILLIS(hit_time), MILLISECOND), 0) AS hit_to_max_time_brr

--    Timestamp_diff(TIMESTAMP_MILLIS(request_timestamp_min), TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS hit_to_req_min,
--    Timestamp_diff(TIMESTAMP_MILLIS(response_timestamp_min), TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS hit_to_resp_min,
--    Timestamp_diff(TIMESTAMP_MILLIS(request_timestamp_max), TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS hit_to_req_max,
--    Timestamp_diff(TIMESTAMP_MILLIS(response_timestamp_max), TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS hit_to_resp_max


from pgv
left join brr using (pageview_id)
left join asr using (pageview_id)
